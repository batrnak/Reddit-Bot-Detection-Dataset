"""Kiểm chứng nguồn nhãn User từ author flair, ngoài pipeline Gold/Silver."""

import argparse
from collections import Counter
from datetime import datetime, timezone
from functools import partial
import json
from pathlib import Path
import re
import time

import polars as pl
import requests

from scripts.gold.common import (
    MIN_EVENTS,
    SKIP_AUTHORS,
    eval_exclusion_users,
    iter_silver,
    read_flags,
    read_gold,
    silver_keys,
)


API = 'https://arctic-shift.photon-reddit.com/api'
OUT = Path('exports/verified_user_probe_2026')
MONTHS = ('2026-06', '2026-07', '2026-08')


def month_bounds(months):
    year, month = map(int, months[-1].split('-'))
    after = datetime(year + (month == 12), month % 12 + 1, 1, tzinfo=timezone.utc)
    return tuple(int(datetime.fromisoformat(f'{value}-01T00:00:00+00:00').timestamp())
                 for value in months) + (int(after.timestamp()),)


BOUNDS = month_bounds(MONTHS)
MIN_SEARCH_WINDOW_SECONDS = 3600
SUBREDDITS = {
    'worldnews', 'politics', 'news', 'geopolitics', 'ukraine', 'ukrainianconflict',
    'israelpalestine', 'internationalnews', 'globaltalk', 'upliftingnews', 'europe',
    'northkorea', 'lesscredibledefence', 'israel', 'palestine',
}
FIELDS = ('id', 'subreddit', 'author', 'author_flair_text', 'created_utc', 'retrieved_on')
SCIENCE_LEVELS = re.compile(
    r'ph\.?d\.?|dphil|doctorate|professor|postdoc(?:toral researcher)?|'
    r'grad(?:uate)? student|master(?:s|[’\']s)?|m\.?sc\.?|m\.?s\.?|m\.?a\.?|mph|meng|'
    r'md|dvm|pharmd|psyd|dds|dmd|dpt|bachelor(?:s|[’\']s)?|b\.?sc\.?|b\.?s\.?|b\.?a\.?',
    re.IGNORECASE,
)
SOURCES = {
    'AskDocs': ('comments', (
        'Physician OR Doctor OR Nurse OR Pharmacist OR Student OR Technologist OR Therapist OR Paramedic',
        'Psychologist OR Dentist OR Optometrist OR Dietitian OR Midwife OR Audiologist OR Technician',
    )),
    'science': ('comments', (
        'PhD OR Professor OR Postdoc OR Student OR Fellow OR Masters OR Master OR MSc OR MS OR MA OR MPH',
        'MEng OR MD OR DVM OR PharmD OR PsyD OR DDS OR DMD OR DPT OR Bachelor OR BSc OR BS OR BA',
        'Journalist OR Doctorate OR DPhil',
    )),
    'selfie': ('posts', ('Verified (18+)',)),
}


class QueryTimeout(Exception):
    pass


class Archive:
    def __init__(self):
        self.session = requests.Session()
        self.calls = 0
        self.last_request = 0.0

    def get(self, endpoint, params):
        for attempt in range(5):
            time.sleep(max(0, 0.65 - (time.monotonic() - self.last_request)))
            self.last_request = time.monotonic()
            self.calls += 1
            try:
                response = self.session.get(f'{API}/{endpoint}', params=params, timeout=30)
            except (requests.ConnectionError, requests.Timeout):
                if attempt == 4:
                    raise
                time.sleep(2 ** attempt)
                continue
            if response.status_code == 422:
                message = response.text[:200]
                if 'timeout' in message.casefold() or 'timed out' in message.casefold():
                    raise QueryTimeout(f'{endpoint}: {message}')
            if response.status_code == 429:
                headers = response.headers
                delay = headers.get('Retry-After') or headers.get('X-RateLimit-Reset') or '30'
                time.sleep(max(float(delay), 2) + 1)
                continue
            if response.status_code in (500, 502, 503, 504):
                time.sleep(2 ** attempt)
                continue
            if not response.ok:
                raise RuntimeError(f'{endpoint} HTTP {response.status_code}: {response.text[:200]}')
            result = response.json()['data']
            assert isinstance(result, list), f'Phản hồi không phải danh sách: {endpoint}'
            return result
        raise RuntimeError(f'Arctic Shift không phản hồi sau retry: {endpoint}')


def find_authors(api, source, kind, query, start, end):
    params = {'subreddit': source, 'author_flair_text': query, 'after': start - 1,
              'before': end, 'aggregate': 'author'}
    try:
        rows = api.get(f'{kind}/search/aggregate', params)
    except QueryTimeout:
        if end - start > 86400:
            mid = (start + end) // 2
            return (find_authors(api, source, kind, query, start, mid)
                    | find_authors(api, source, kind, query, mid, end))
        records = search_rows(
            api, source, kind, start, end, author_flair_text=query,
            fields='id,subreddit,author,created_utc',
        )
        names = (record.get('author') for record in records)
    else:
        assert all(int(row['count']) > 0 for row in rows)
        names = (row.get('key') for row in rows)
    return {name.casefold() for name in names if name} - set(SKIP_AUTHORS)


def is_verified(source, row, *, until=BOUNDS[-1]):
    flair = (row['author_flair_text'] or '').strip()
    captured = row.get('retrieved_on')
    if not isinstance(captured, int) or not row['created_utc'] <= captured < until:
        return False
    if source == 'selfie':
        return flair == 'Verified (18+)' and row.get('author_flair_css_class') in (
            'VerifiedOver18', 'Verified18Promo')
    if source == 'science':
        level, sep, field = flair.partition('|')
        return bool(sep and field.strip() and SCIENCE_LEVELS.fullmatch(level.strip()))
    return bool(flair and (row.get('author_flair_css_class') or '').startswith('verified-'))


def search_rows(api, source, kind, start, end, **filters):
    seen = set()

    def read_window(start, end):
        cursor = start - 1
        while cursor < end - 1:
            params = {'subreddit': source, 'after': cursor,
                      'before': end, 'sort': 'asc', 'limit': 100, **filters}
            for attempt in range(4):
                try:
                    rows = api.get(f'{kind}/search', params)
                    break
                except QueryTimeout as error:
                    remaining_start = cursor + 1
                    if attempt == 1 and end - remaining_start > MIN_SEARCH_WINDOW_SECONDS:
                        # Giữ ranh giới phân trang để đọc hết các record cùng giây.
                        mid = (remaining_start + end) // 2
                        yield from read_window(remaining_start, mid)
                        yield from read_window(mid, end)
                        return
                    if attempt == 3:
                        left = datetime.fromtimestamp(remaining_start, tz=timezone.utc).isoformat()
                        right = datetime.fromtimestamp(end, tz=timezone.utc).isoformat()
                        raise QueryTimeout(f'r/{source} {kind}/search timeout từ {left} đến {right}') from error
                    time.sleep(attempt + 1)
            times = [row['created_utc'] for row in rows]
            assert times == sorted(times)
            for row in rows:
                assert row['subreddit'].casefold() == source.casefold()
                assert cursor < row['created_utc'] < end
                if row['id'] not in seen:
                    seen.add(row['id'])
                    yield row
            if len(rows) < 100:
                return
            next_cursor = times[-1] - 1
            assert next_cursor > cursor, 'Trang API có 100 records trong cùng một giây'
            cursor = next_cursor

    yield from read_window(start, end)


def flair_evidence(row):
    return {field: row[field] for field in (*FIELDS, 'author_flair_css_class', 'author_flair_template_id')
            if field in row}


def find_evidence(api, source, kind, name, start, end, verify):
    filters = {'author': name}
    if source == 'science':
        filters['fields'] = ','.join(FIELDS)
    for row in search_rows(api, source, kind, start, end, **filters):
        assert row['author'].casefold() == name
        if verify(source, row):
            return flair_evidence(row)
    return None


def fetch_source(api, source, activity, *, sources=SOURCES, verify=None, out=OUT, months=MONTHS):
    bounds = month_bounds(months)
    if verify is None:
        verify = partial(is_verified, until=bounds[-1])
    out.mkdir(parents=True, exist_ok=True)
    path = out / f'{source}.json'
    if path.exists():
        cached = json.loads(path.read_text(encoding='utf-8'))
        assert tuple(cached['monthly_flair_query_hits']) == months, f'Cache khác cửa sổ: {path}'
        return cached
    shortlists = {}
    all_names = set()
    kind, queries = sources[source]
    for month, start, end in zip(months, bounds, bounds[1:]):
        hits = set()
        for query in queries:
            assert len(query) <= 100
            hits.update(find_authors(api, source, kind, query, start, end))
        all_names.update(hits)
        shortlists[month] = hits
        print(f'Đã tìm {len(hits):,} username khớp từ khóa flair r/{source} {month}', flush=True)

    eligible = sorted(name for name in all_names if activity.get(name, (0, None))[0] >= MIN_EVENTS)
    evidence = {}
    print(f'Đang kiểm tra flair gốc của {len(eligible)} account đủ events từ r/{source}', flush=True)
    for index, name in enumerate(eligible, start=1):
        row = find_evidence(api, source, kind, name, bounds[0], bounds[-1], verify)
        if row:
            evidence[name] = row
        if index % 20 == 0:
            print(f'  r/{source} đã kiểm tra {index}/{len(eligible)}, đạt {len(evidence)}', flush=True)
    assert len(evidence) <= len(eligible) <= len(all_names)
    payload = {'flair_query_hits': len(all_names), 'any_events_in_15': len(all_names & activity.keys()),
               'at_least_min_events_before_flair_check': len(eligible),
               'monthly_flair_query_hits': {month: len(hits) for month, hits in shortlists.items()},
               'evidence': evidence}
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')
    temp.replace(path)
    return payload


def read_activity(months=MONTHS):
    bounds = month_bounds(months)
    month_bounds_by_name = dict(zip(months, zip(bounds, bounds[1:])))
    parts = []
    observed_subreddits = set()
    for kind in ('posts', 'comments'):
        assert [Path(key).stem[-7:] for key in silver_keys(kind)] == list(months)
        for key, frame in iter_silver(kind, ['author_name', 'subreddit', 'created_utc']):
            observed_subreddits.update(frame['subreddit'].drop_nulls().str.to_lowercase().unique())
            low, high = frame.select(pl.col('created_utc').min().alias('lo'),
                                     pl.col('created_utc').max().alias('hi')).row(0)
            start, end = month_bounds_by_name[Path(key).stem[-7:]]
            assert start <= low.timestamp() <= high.timestamp() < end, key
            parts.append(frame.filter(
                pl.col('author_name').is_not_null() & ~pl.col('author_name').is_in(SKIP_AUTHORS)
            ).group_by('author_name').agg(pl.len().alias('n_events')))
            print(f'Đã đếm events trong {Path(key).name}', flush=True)
    assert observed_subreddits == SUBREDDITS, sorted(observed_subreddits ^ SUBREDDITS)
    activity = pl.concat(parts).group_by('author_name').agg(pl.col('n_events').sum())
    users = read_gold('users', ['user_id', 'n_events', 'split'])
    checked = activity.join(users, left_on='author_name', right_on='user_id', how='left')
    assert checked.filter(pl.col('split').is_null() | (pl.col('n_events') != pl.col('n_events_right'))).is_empty()
    assert users.filter(pl.col('n_events') > 0).height == activity.height, 'Build lại gold.users từ Silver hiện có'
    return {name: (events, split) for name, events, split, _ in
            checked.select('author_name', 'n_events', 'split', 'n_events_right').iter_rows()}


def run(report_only):
    flags = set(read_flags()['user_id'])
    heldout = set(eval_exclusion_users(read_gold('eval_users'))['user_id'])
    activity = read_activity()
    api = Archive()
    payload = {'months': MONTHS, 'min_events': MIN_EVENTS, 'sources': {}, 'candidates': [],
               'flair_search_queries': {source: queries for source, (_, queries) in SOURCES.items()}}
    all_hits = {}
    for source in SOURCES:
        if report_only:
            document = json.loads((OUT / f'{source}.json').read_text(encoding='utf-8'))
        else:
            document = fetch_source(api, source, activity)
        source_hits = document.pop('evidence')
        payload['sources'][source] = {**document, **summarize(source_hits, activity, flags, heldout)}
        for name, evidence in source_hits.items():
            all_hits.setdefault(name, {})[source] = evidence
    payload['unique'] = summarize(all_hits, activity, flags, heldout)
    for name, evidence in sorted(all_hits.items()):
        count, split = activity.get(name, (0, None))
        if count >= MIN_EVENTS and name not in flags and name not in heldout:
            payload['candidates'].append({'user_id': name, 'n_events': count, 'split': split, 'evidence': evidence})
    assert len(payload['candidates']) == payload['unique']['usable']
    path = OUT / 'report.json'
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({key: value for key, value in payload.items() if key != 'candidates'},
                     ensure_ascii=False, indent=2), flush=True)


def summarize(hits, activity, flags, heldout):
    eligible = hits.keys()
    assert all(activity[name][0] >= MIN_EVENTS for name in eligible)
    usable = eligible - flags - heldout
    return {'verified_at_least_min_events_in_15': len(eligible),
            'excluded_flagged': len(eligible & flags),
            'excluded_eval': len(eligible & heldout), 'usable': len(usable),
            'usable_by_split': dict(sorted(Counter(activity[name][1] for name in usable).items()))}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report-only', action='store_true')
    args = parser.parse_args()
    run(args.report_only)
