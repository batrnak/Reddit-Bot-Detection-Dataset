"""Đếm seed User bổ sung từ author flair có quy trình xác minh công khai."""

from collections import Counter
import json
from pathlib import Path
import re

from exports.verified_user_probe_2026 import (
    Archive, BOUNDS, MIN_EVENTS, MONTHS, OUT as BASELINE, SKIP_AUTHORS, eval_exclusion_users,
    fetch_source, flair_evidence, month_bounds, read_flags, read_gold, search_rows, silver_keys, summarize,
)


OUT = Path('exports/verified_user_expansion_2026')
SOURCES = {
    'ProtectAndServe': ('comments', (
        '(Officer OR Cop OR Police OR Sheriff OR Trooper OR Constable OR Detective) -Unverified',
        '(LEO OR Deputy OR Agent OR Marshal) -Unverified',
    )),
    'AskLE': ('comments', ('Verified -Unverified', 'Officer OR Cop OR Police OR Sheriff OR Deputy OR Trooper OR Popo')),
    'Ask_Lawyers': ('comments', ('Lawyer OR Attorney OR Counsel OR Esq OR Solicitor OR Barrister',)),
    'AskALawyer': ('comments', ('Verified Lawyer OR Verified Attorney',)),
    'Lawyertalk': ('comments', ('Confirmed Lawyer',)),
    'AskVet': ('comments', ('Veterinarian OR Veterinary OR DVM OR VMD OR RVT OR LVT OR CVT OR MRCVS',)),
    'FamilyMedicine': ('comments', ('verified',)),
    'medical_advice': ('comments', ('Nurse OR Doctor OR Physician OR Pharmacist OR Paramedic OR Therapist',)),
    'chess': ('comments', ('Verified_Master',)),
    'indiehackers': ('comments', ('Verified Human',)),
    'web3': ('comments', ('Verified Human',)),
    'BlockchainStartups': ('comments', ('Verified Human',)),
    'artcommissions': ('comments', ('Verified Artist',)),
    'hireanartist': ('posts', ('Verified Artist',)),
    'JustMyFace': ('posts', ('Verified',)),
}

POLICE = re.compile(r'\b(officer|cop|police|sheriff|trooper|constable|detective|deputy|agent|leo|marshal)\b')
LAWYER = re.compile(r'\b(lawyer|attorney|counsel|esq|solicitor|barrister)\b')
VET = re.compile(r'\b(veterinarian|veterinary|dvm|vmd|rvt|lvt|cvt|mrcvs)\b')
MEDICAL = re.compile(r'\b(nurse|doctor|physician|pharmacist|paramedic|therapist)\b')
TITLE = re.compile(r'\b(?:w?[gfic]m|nm|wnm)\b.*:verified_master:', re.IGNORECASE)


def verified(source, row, *, until=BOUNDS[-1]):
    captured = row.get('retrieved_on')
    if not isinstance(captured, int) or not row['created_utc'] <= captured < until:
        return False
    flair = (row.get('author_flair_text') or '').strip().casefold()
    template = bool(row.get('author_flair_template_id'))
    if 'unverified' in flair or 'not verified' in flair:
        return False
    if source == 'ProtectAndServe':
        return bool(POLICE.search(flair) and not re.search(r'\bnot (?:a\b|an\b|leo\b)', flair))
    if source == 'AskLE':
        return row.get('author_flair_css_class') == 'verified'
    if source == 'Ask_Lawyers':
        return bool(LAWYER.search(flair) and 'not a lawyer' not in flair and 'not an attorney' not in flair)
    if source == 'AskALawyer':
        return flair in {'verified lawyer', 'verified attorney'}
    if source == 'Lawyertalk':
        return bool(template and flair.startswith('confirmed lawyer'))
    if source == 'AskVet':
        return bool(template and VET.search(flair))
    if source == 'FamilyMedicine':
        return template and '(verified)' in flair
    if source == 'medical_advice':
        return bool(template and MEDICAL.search(flair) and 'professional student' not in flair)
    if source == 'chess':
        return bool(template and TITLE.search(flair))
    if source == 'indiehackers':
        return template and flair in {'verified human fair', 'verified human strong',
                                      'human verified fair', 'human verified strong'}
    css = (row.get('author_flair_css_class') or '').casefold()
    if source == 'artcommissions':
        return template and css == 'artist' and flair.startswith('verified artist')
    if source == 'hireanartist':
        return template and css in {'verified2d', 'verified3d'}
    if source == 'JustMyFace':
        return template and css == 'verover18' and flair == 'verified'
    assert source in {'web3', 'BlockchainStartups'}
    return template and flair == 'verified human'


def fetch_askle(api, activity, *, months=MONTHS, out=OUT):
    source = 'AskLE'
    bounds = month_bounds(months)
    path = out / f'{source}.json'
    if path.exists():
        cached = json.loads(path.read_text(encoding='utf-8'))
        assert tuple(cached['monthly_flair_query_hits']) == months, f'Cache khác cửa sổ: {path}'
        return cached
    out.mkdir(parents=True, exist_ok=True)
    kind, queries = SOURCES[source]
    monthly = {}
    names = set()
    evidence = {}
    for month, start, end in zip(months, bounds, bounds[1:]):
        hits = set()
        for query in queries:
            for left in range(start, end, 7 * 86400):
                rows = search_rows(api, source, kind, left, min(left + 7 * 86400, end), author_flair_text=query)
                for row in rows:
                    name = (row.get('author') or '').casefold()
                    if not name or name in SKIP_AUTHORS:
                        continue
                    hits.add(name)
                    if activity.get(name, (0, None))[0] >= MIN_EVENTS and verified(source, row, until=bounds[-1]):
                        evidence.setdefault(name, flair_evidence(row))
        monthly[month] = len(hits)
        names.update(hits)
        print(f'Đã đọc flair r/{source} {month}: {len(hits)} username, {len(evidence)} đạt lũy kế', flush=True)
    document = {'flair_query_hits': len(names), 'any_events_in_15': len(names & activity.keys()),
                'at_least_min_events_before_flair_check': sum(
                    activity.get(name, (0, None))[0] >= MIN_EVENTS for name in names),
                'monthly_flair_query_hits': monthly, 'evidence': evidence}
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(document, ensure_ascii=False), encoding='utf-8')
    temp.replace(path)
    return document


def run():
    # Lượt trước đã đối chiếu n_events của gold.users với đủ sáu file Silver 06–08.
    for kind in ('posts', 'comments'):
        assert [Path(key).stem[-7:] for key in silver_keys(kind)] == list(MONTHS)
    users = read_gold('users', ['user_id', 'n_events', 'split'])
    activity = {name: (events, split) for name, events, split in users.iter_rows() if events > 0}
    flags = set(read_flags()['user_id'])
    heldout = set(eval_exclusion_users(read_gold('eval_users'))['user_id'])
    baseline = json.loads((BASELINE / 'report.json').read_text(encoding='utf-8'))
    assert tuple(baseline['months']) == MONTHS and baseline['min_events'] == MIN_EVENTS
    base = {row['user_id']: row for row in baseline['candidates']}
    assert len(base) == len(baseline['candidates'])
    for name, row in base.items():
        assert activity[name] == (row['n_events'], row['split']) and name not in flags | heldout
    print(f'Đã đối chiếu {len(base)} seed User cũ với gold.users hiện tại', flush=True)

    api = Archive()
    all_evidence = {}
    counts = {}
    for source in SOURCES:
        if source == 'AskLE':
            document = fetch_askle(api, activity)
        else:
            document = fetch_source(api, source, activity, sources=SOURCES, verify=verified, out=OUT)
        evidence = document.pop('evidence')
        for name, row in evidence.items():
            assert row['author'].casefold() == name and row['subreddit'].casefold() == source.casefold()
            assert BOUNDS[0] <= row['created_utc'] <= row['retrieved_on'] < BOUNDS[-1]
            assert verified(source, row)
            all_evidence.setdefault(name, {})[source] = row
        newly_usable = evidence.keys() - base.keys() - flags - heldout
        counts[source] = {**document, **summarize(evidence, activity, flags, heldout),
                          'new_vs_baseline': len(newly_usable)}
        print(f'r/{source}: {len(evidence)} đã xác minh, {len(newly_usable)} mới so với 164 seed cũ', flush=True)

    candidates = []
    for name in sorted(all_evidence.keys() - base.keys() - flags - heldout):
        events, split = activity[name]
        assert events >= MIN_EVENTS
        candidates.append({'user_id': name, 'n_events': events, 'split': split, 'evidence': all_evidence[name]})
    new_split = Counter(row['split'] for row in candidates)
    base_split = Counter(row['split'] for row in base.values())
    combined_split = base_split + new_split
    report = {'months': MONTHS, 'min_events': MIN_EVENTS, 'baseline': len(base), 'sources': counts,
              'new_distinct_users': len(candidates), 'new_by_split': dict(new_split),
              'combined_distinct_users': len(base) + len(candidates), 'combined_by_split': dict(combined_split),
              'candidates': candidates}
    path = OUT / 'report.json'
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({key: value for key, value in report.items() if key != 'candidates'},
                     ensure_ascii=False, indent=2), flush=True)


if __name__ == '__main__':
    run()
