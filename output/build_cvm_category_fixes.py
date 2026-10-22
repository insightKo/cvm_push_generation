# CVM_CATEGORY_FIXES.sql — правки справочника категорий CVM данными (после выкладки 19.10.2026):
# 52 замечания о неоднородных полках, 5 несопоставленных товаров, ежедневная автопривязка новых кодов ДИКСИ.
# Вход: справочник_CVM_правки_замечаний.json (решения и переносы), CVM_CATEGORY_REFERENCE.sql (что залито в базу),
#       привязка_товаров_к_категориям_CVM.xlsx (товары и названия на 19.10), cvm_category_refresh_proc.sql (процедура).
# Ничего не перевыдаётся: новые полки — новые ID, старые закрываются статусом.
import collections
import json
import re
import sys
from pathlib import Path

import openpyxl

OUT = Path(__file__).resolve().parent
FIXES = OUT / 'справочник_CVM_правки_замечаний.json'
REF = OUT / 'CVM_CATEGORY_REFERENCE.sql'
PRODS = OUT / 'привязка_товаров_к_категориям_CVM.xlsx'
PROC = OUT / 'cvm_category_refresh_proc.sql'
DST = OUT / 'CVM_CATEGORY_FIXES.sql'
REPORT = OUT / 'справочник_CVM_правки_итог.json'
DAY = '19.10.2026'
UNMAPPED = 1651
BATCH = 500
# 5 несопоставленных товаров → «Служебные ТМЦ и упаковка» (решение Елены 19.10: в «Служебное»)
EXTRA = {297549: 1654, 292839: 1654, 319744: 1654, 188843: 1654, 314799: 1654}


def rowcheck(n, what):
    return (f'if @@rowcount <> {n}\nbegin\n    rollback transaction;\n'
            f"    raiserror(N'СТОП: найдено не {n} {what} для правки — ничего не изменено.', 16, 1);\n"
            '    set noexec on;\n    return;\nend;\n')


def q(v):
    if v is None or v == '':
        return 'null'
    if isinstance(v, (int, float)):
        return str(v)
    return "N'" + str(v).replace("'", "''") + "'"


# ---------- что залито в базу 19.10 ----------
def load_state():
    tok = re.compile(r"N'(?:[^']|'')*'|null|-?\d+")
    def val(t):
        return None if t == 'null' else t[2:-1].replace("''", "'") if t.startswith("N'") else int(t)
    sec, st = None, {'cat': [], 'map': [], 'rule': [], 'man': []}
    heads = {'insert into dbo.I_CVM_CATEGORY (': 'cat', 'insert into dbo.I_CVM_CATEGORY_MAP': 'map',
             'insert into dbo.I_CVM_CATEGORY_RULE': 'rule', 'insert into dbo.I_CVM_PRODUCT_CATEGORY_MANUAL': 'man'}
    for ln in REF.read_text(encoding='utf-8').split('\n'):
        h = next((v for k, v in heads.items() if ln.startswith(k)), None)
        if h:
            sec = h
            continue
        s = ln.strip()
        if sec and s.startswith('(') and s[-2:] in ('),', ');'):
            st[sec].append([val(t) for t in tok.findall(s[1:-2])])
        elif sec and s and s != 'values':
            sec = None
    return st


def load_products():
    # из файла берутся только ID, название и код ДИКСИ (файл обновляется после правок);
    # полка «как в базе» пересчитывается по залитому справочнику в main()
    wb = openpyxl.load_workbook(PRODS, read_only=True)
    out = []
    for r in wb['Товар → категория'].iter_rows(min_row=2, values_only=True):
        c = r[9] if len(r) > 9 else None
        out.append((int(r[0]), str(r[1]), int(r[4]), (str(c) if c not in (None, '') else None)))
    return out


# ---------- точная копия логики dbo.cvm_category_refresh ----------
_wc = {}


def hit(nn, word):
    w = word.strip().upper().replace('Ё', 'Е')
    r = _wc.get(w)
    if r is None:
        r = _wc[w] = re.compile(r'[^А-ЯA-Z0-9]' + re.escape(w))
    return r.search(nn) is not None


def assign(prods, mapd, rules, man):
    res = {}
    for pid, name, _, code in prods:
        if pid in man:
            res[pid] = man[pid]
            continue
        m = mapd.get(code)
        if m and m[0] == 1:
            res[pid] = m[1]
            continue
        got = None
        if m and m[0] == 2:
            nn = ' ' + name.upper().replace('Ё', 'Е')
            for order, wa, wn, cat in sorted(rules.get(code, [])):
                A = [x for x in (wa or '').split(';') if x.strip()]
                N = [x for x in (wn or '').split(';') if x.strip()]
                if (not A or any(hit(nn, x) for x in A)) and not any(hit(nn, x) for x in N):
                    got = cat
                    break
        res[pid] = got or UNMAPPED
    return res


def main():
    fx = json.loads(FIXES.read_text(encoding='utf-8'))
    st = load_state()
    prods = load_products()
    name_of = {p[0]: p[1] for p in prods}
    code_of = {p[0]: p[3] for p in prods}
    cats = {c[0]: {'name': c[1], 'node': c[2], 'parent': c[3], 'unit': c[4], 'no_scale': c[5]} for c in st['cat']}
    mapd = {c: (m, cat) for c, n, m, cat, s in st['map']}
    rules = collections.defaultdict(list)
    for c, o, a, n, cat in st['rule']:
        rules[c].append((o, a, n, cat))
    man = {p: c for p, c, s in st['man']}
    cur = assign(prods, mapd, rules, man)
    assert len(cur) == 312558 and sum(1 for c in cur.values() if c == UNMAPPED) == 5, 'состояние не как в базе 19.10'

    errors = []
    # новые полки → ID с max+1 по порядку замечаний
    nid = max(cats) + 1
    key_id, new_cats, renames, scale, close, defs = {}, [], {}, {}, set(), {}
    remarks = fx['remarks']
    for rno in sorted(remarks, key=int):
        R = remarks[rno]
        for nc in R.get('new_categories', []):
            key_id[nc['key']] = nid
            new_cats.append((nid, nc['name'], nc['group_id'], nc['unit'], int(nc.get('no_scale', 0)), rno))
            nid += 1
        for rn in R.get('renames', []):
            if rn.get('name') and rn['name'] != cats[rn['id']]['name']:
                renames[rn['id']] = rn['name']
            if rn.get('definition'):
                defs[rn['id']] = rn['definition']
        for nc in R.get('new_categories', []):
            defs[nc['key']] = nc.get('definition')
        for ns in R.get('no_scale', []):
            scale[ns['id']] = int(ns['no_scale'])
        close.update(R.get('close', []))
    tid = lambda t: key_id[t] if isinstance(t, str) else int(t)
    names = collections.Counter()
    for i, c in cats.items():
        if c['node'] == 2 and i not in close:
            names[renames.get(i, c['name']).lower()] += 1
    for c in new_cats:
        names[c[1].lower()] += 1
    errors += [f'название полки не уникально: {n}' for n, k in names.items() if k > 1]

    # переносы
    desired = dict(cur)
    why = {}
    for rno in sorted(remarks, key=int):
        for mv in remarks[rno].get('moves', []):
            pid, frm, to = int(mv['id']), int(mv['from']), tid(mv['to'])
            if pid not in cur:
                errors.append(f'№{rno}: товара {pid} нет')
                continue
            if pid in why and desired[pid] != to:
                errors.append(f'№{rno}: товар {pid} уже перенесён по №{why[pid]} в {desired[pid]}, здесь в {to}')
                continue
            if cur[pid] != frm:
                errors.append(f'№{rno}: товар {pid} лежит в {cur[pid]}, а не в {frm}')
                continue
            desired[pid], why[pid] = to, rno
    for pid, to in EXTRA.items():
        desired[pid], why[pid] = to, 'несопоставленные'
    for i in close:
        left = [p for p, c in desired.items() if c == i]
        if left:
            errors.append(f'закрываемая {i} не пуста: {len(left)} товаров')
    if errors:
        print('\n'.join(errors))
        sys.exit(1)

    # будущие товары: код целиком переехал → маппинг; часть кода → правило слов, если оно точно повторяет выбор
    new_map, new_rules = dict(mapd), {c: list(v) for c, v in rules.items()}
    agent_rules = collections.defaultdict(list)
    for rno in sorted(remarks, key=int):
        for r in remarks[rno].get('rules', []):
            wa = ';'.join(w.strip().upper().replace('Ё', 'Е') for w in r.get('words_any', []) if w.strip())
            wn = ';'.join(w.strip().upper().replace('Ё', 'Е') for w in r.get('words_none', []) if w.strip())
            if not wa or re.search(r'[%_\[\]]', wa + wn):
                continue
            agent_rules[int(r['from'])].append((wa, wn or None, tid(r['to'])))
    by_code = collections.defaultdict(list)
    for p in prods:
        by_code[p[3]].append(p)
    moved_codes = {code_of[p] for p in why if code_of[p] is not None}
    rule_codes, map_changes = set(), {}
    for code in sorted(moved_codes):
        m = mapd.get(code)
        nonman = [p for p in by_code[code] if p[0] not in man]
        if not m or not nonman or not any(p[0] in why for p in nonman):
            continue
        targets = collections.Counter(desired[p[0]] for p in nonman)
        if m[0] == 1 and len(targets) == 1:
            new_map[code] = (1, next(iter(targets)))
            map_changes[code] = new_map[code]
            continue
        present = {cur[p[0]] for p in nonman}
        cand = [r for f in sorted(present) for r in agent_rules.get(f, [])]
        if not cand:
            continue
        if m[0] == 1:
            rest = m[1] if m[1] not in close else targets.most_common(1)[0][0]
            trial = [(i + 1, a, n, t) for i, (a, n, t) in enumerate(cand)] + [(len(cand) + 1, None, None, rest)]
        else:
            trial = [(i + 1, a, n, t) for i, (a, n, t) in enumerate(cand)] + \
                    [(o + len(cand), a, n, t) for o, a, n, t in sorted(rules[code])]
        if len(trial) > 255:
            continue
        sim = assign(nonman, {code: (2, None)}, {code: trial}, {})
        if all(sim[p[0]] == desired[p[0]] for p in nonman):
            new_map[code] = (2, None)
            new_rules[code] = trial
            rule_codes.add(code)
            map_changes[code] = (2, None)
    # маппинг и правила, смотрящие в закрытые полки → куда ушло большинство товаров
    for code, (mm, cat) in list(new_map.items()):
        if mm == 1 and cat in close:
            nonman = [p for p in by_code[code] if p[0] not in man]
            t = collections.Counter(desired[p[0]] for p in nonman).most_common(1)
            new_map[code] = map_changes[code] = (1, t[0][0] if t else UNMAPPED)
    for code, rl in new_rules.items():
        if any(r[3] in close for r in rl):
            sim_now = [p for p in by_code[code] if p[0] not in man]
            fixed = []
            for o, a, n, t in rl:
                if t in close:
                    caught = [p for p in sim_now if assign([p], {code: (2, None)}, {code: [(1, a, n, t)]}, {})[p[0]] == t]
                    c = collections.Counter(desired[p[0]] for p in caught).most_common(1)
                    t = c[0][0] if c else UNMAPPED
                fixed.append((o, a, n, t))
            new_rules[code] = fixed
            rule_codes.add(code)
    # остаток — поштучно; итог проверяется полной симуляцией
    new_man = dict(man)
    for _ in range(3):
        sim = assign(prods, new_map, new_rules, new_man)
        diff = [p for p in desired if sim[p] != desired[p]]
        if not diff:
            break
        for p in diff:
            new_man[p] = desired[p]
    sim = assign(prods, new_map, new_rules, new_man)
    assert all(sim[p] == desired[p] for p in desired), 'итог не сошёлся'
    man_rows = {p: c for p, c in new_man.items() if man.get(p) != c}

    # ---------- SQL ----------
    new_ids = [c[0] for c in new_cats]
    moved = {p: c for p, c in desired.items() if c != cur[p]}
    L = []
    L.append(f'''-- ============================================================================================
-- CVM_CATEGORY_FIXES — правки справочника категорий CVM данными ({DAY}, после выкладки справочника).
-- Запустить целиком в SSMS (F5). Повторный запуск безопасен: результат тот же.
--
-- Действия по объектам:
--   dbo.I_CVM_CATEGORY                — добавляются {len(new_cats)} новых полок (ID {min(new_ids)}–{max(new_ids)});
--                                       переименовываются {len(renames)}; «без шкалы» меняется у {len(scale)};
--                                       закрываются (STATUS 0, DATE_CLOSE) {len(close)} пустых: {", ".join(map(str, sorted(close))) or "—"}.
--   dbo.I_CVM_CATEGORY_MAP            — у {len(map_changes)} кодов ДИКСИ меняется привязка (код целиком → новая полка
--                                       или код режется правилами слов).
--   dbo.I_CVM_CATEGORY_RULE           — правила {len(rule_codes)} кодов пересобираются (удаляются и вставляются заново
--                                       только у этих кодов).
--   dbo.I_CVM_PRODUCT_CATEGORY_MANUAL — {len(man_rows)} товаров поштучно (новые строки или новая полка у старых).
--   dbo.cvm_category_refresh          — новая версия (create or alter): новые коды ДИКСИ привязываются каждый день
--                                       сразу после загрузки I_PRODUCT — по прежней категории их товаров
--                                       (перекодировка ДИКСИ), иначе по соседним кодам того же родителя
--                                       (≥ 2 соседа, все в одной полке); остальное — «несопоставлен», как раньше.
--   dbo.I_CVM_PRODUCT_CATEGORY        — пересчитывается процедурой: меняется полка у {len(moved)} товаров.
--   Ничего не удаляется, кроме правил пересобираемых кодов; ID не перевыдаются; задание MCI не меняется.
-- ============================================================================================

use mci_model;
go
set noexec off;
set nocount on;
set xact_abort on;
go

-- 0. Не во время «MCI Regular update»: скрипт пересоздаёт процедуру его шага CVM_CATEGORY и запускает её —
--    параллельный прогон даст конфликт ключей и чтение I_PRODUCT во время перезаливки. Иначе — стоп, ничего не изменено.
exec sys.sp_set_session_context @key = N'cvm_fixes_ok', @value = 0;
if exists (select 1 from msdb.dbo.sysjobactivity as a (nolock)
           join msdb.dbo.sysjobs as j (nolock) on j.job_id = a.job_id
           where j.name = N'MCI Regular update' and a.start_execution_date is not null and a.stop_execution_date is null
             and a.session_id = (select max(session_id) from msdb.dbo.syssessions (nolock)))
begin
    raiserror(N'СТОП: идёт «MCI Regular update» — запустите скрипт после его окончания, ничего не изменено.', 16, 1);
    set noexec on;
end;
go

-- 0а. Новые ID свободны или уже заняты этими же полками (повторный запуск)
if exists (select 1 from dbo.I_CVM_CATEGORY as c (nolock)
           join (values {", ".join(f"({c[0]}, {q(c[1])})" for c in new_cats)}) as v (ID, NAME) on v.ID = c.ID_CVM_CATEGORY
           where c.NAME <> v.NAME)
begin
    raiserror(N'СТОП: ID новых полок заняты другими категориями — ничего не изменено.', 16, 1);
    set noexec on;
end;
go

begin transaction;

-- 1. Новые полки
insert into dbo.I_CVM_CATEGORY (ID_CVM_CATEGORY, NAME, NODE_TYPE, ID_PARENT, UNIT, NO_SCALE, STATUS, NOTE)
select v.ID, v.NAME, 2, v.ID_PARENT, v.UNIT, v.NO_SCALE, 1, v.NOTE
from (values
''')
    L.append(',\n'.join(f'    ({c[0]}, {q(c[1])}, {c[2]}, {q(c[3])}, {c[4]}, {q("замечание №" + c[5] + " проверки, " + DAY)})'
                        for c in new_cats))
    L.append(''') as v (ID, NAME, ID_PARENT, UNIT, NO_SCALE, NOTE)
where not exists (select 1 from dbo.I_CVM_CATEGORY as c where c.ID_CVM_CATEGORY = v.ID);
''')
    if renames:
        L.append('\n-- 2. Переименования (ID те же)\nupdate c set c.NAME = v.NAME\nfrom dbo.I_CVM_CATEGORY as c\n'
                 'join (values ' + ', '.join(f'({i}, {q(n)})' for i, n in sorted(renames.items()))
                 + ') as v (ID, NAME) on v.ID = c.ID_CVM_CATEGORY;\n' + rowcheck(len(renames), 'переименований'))
    if scale:
        L.append('\n-- 3. Признак «без шкалы»\nupdate c set c.NO_SCALE = v.NO_SCALE\nfrom dbo.I_CVM_CATEGORY as c\n'
                 'join (values ' + ', '.join(f'({i}, {s})' for i, s in sorted(scale.items()))
                 + ') as v (ID, NO_SCALE) on v.ID = c.ID_CVM_CATEGORY;\n')
    if map_changes:
        L.append('\n-- 4. Привязка кодов ДИКСИ (1 — код целиком в полку, 2 — код режется правилами слов)\n'
                 'update m set m.MAP_METHOD = v.MAP_METHOD, m.ID_CVM_CATEGORY = v.ID, m.STATUS = 1,\n'
                 f'       m.DATE_UPDATE = cast(getdate() as date), m.NOTE = N\'правки замечаний {DAY}\'\n'
                 'from dbo.I_CVM_CATEGORY_MAP as m\njoin (values\n'
                 + ',\n'.join(f'    ({q(c)}, {mm}, {q(cat)})' for c, (mm, cat) in sorted(map_changes.items()))
                 + '\n) as v (CODE, MAP_METHOD, ID) on v.CODE = m.ID_CATEGORY_5_ext;\n' + rowcheck(len(map_changes), 'кодов ДИКСИ'))
    if rule_codes:
        rows = [(c, o, a, n, t) for c in sorted(rule_codes) for o, a, n, t in sorted(new_rules[c])]
        L.append('\n-- 5. Правила слов пересобираемых кодов\ndelete from dbo.I_CVM_CATEGORY_RULE where ID_CATEGORY_5_ext in ('
                 + ', '.join(q(c) for c in sorted(rule_codes)) + ');\n')
        for i in range(0, len(rows), BATCH):
            L.append('insert into dbo.I_CVM_CATEGORY_RULE (ID_CATEGORY_5_ext, RULE_ORDER, WORDS_ANY, WORDS_NONE, ID_CVM_CATEGORY)\nvalues\n'
                     + ',\n'.join(f'    ({q(c)}, {o}, {q(a)}, {q(n)}, {t})' for c, o, a, n, t in rows[i:i + BATCH]) + ';\n')
    L.append('\n-- 6. Товары поштучно: новая полка у существующих строк, новые строки\n'
             'drop table if exists #m;\ncreate table #m (ID_PRODUCT bigint not null primary key, ID_CVM_CATEGORY int not null, NOTE nvarchar(500) null);\n')
    mr = sorted(man_rows.items())
    for i in range(0, len(mr), BATCH):
        L.append('insert into #m (ID_PRODUCT, ID_CVM_CATEGORY, NOTE)\nvalues\n'
                 + ',\n'.join(f'    ({p}, {c}, {q("замечание №" + str(why.get(p, "-")) + ", " + DAY)})' for p, c in mr[i:i + BATCH]) + ';\n')
    L.append('''update x set x.ID_CVM_CATEGORY = m.ID_CVM_CATEGORY, x.SOURCE = 2, x.DATE_UPDATE = cast(getdate() as date), x.NOTE = m.NOTE
from dbo.I_CVM_PRODUCT_CATEGORY_MANUAL as x join #m as m on m.ID_PRODUCT = x.ID_PRODUCT
where x.ID_CVM_CATEGORY <> m.ID_CVM_CATEGORY;
insert into dbo.I_CVM_PRODUCT_CATEGORY_MANUAL (ID_PRODUCT, ID_CVM_CATEGORY, SOURCE, DATE_UPDATE, NOTE)
select m.ID_PRODUCT, m.ID_CVM_CATEGORY, 2, cast(getdate() as date), m.NOTE
from #m as m where not exists (select 1 from dbo.I_CVM_PRODUCT_CATEGORY_MANUAL as x where x.ID_PRODUCT = m.ID_PRODUCT);
''')
    if close:
        L.append('\n-- 7. Закрыть опустевшие полки (ID не переиспользуется)\nupdate dbo.I_CVM_CATEGORY set STATUS = 0, DATE_CLOSE = cast(getdate() as date)\n'
                 f'where ID_CVM_CATEGORY in ({", ".join(map(str, sorted(close)))}) and STATUS = 1;\n')
    L.append('''
-- 8. Контроль ссылок: при нарушении — список, откат, ничего не изменено
declare @bad table (PROBLEM nvarchar(100), KEY_VALUE nvarchar(200));
insert into @bad (PROBLEM, KEY_VALUE)
select N'код ведёт не на активную категорию', m.ID_CATEGORY_5_ext
from   dbo.I_CVM_CATEGORY_MAP as m (nolock)
where  m.ID_CVM_CATEGORY is not null
  and  not exists (select 1 from dbo.I_CVM_CATEGORY as c (nolock) where c.ID_CVM_CATEGORY = m.ID_CVM_CATEGORY and c.NODE_TYPE = 2 and c.STATUS = 1)
union all
select N'правило не на активную категорию или код не «по словам»', r.ID_CATEGORY_5_ext
from   dbo.I_CVM_CATEGORY_RULE as r (nolock)
where  not exists (select 1 from dbo.I_CVM_CATEGORY as c (nolock) where c.ID_CVM_CATEGORY = r.ID_CVM_CATEGORY and c.NODE_TYPE = 2 and c.STATUS = 1)
   or  not exists (select 1 from dbo.I_CVM_CATEGORY_MAP as m (nolock) where m.ID_CATEGORY_5_ext = r.ID_CATEGORY_5_ext and m.MAP_METHOD = 2)
union all
select N'код «по словам» без правил', m.ID_CATEGORY_5_ext
from   dbo.I_CVM_CATEGORY_MAP as m (nolock)
where  m.MAP_METHOD = 2
  and  not exists (select 1 from dbo.I_CVM_CATEGORY_RULE as r (nolock) where r.ID_CATEGORY_5_ext = m.ID_CATEGORY_5_ext)
union all
select N'товар поштучно не в активной категории', cast(p.ID_PRODUCT as nvarchar(50))
from   dbo.I_CVM_PRODUCT_CATEGORY_MANUAL as p (nolock)
where  not exists (select 1 from dbo.I_CVM_CATEGORY as c (nolock) where c.ID_CVM_CATEGORY = p.ID_CVM_CATEGORY and c.NODE_TYPE = 2 and c.STATUS = 1)
union all
select N'категория без активной группы', cast(c.ID_CVM_CATEGORY as nvarchar(50))
from   dbo.I_CVM_CATEGORY as c (nolock)
where  c.NODE_TYPE = 2 and c.STATUS = 1
  and  not exists (select 1 from dbo.I_CVM_CATEGORY as g (nolock) where g.ID_CVM_CATEGORY = c.ID_PARENT and g.NODE_TYPE = 1 and g.STATUS = 1)
union all
select N'в закрываемой полке товар вне I_PRODUCT', cast(t.ID_PRODUCT as nvarchar(50))
from   dbo.I_CVM_PRODUCT_CATEGORY as t (nolock)
       join dbo.I_CVM_CATEGORY as c (nolock) on c.ID_CVM_CATEGORY = t.ID_CVM_CATEGORY and c.STATUS = 0
where  not exists (select 1 from dbo.I_PRODUCT as p (nolock) where p.ID_COMPANY = 1 and p.ID_PRODUCT = t.ID_PRODUCT)
union all
select N'название категории не уникально', c.NAME
from   dbo.I_CVM_CATEGORY as c (nolock)
where  c.NODE_TYPE = 2 and c.STATUS = 1
group by c.NAME having count(*) > 1;

if exists (select 1 from @bad)
begin
    rollback transaction;
    select PROBLEM, KEY_VALUE from @bad order by PROBLEM, KEY_VALUE;
    raiserror(N'СТОП: нарушены ссылки справочника (список выше) — правки отменены, ничего не изменено.', 16, 1);
    set noexec on;
    return;
end;

commit transaction;
exec sys.sp_set_session_context @key = N'cvm_fixes_ok', @value = 1;
go

-- правки не применились (ошибка выше) — процедуру не меняем, пересчёт не запускаем
if isnull(cast(session_context(N'cvm_fixes_ok') as int), 0) <> 1
begin
    if @@trancount > 0 rollback transaction;
    raiserror(N'СТОП: правки справочника не применились (ошибка выше) — процедура не меняется, пересчёт не запускается.', 16, 1);
    set noexec on;
end;
go

''')
    L.append(PROC.read_text(encoding='utf-8'))
    L.append('''
-- 10. Пересчёт привязки товаров (до него — снимок, чтобы увидеть смены полки вне списка правок)
drop table if exists #before;
select ID_PRODUCT, ID_CVM_CATEGORY into #before from dbo.I_CVM_PRODUCT_CATEGORY (nolock);
go
exec dbo.cvm_category_refresh;
if @@trancount > 0
begin
    rollback transaction;
    raiserror(N'СТОП: пересчёт прерван ошибкой — транзакция процедуры отменена, проверки ниже не смотреть.', 16, 1);
end;
go

-- 11. Проверка: каждый перенесённый товар — в своей новой полке
drop table if exists #e;
create table #e (ID_PRODUCT bigint not null primary key, ID_CVM_CATEGORY int not null);
''')
    mv = sorted(moved.items())
    for i in range(0, len(mv), BATCH):
        L.append('insert into #e (ID_PRODUCT, ID_CVM_CATEGORY)\nvalues\n'
                 + ',\n'.join(f'    ({p}, {c})' for p, c in mv[i:i + BATCH]) + ';\n')
    L.append(f'''
select CHECK_NAME = N'Перенесено товаров (ожидается {len(moved)})', CNT = count(*)
from   #e as e join dbo.I_CVM_PRODUCT_CATEGORY as t (nolock) on t.ID_PRODUCT = e.ID_PRODUCT and t.ID_CVM_CATEGORY = e.ID_CVM_CATEGORY
union all
select N'Не на месте (должно быть 0)', count(*)
from   #e as e left join dbo.I_CVM_PRODUCT_CATEGORY as t (nolock) on t.ID_PRODUCT = e.ID_PRODUCT
where  isnull(t.ID_CVM_CATEGORY, -1) <> e.ID_CVM_CATEGORY
union all
select N'Товаров в закрытых полках (должно быть 0)', count(*)
from   dbo.I_CVM_PRODUCT_CATEGORY as t (nolock)
       join dbo.I_CVM_CATEGORY as c (nolock) on c.ID_CVM_CATEGORY = t.ID_CVM_CATEGORY
where  c.STATUS = 0
union all
select N'Сменили полку вне списка правок (проверить)', count(*)
from   dbo.I_CVM_PRODUCT_CATEGORY as t (nolock) join #before as b on b.ID_PRODUCT = t.ID_PRODUCT
where  b.ID_CVM_CATEGORY <> t.ID_CVM_CATEGORY and not exists (select 1 from #e as e where e.ID_PRODUCT = t.ID_PRODUCT)
union all
select N'Несопоставленные товары', count(*) from dbo.I_CVM_PRODUCT_CATEGORY (nolock) where HOW = 0;

-- новые и изменённые полки: сколько товаров
select c.ID_CVM_CATEGORY, c.NAME, c.UNIT, c.NO_SCALE, c.STATUS, PRODUCTS = count(t.ID_PRODUCT)
from   dbo.I_CVM_CATEGORY as c (nolock)
       left join dbo.I_CVM_PRODUCT_CATEGORY as t (nolock) on t.ID_CVM_CATEGORY = c.ID_CVM_CATEGORY
where  c.ID_CVM_CATEGORY in ({", ".join(map(str, sorted(set(new_ids) | set(renames) | set(scale) | close | set(moved.values()) | set(cur[p] for p in moved))))})
group by c.ID_CVM_CATEGORY, c.NAME, c.UNIT, c.NO_SCALE, c.STATUS
order by c.ID_CVM_CATEGORY;
go
set noexec off;
go
''')
    DST.write_text(''.join(L), encoding='utf-8-sig')

    # итог для отчёта и справочника
    rep = {'new_categories': [{'id': c[0], 'name': c[1], 'group_id': c[2], 'unit': c[3], 'no_scale': c[4], 'remark': c[5]}
                              for c in new_cats],
           'definitions': {str(key_id.get(k, k)): v for k, v in defs.items() if v},
           'moves': {str(p): [cur[p], c, why.get(p)] for p, c in moved.items()},
           'renames': renames, 'no_scale': scale, 'close': sorted(close),
           'map_changes': {c: list(v) for c, v in map_changes.items()}, 'rule_codes': sorted(rule_codes),
           'final_map': {c: list(v) for c, v in new_map.items()},
           'final_rules': {c: [list(r) for r in sorted(new_rules[c])] for c in sorted(rule_codes)},
           'final_manual': {str(p): c for p, c in new_man.items()},
           'final_category': {str(p): c for p, c in desired.items()},
           'manual_rows': len(man_rows), 'moved': len(moved),
           'moves_by_pair': collections.Counter(f'{cur[p]}→{c}' for p, c in moved.items()).most_common()}
    REPORT.write_text(json.dumps(rep, ensure_ascii=False, indent=1), encoding='utf-8')
    print(f'новых полок {len(new_cats)}, переименований {len(renames)}, закрыто {len(close)}, кодов {len(map_changes)}, '
          f'правил по кодам {len(rule_codes)}, поштучно {len(man_rows)}, перенесено товаров {len(moved)}')


if __name__ == '__main__':
    main()
