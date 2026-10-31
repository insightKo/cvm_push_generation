# Снимок для аудита витрин: настройки сервера, команды шагов задания, тексты процедур шагов и вложенных вызовов,
# их зависимости от таблиц, размеры этих таблиц. Только метаданные; ревью sql-senior 31.10.2026.
# Запуск (учётка cvm-api, только чтение), вывод — в файл, потом unpack_catalog.py:
#   ssh kz 'ssh ru "docker exec -i cvm-api python -"' < catalog_snapshot.py > catalog.out
# Во время загрузки и прохода (01:00–18:40) каталог частично под блокировками: всё точечно по OBJECT_ID,
# lock_timeout 5 с, заблокированные таблицы пропускаются (список в выводе). Широкие запросы по всей базе
# (список всех таблиц, размеры по sys.partitions целиком) в это время упираются в ошибку 1222 — не делать.
JOB = 'MCI Regular update'
STEP_FROM, STEP_TO = 18, 56          # шаги витрин; 1–17 — интеграция MCI, её процедуры не разбираем
SKIP = {'clear_data_update', 'shop_processing', 'products_processing', 'check_product_update', 'campaignes_processing',
        'rules_processing', 'check_rules_processing', 'check_checkheader_processing', 'check_checkheader_processing_ea',
        'card_processing_new', 'price_processing', 'check_insert', 'contacts_processing', 'mistake_contact',
        'communication_processing', 'sp_send_dbmail'}
BIG = ('I_CHECK', 'I_CHECKHEADER', 'I_CHECK_MISSION', 'I_PROMO_DELIVERY_REPORT', 'I_PROMO_OFFER', 'I_CVM_CONTACT',
       'I_CONTACT', 'I_CHECK_DELTA')

import sys, json, re, pyodbc
from app.config import Settings

cn = pyodbc.connect(Settings.from_env().connection_string(), timeout=30, autocommit=True)
cn.timeout = 60
cur = cn.cursor()
cur.execute("set transaction isolation level read uncommitted; set lock_timeout 5000; set deadlock_priority low")


def rows(sql, params=(), n=5000):
    cur.execute(sql, params)
    cols = [c[0] for c in cur.description]
    out = []
    while len(out) < n:
        part = cur.fetchmany(500)
        if not part:
            break
        out += [dict(zip(cols, [v if isinstance(v, (int, float)) or v is None else str(v) for v in r])) for r in part]
    return out


def log(*a):
    print(*a)
    sys.stdout.flush()


res = {}
# 1. права и настройки сервера (метаданные, без DMV)
res['perms'] = rows("""select has_perms_by_name(null, null, 'VIEW SERVER STATE') vss,
                              has_perms_by_name('mci_model', 'DATABASE', 'VIEW DATABASE STATE') vds,
                              has_perms_by_name('mci_model', 'DATABASE', 'VIEW DEFINITION') vdef""")
res['config'] = rows("""select name, cast(value_in_use as bigint) value_in_use from sys.configurations
                        where name in ('max degree of parallelism','cost threshold for parallelism',
                                       'max server memory (MB)','min memory per query (KB)','optimize for ad hoc workloads')""")
res['dbs'] = rows("""select name, recovery_model_desc, compatibility_level, is_read_committed_snapshot_on,
                            snapshot_isolation_state_desc, is_auto_update_stats_async_on, page_verify_option_desc
                     from sys.databases where name in ('mci_model','tempdb')""")
res['tempdb_files'] = rows("select type_desc, count(*) n, sum(size)/128 size_mb from tempdb.sys.database_files group by type_desc")
res['version'] = rows("""select cast(serverproperty('Edition') as nvarchar(200)) edition,
                                cast(serverproperty('ProductVersion') as nvarchar(50)) ver,
                                cast(serverproperty('ProductUpdateLevel') as nvarchar(50)) cu""")
log('server ok')

# 2. тексты процедур шагов и вложенных вызовов — точечно по OBJECT_ID, без поиска по sys.sql_modules
res['steps'] = [{k: s[k] for k in ('step_id', 'step_name', 'command')}
                for s in rows(f"exec msdb.dbo.sp_help_jobstep @job_name = N'{JOB}'")]
pat = re.compile(r'\bexec(?:ute)?\s+(?:@\w+\s*=\s*)?(?:\[?(\w+)\]?\.)?\[?(\w+)\]?', re.I)
queue = [((m.group(1) or 'dbo'), m.group(2))
         for s in res['steps'] if STEP_FROM <= int(s['step_id']) <= STEP_TO
         for m in pat.finditer(s['command'] or '')]
procs, seen = {}, set()
while queue and len(procs) < 150:
    sch, name = queue.pop(0)
    key = (sch.lower(), name.lower())
    if key in seen or name.lower() in SKIP:
        continue
    seen.add(key)
    full = f'mci_model.{sch}.{name}'
    r = rows("select object_id(?) oid, object_definition(object_id(?)) def", (full, full))
    if not r or r[0]['oid'] is None or r[0]['def'] is None:
        continue
    procs[f'{sch}.{name}'] = r[0]['def']
    queue += [((m.group(1) or 'dbo'), m.group(2)) for m in pat.finditer(r[0]['def'])]
res['procs'] = procs
log('procs', len(procs), sum(len(v) for v in procs.values()))

# 3. зависимости процедур от таблиц (без dynamic SQL; чтение/запись не различаются — определять по тексту)
deps = []
for p in procs:
    deps += [dict(proc=p, **d) for d in rows("""
        select coalesce(referenced_schema_name, 'dbo') ref_sch, referenced_entity_name ref_name
        from mci_model.sys.sql_expression_dependencies with (nolock) where referencing_id = object_id(?)""",
                                              (f'mci_model.{p}',), n=2000)]
res['deps'] = deps
log('deps', len(deps))
print('JSON1>>' + json.dumps(res, ensure_ascii=False, default=str))
sys.stdout.flush()

# 4. размеры таблиц, на которые ссылаются процедуры: по одной, заблокированные пропускаем
names = sorted({(d['ref_sch'] or 'dbo', d['ref_name']) for d in deps} | {('dbo', x) for x in BIG})
tabs, locked = [], []
for sch, name in names:
    try:
        r = rows("""
declare @o int = object_id(?);
select @o oid, objectproperty(@o, 'IsUserTable') is_table,
       (select sum(p.rows) from mci_model.sys.partitions p with (nolock) where p.object_id = @o and p.index_id in (0,1)) row_cnt,
       (select sum(au.total_pages) / 128 from mci_model.sys.partitions p with (nolock)
          join mci_model.sys.allocation_units au with (nolock) on au.container_id = p.partition_id where p.object_id = @o) total_mb,
       (select string_agg(i.type_desc, ',') from mci_model.sys.indexes i with (nolock) where i.object_id = @o) idx_types,
       (select count(distinct p.partition_number) from mci_model.sys.partitions p with (nolock) where p.object_id = @o) parts""",
                 (f'mci_model.{sch}.{name}',))[0]
        if r['oid'] is not None and r['is_table'] == 1:
            tabs.append(dict(r, sch=sch, tbl=name))
    except pyodbc.Error:
        locked.append(f'{sch}.{name}')
log('tables', len(tabs), 'locked', locked)
print('JSON2>>' + json.dumps({'tables': tabs, 'locked': locked}, ensure_ascii=False, default=str))
