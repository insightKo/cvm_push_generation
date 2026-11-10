# План декабря 2026 в мере ГКГ: что лежит в витринах ГКГ (только SELECT, nolock, малые витрины).
import sys, json, datetime, decimal
import pyodbc
from app.config import Settings
cn = pyodbc.connect(Settings.from_env().connection_string(), timeout=30)
cn.timeout = 120
cur = cn.cursor()
def conv(v):
    if isinstance(v, decimal.Decimal): return float(v)
    if isinstance(v, (datetime.date, datetime.datetime)): return v.isoformat()
    return v
def dump(t, sql):
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    for r in cur.fetchmany(3000):
        d = {'t': t}; d.update({c: conv(v) for c, v in zip(cols, r)})
        print(json.dumps(d, ensure_ascii=False))
dump('tables', "select t.name, p.rows from sys.tables t join sys.partitions p on p.object_id = t.object_id and p.index_id in (0,1) where t.name like 'I[_]GCG%' or t.name like '%GLOBAL[_]CG%' or t.name like 'I[_]CVM[_]EFFECT%'")
dump('eff_by_month', "select ym = convert(char(6), [Начало], 112), n = count(*) from dbo.I_CVM_EFFECT (nolock) where CHANNEL_ID = 0 group by convert(char(6), [Начало], 112) order by 1")
dump('gcg_cols', "select c.name, ty.name as type from sys.columns c join sys.types ty on ty.user_type_id = c.user_type_id where c.object_id = object_id('dbo.I_GCG_REPORT') order by c.column_id")
dump('job', """select top 3 a.run_requested_date, a.start_execution_date, a.stop_execution_date, a.last_executed_step_id, a.last_executed_step_date
 from msdb.dbo.sysjobactivity a (nolock) join msdb.dbo.sysjobs j (nolock) on j.job_id = a.job_id where j.name = 'MCI Regular update' order by a.run_requested_date desc""")
