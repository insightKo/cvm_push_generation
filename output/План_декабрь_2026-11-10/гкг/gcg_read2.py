# План декабря 2026 в мере ГКГ: витрины ГКГ целиком (малые: 18 / 246 / 3 строки) и состояние витрины эффекта. Только SELECT, nolock.
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
dump('eff_by_month', "select ym = convert(char(6), [Дата старта], 112), n = count(*) from dbo.I_CVM_EFFECT (nolock) where CHANNEL_ID = 0 group by convert(char(6), [Дата старта], 112) order by 1")
dump('gcg', "select * from dbo.I_GCG_REPORT (nolock)")
dump('layer', "select * from dbo.I_GCG_REPORT_LAYER (nolock)")
dump('batch', "select * from dbo.I_GCG_BATCH (nolock)")
dump('job', """select top 3 a.run_requested_date, a.start_execution_date, a.stop_execution_date, a.last_executed_step_id, a.last_executed_step_date
 from msdb.dbo.sysjobactivity a (nolock) join msdb.dbo.sysjobs j (nolock) on j.job_id = a.job_id where j.name = 'MCI Regular update' order by a.run_requested_date desc""")
