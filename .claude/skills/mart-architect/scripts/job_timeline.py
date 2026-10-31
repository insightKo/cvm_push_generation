# Хронометраж задания SQL Agent по истории msdb: старт/длительность каждого шага в каждом сохранённом прогоне.
# Запуск (только чтение, учётка cvm-api):
#   ssh kz 'ssh ru "docker exec -i cvm-api python -"' < job_timeline.py
# История агента — ~100 строк на задание (≈ 2 последних прогона): дни 1–10 и первые пятницы снимать в эти дни,
# вывод сохранять в output/ (не в базу). Имя задания и порог «показывать шаги от N секунд» — ниже.
JOB = 'MCI Regular update'
MIN_SEC = 60

import sys, collections, pyodbc
from app.config import Settings

cn = pyodbc.connect(Settings.from_env().connection_string(), timeout=30, autocommit=True)
cn.timeout = 90
cur = cn.cursor()
cur.execute("set transaction isolation level read uncommitted; set lock_timeout 5000; set deadlock_priority low")


def fetch(sql):
    cur.execute(sql)
    cols = [c[0] for c in cur.description]
    out = []
    while True:
        part = cur.fetchmany(200)
        if not part:
            return out
        out += [dict(zip(cols, r)) for r in part]


def sec(hhmmss):
    x = int(hhmmss or 0)
    return x // 10000 * 3600 + x // 100 % 100 * 60 + x % 100


def hms(s):
    return f'{s // 3600}:{s // 60 % 60:02d}:{s % 60:02d}'


act = fetch(f"exec msdb.dbo.sp_help_jobactivity @job_name = N'{JOB}'")
if act:
    a = act[0]
    print('активность:', 'старт', a['start_execution_date'], '| последний шаг', a['last_executed_step_id'],
          a['last_executed_step_date'], '| конец', a['stop_execution_date'])

hist = fetch(f"exec msdb.dbo.sp_help_jobhistory @job_name = N'{JOB}', @mode = 'FULL'")
runs = collections.defaultdict(dict)
for r in hist:
    runs[r['run_date']][r['step_id']] = (int(r['run_time']), sec(r['run_duration']), r['run_status'], r['step_name'])

total = collections.Counter()
for day in sorted(runs):
    steps = runs[day]
    head = steps.get(0)
    print(f'\n== прогон {day}: шагов в истории {len([s for s in steps if s])}',
          f'| итог {hms(head[1])} статус {head[2]}' if head else '| ещё идёт или итог не в истории')
    for st in sorted(s for s in steps if s):
        t, d, status, name = steps[st]
        total[(st, name)] += d
        if d >= MIN_SEC or status != 1:
            print(f'  {st:>3} {name[:32]:32} старт {t:06d} длит {hms(d)}' + ('' if status == 1 else f' СТАТУС {status}'))

print('\n== топ-15 шагов по сумме длительностей в истории')
for (st, name), d in total.most_common(15):
    print(f'  {st:>3} {name[:32]:32} {hms(d)}')
sys.stdout.flush()
