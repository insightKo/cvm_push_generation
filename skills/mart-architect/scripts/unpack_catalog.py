# Распаковка вывода catalog_snapshot.py локально (в scratchpad сессии, не в репозиторий и не на kz):
#   python3 unpack_catalog.py catalog.out <папка>
# В папке: procs/<схема.имя>.sql, steps.txt, deps.csv, tables.csv (по убыванию размера), server.txt.
import sys, os, csv, json

src, dst = sys.argv[1], sys.argv[2]
lines = open(src, encoding='utf-8').read().splitlines()
j1 = json.loads(next(l for l in lines if l.startswith('JSON1>>'))[7:])
j2 = json.loads(next(l for l in lines if l.startswith('JSON2>>'))[7:])
os.makedirs(os.path.join(dst, 'procs'), exist_ok=True)
for name, txt in j1['procs'].items():
    open(os.path.join(dst, 'procs', f'{name}.sql'), 'w', encoding='utf-8').write(txt.replace('\r\n', '\n'))
with open(os.path.join(dst, 'deps.csv'), 'w', encoding='utf-8', newline='') as f:
    w = csv.writer(f)
    w.writerow(['proc', 'ref_sch', 'ref_name'])
    for d in j1['deps']:
        w.writerow([d['proc'], d['ref_sch'], d['ref_name']])
with open(os.path.join(dst, 'tables.csv'), 'w', encoding='utf-8', newline='') as f:
    w = csv.writer(f)
    w.writerow(['table', 'rows', 'total_mb', 'index_types', 'partitions'])
    for t in sorted(j2['tables'], key=lambda t: -(t['total_mb'] or 0)):
        w.writerow([f"{t['sch']}.{t['tbl']}", t['row_cnt'], t['total_mb'], t['idx_types'], t['parts']])
    for name in j2['locked']:
        w.writerow([name, 'заблокирована при чтении (идёт запись)', '', '', ''])
with open(os.path.join(dst, 'server.txt'), 'w', encoding='utf-8') as f:
    for k in ('perms', 'config', 'dbs', 'tempdb_files', 'version'):
        f.write(f'{k}: {json.dumps(j1[k], ensure_ascii=False)}\n')
with open(os.path.join(dst, 'steps.txt'), 'w', encoding='utf-8') as f:
    for s in j1['steps']:
        f.write(f"=== шаг {s['step_id']} {s['step_name']}\n{(s['command'] or '').strip()}\n")
print('процедур', len(j1['procs']), '| зависимостей', len(j1['deps']), '| таблиц', len(j2['tables']),
      '| заблокированы', j2['locked'])
