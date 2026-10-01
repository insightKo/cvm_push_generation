#!/usr/bin/env python3
"""
Склейка одного файла загрузки: DDL + данные (copy … from stdin) одной транзакцией.
Так справочник уезжает на ru за один заход, и при любой ошибке база остаётся как была.

Запуск: python3 output/assortment/make_load_sql.py  → output/assortment/assortment_load.sql
"""
from pathlib import Path

D = Path(__file__).resolve().parent
TABLES = ('category', 'product', 'product_city')

parts = [
    '-- Загрузка справочника ассортимента в базу портала: DDL + данные одной транзакцией.\n',
    '-- Файл собирается make_load_sql.py, руками не правится. Пересборка идемпотентна:\n',
    '-- три таблицы очищаются и наполняются заново, остальная схема cvm не трогается.\n\n',
    (D / '004_assortment.sql').read_text(encoding='utf-8'),
    '\n\nbegin;\n\ntruncate cvm.product_city, cvm.product, cvm.category;\n\n',
]

for tbl in TABLES:
    head, data = (D / f'{tbl}.csv').read_text(encoding='utf-8').split('\n', 1)
    parts.append(f'copy cvm.{tbl} ({head.strip()}) from stdin with (format csv);\n')
    parts.append(data.rstrip('\n') + '\n\\.\n\n')

parts.append('commit;\n\n')
parts.append('analyze cvm.category;\nanalyze cvm.product;\nanalyze cvm.product_city;\n\n')
parts.append("""select 'категории' as таблица, count(*) as строк from cvm.category
union all select 'товары', count(*) from cvm.product
union all select 'товары по городам', count(*) from cvm.product_city;
""")

out = D / 'assortment_load.sql'
out.write_text(''.join(parts), encoding='utf-8')
print(f'{out} — {out.stat().st_size / 1e6:.1f} МБ')
