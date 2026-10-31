# CVM_PRODUCT_UNIT_LOAD.sql — разметка единиц из названий в dbo.I_CVM_PRODUCT_UNIT (SOURCE = 1).
# Вход: TSV от cvm_product_unit_parse.py (ID_PRODUCT, UNIT_NAME, QTY_PER_PIECE, PIECES, IS_MULTIPACK, QTY_TOTAL,
#       QTY_ALT_TOTAL, IS_WEIGHT_NAME, STATUS, NOTE). Берутся строки STATUS = ok.
# Скрипт для обменника (повторяется каждые 10 минут, пока в манифесте): идемпотентный — если есть строки «по названию»
# с датой не раньше даты сборки, ничего не делает (вторая сборка в тот же день не зальётся — пересобирать на следующий
# день). Ручная разметка (SOURCE = 2) не трогается.
import csv
import datetime
import hashlib
from decimal import Decimal
import sys
from pathlib import Path

OUT = Path(__file__).resolve().parent
DST = OUT / 'CVM_PRODUCT_UNIT_LOAD.sql'
BATCH = 1000


PLAIN = {'объём из названия', 'одна штука', 'штук из названия', 'весовой'}


def note(t):
    # типовые случаи без пояснения — короче скрипт для обменника
    return None if t in PLAIN else t[:500]


def d(v):
    # фиксированная точка: «3e-06» в VALUES T-SQL прочитал бы как float
    return 'null' if v in (None, '') else format(Decimal(v), 'f')


def q(v):
    return 'null' if v in (None, '') else "N'" + str(v).replace("'", "''") + "'"


def main(src):
    rows = [r for r in csv.DictReader(open(src), delimiter='\t') if r['STATUS'] == 'ok']
    n = len(rows)
    digest = hashlib.sha256(Path(src).read_bytes()).hexdigest()[:12]
    build = datetime.date.today().strftime('%Y%m%d')
    L = [f'''-- ============================================================================================
-- CVM_PRODUCT_UNIT_LOAD — разметка единиц из названий товаров в dbo.I_CVM_PRODUCT_UNIT (источник {digest}).
-- Для обменника: повторный запуск ничего не меняет. Защита — по дате сборки {build}: если строки «по названию» с этой
-- датой или позже уже есть, скрипт ничего не делает (вторая сборка в тот же день не зальётся).
--
-- Действия по объектам:
--   dbo.I_CVM_PRODUCT_UNIT — колонки: + UNIT_NAME (единица названия: кг / л / шт), + QTY_ALT_TOTAL (вторая единица
--                            с двойной этикетки «680Г/720МЛ»); QTY_PER_PIECE и QTY_TOTAL — decimal(14, 6) (было (12, 4):
--                            пакетик чая 1,33 г = 0,00133 кг терял точность). Таблица до заливки пустая.
--                            Строки SOURCE = 1 (по названию) удаляются и вставляются заново: {n} товаров.
--                            Строки SOURCE = 2 (вручную) не трогаются.
--   Ничего больше не меняется.
-- Количества — в единице названия UNIT_NAME; перевод кг ↔ л на полке — в модели ценового индекса (решение 23.10.2026).
-- ============================================================================================

use mci_model;
go
set noexec off;
set nocount on;
set xact_abort on;
set lock_timeout 60000;   -- не ждать блокировку дольше минуты: ошибка → откат → обменник повторит через 10 минут
go

-- 0. Строки «по названию» с датой сборки {build} или позже уже есть — ничего не делать (повтор обменника, следующие дни)
if col_length(N'dbo.I_CVM_PRODUCT_UNIT', N'UNIT_NAME') is not null
   and exists (select 1 from dbo.I_CVM_PRODUCT_UNIT (nolock) where SOURCE = 1 and DATE_UPDATE >= '{build}')
begin
    raiserror(N'Разметка единиц уже залита {build} или позже — ничего не делаю (источник {digest}).', 10, 1) with nowait;
    set noexec on;
end;
go

-- 1. Колонки (таблица пустая или только наши строки — изменение типов безопасно)
if col_length(N'dbo.I_CVM_PRODUCT_UNIT', N'UNIT_NAME') is null
    alter table dbo.I_CVM_PRODUCT_UNIT add UNIT_NAME nvarchar(3) null
        constraint CK_I_CVM_PRODUCT_UNIT_UNIT check (UNIT_NAME in (N'кг', N'л', N'шт'));
if col_length(N'dbo.I_CVM_PRODUCT_UNIT', N'QTY_ALT_TOTAL') is null
    alter table dbo.I_CVM_PRODUCT_UNIT add QTY_ALT_TOTAL decimal(14, 6) null;
go
if exists (select 1 from sys.columns (nolock) where object_id = object_id(N'dbo.I_CVM_PRODUCT_UNIT') and name = N'QTY_PER_PIECE' and scale < 6)
    alter table dbo.I_CVM_PRODUCT_UNIT alter column QTY_PER_PIECE decimal(14, 6) null;
if exists (select 1 from sys.columns (nolock) where object_id = object_id(N'dbo.I_CVM_PRODUCT_UNIT') and name = N'QTY_TOTAL' and scale < 6)
    alter table dbo.I_CVM_PRODUCT_UNIT alter column QTY_TOTAL decimal(14, 6) null;
go

-- 2. Заливка одной транзакцией
drop table if exists #u;
create table #u (ID_PRODUCT bigint not null primary key, UNIT_NAME nvarchar(3) not null, QTY_PER_PIECE decimal(14, 6) not null,
                 PIECES int not null, IS_MULTIPACK bit not null, QTY_TOTAL decimal(14, 6) not null,
                 QTY_ALT_TOTAL decimal(14, 6) null, IS_WEIGHT_NAME bit not null, NOTE nvarchar(500) null);
''']
    for i in range(0, n, BATCH):
        part = rows[i:i + BATCH]
        L.append('insert into #u (ID_PRODUCT, UNIT_NAME, QTY_PER_PIECE, PIECES, IS_MULTIPACK, QTY_TOTAL, QTY_ALT_TOTAL, IS_WEIGHT_NAME, NOTE)\nvalues\n'
                 + ',\n'.join(f"    ({r['ID_PRODUCT']}, {q(r['UNIT_NAME'])}, {d(r['QTY_PER_PIECE'])}, {r['PIECES']}, {r['IS_MULTIPACK']}, "
                              f"{d(r['QTY_TOTAL'])}, {d(r['QTY_ALT_TOTAL'])}, {r['IS_WEIGHT_NAME']}, {q(note(r['NOTE']))})"
                              for r in part) + ';\n')
    L.append(f'''
if (select count(*) from #u) <> {n}
begin
    raiserror(N'СТОП: во временной таблице не {n} строк — ничего не изменено.', 16, 1);
    set noexec on;
end;
go

begin transaction;
delete from dbo.I_CVM_PRODUCT_UNIT where SOURCE = 1;
insert into dbo.I_CVM_PRODUCT_UNIT (ID_PRODUCT, UNIT_NAME, QTY_PER_PIECE, PIECES, IS_MULTIPACK, QTY_TOTAL, QTY_ALT_TOTAL,
                                    IS_WEIGHT_NAME, SOURCE, DATE_UPDATE, NOTE)
select u.ID_PRODUCT, u.UNIT_NAME, u.QTY_PER_PIECE, u.PIECES, u.IS_MULTIPACK, u.QTY_TOTAL, u.QTY_ALT_TOTAL,
       u.IS_WEIGHT_NAME, 1, cast(getdate() as date), u.NOTE
from   #u as u
where  not exists (select 1 from dbo.I_CVM_PRODUCT_UNIT as x (nolock) where x.ID_PRODUCT = u.ID_PRODUCT);   -- ручные (SOURCE 2) главнее
commit transaction;
go

-- 3. Итог
select CHECK_NAME = N'Строк по названию (SOURCE 1)', CNT = count(*) from dbo.I_CVM_PRODUCT_UNIT (nolock) where SOURCE = 1
union all select N'Строк вручную (SOURCE 2)', count(*) from dbo.I_CVM_PRODUCT_UNIT (nolock) where SOURCE = 2
union all select N'Единица названия ≠ единица полки', count(*)
from   dbo.I_CVM_PRODUCT_UNIT as u (nolock)
       join dbo.I_CVM_PRODUCT_CATEGORY as t (nolock) on t.ID_PRODUCT = u.ID_PRODUCT
       join dbo.I_CVM_CATEGORY as c (nolock) on c.ID_CVM_CATEGORY = t.ID_CVM_CATEGORY
where  u.UNIT_NAME <> c.UNIT
union all select N'С двойной этикеткой', count(*) from dbo.I_CVM_PRODUCT_UNIT (nolock) where QTY_ALT_TOTAL is not null
union all select N'Весовые по названию', count(*) from dbo.I_CVM_PRODUCT_UNIT (nolock) where IS_WEIGHT_NAME = 1;
go
set noexec off;
go
''')
    DST.write_text(''.join(L), encoding='utf-8-sig')
    print('строк', n, 'файл', DST.name, round(DST.stat().st_size / 1e6, 1), 'МБ')


if __name__ == '__main__':
    main(sys.argv[1])
