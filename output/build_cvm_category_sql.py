# CVM_CATEGORY_REFERENCE.sql — справочник категорий CVM в mci_model: таблицы, первая заливка из
# справочник_категорий_CVM.xlsx, ежедневная процедура cvm_category_refresh и её шаг в «MCI Regular update».
# Пересборка после правок дерева: build_cvm_category_reference.py → этот скрипт.
from pathlib import Path

import openpyxl

OUT = Path(__file__).resolve().parent
SRC = OUT / 'справочник_категорий_CVM.xlsx'
DST = OUT / 'CVM_CATEGORY_REFERENCE.sql'
BATCH = 500
AFTER_STEP = 'Check Product Update'   # шаг ставится сразу после загрузки товаров

METHOD = {'целиком': 1, 'по словам': 2, 'по товарам': 3}


def code(v):
    return str(int(v)) if isinstance(v, (int, float)) else str(v)


def q(v):
    if v is None or v == '':
        return 'null'
    if isinstance(v, (int, float)):
        return str(v)
    return "N'" + str(v).replace("'", "''") + "'"


def inserts(table, cols, rows):
    out = []
    for i in range(0, len(rows), BATCH):
        part = rows[i:i + BATCH]
        out.append(f'insert into {table} ({", ".join(cols)})\nvalues\n'
                   + ',\n'.join('    (' + ', '.join(q(v) for v in r) + ')' for r in part) + ';\n')
    return '\n'.join(out)


TABLES = '''
-- 1. Таблицы (создаются, если их нет)
if object_id(N'dbo.I_CVM_CATEGORY') is null
create table dbo.I_CVM_CATEGORY
(
    ID_CVM_CATEGORY int            not null constraint PK_I_CVM_CATEGORY primary key clustered,
    NAME            nvarchar(200)  not null,
    NODE_TYPE       tinyint        not null constraint CK_I_CVM_CATEGORY_NODE check (NODE_TYPE in (1, 2)),
    ID_PARENT       int            null,
    UNIT            nvarchar(3)    null constraint CK_I_CVM_CATEGORY_UNIT check (UNIT in (N'кг', N'л', N'шт')),
    NO_SCALE        bit            not null constraint DF_I_CVM_CATEGORY_NO_SCALE default (0),
    STATUS          tinyint        not null constraint DF_I_CVM_CATEGORY_STATUS default (1),
    DATE_CREATE     date           not null constraint DF_I_CVM_CATEGORY_DC default (cast(getdate() as date)),
    DATE_CLOSE      date           null,
    NOTE            nvarchar(1000) null,
    constraint CK_I_CVM_CATEGORY_LEVEL check ((NODE_TYPE = 1 and ID_PARENT is null and UNIT is null)
                                           or (NODE_TYPE = 2 and ID_PARENT is not null and UNIT is not null))
);
go
if object_id(N'dbo.I_CVM_CATEGORY_MAP') is null
create table dbo.I_CVM_CATEGORY_MAP
(
    ID_CATEGORY_5_ext nvarchar(50)   not null constraint PK_I_CVM_CATEGORY_MAP primary key clustered,
    EXT_NAME          nvarchar(255)  null,
    MAP_METHOD        tinyint        null constraint CK_I_CVM_CATEGORY_MAP_METHOD check (MAP_METHOD in (1, 2, 3)),
    ID_CVM_CATEGORY   int            null,
    STATUS            tinyint        not null constraint CK_I_CVM_CATEGORY_MAP_STATUS check (STATUS in (0, 1, 2)),
    DATE_CREATE       date           not null constraint DF_I_CVM_CATEGORY_MAP_DC default (cast(getdate() as date)),
    DATE_UPDATE       date           null,
    NOTE              nvarchar(1000) null,
    constraint CK_I_CVM_CATEGORY_MAP_TARGET check ((MAP_METHOD = 1 and ID_CVM_CATEGORY is not null)
                                                or (MAP_METHOD in (2, 3) and ID_CVM_CATEGORY is null)
                                                or (MAP_METHOD is null and ID_CVM_CATEGORY is null and STATUS = 2))
);
go
if object_id(N'dbo.I_CVM_CATEGORY_RULE') is null
create table dbo.I_CVM_CATEGORY_RULE
(
    ID_CATEGORY_5_ext nvarchar(50)   not null,
    RULE_ORDER        tinyint        not null,
    WORDS_ANY         nvarchar(1000) null,   -- через «;»; пусто = всё остальное
    WORDS_NONE        nvarchar(1000) null,   -- через «;»
    ID_CVM_CATEGORY   int            not null,
    DATE_CREATE       date           not null constraint DF_I_CVM_CATEGORY_RULE_DC default (cast(getdate() as date)),
    constraint PK_I_CVM_CATEGORY_RULE primary key clustered (ID_CATEGORY_5_ext, RULE_ORDER)
);
go
if object_id(N'dbo.I_CVM_PRODUCT_CATEGORY_MANUAL') is null
create table dbo.I_CVM_PRODUCT_CATEGORY_MANUAL
(
    ID_PRODUCT      bigint         not null constraint PK_I_CVM_PRODUCT_CATEGORY_MANUAL primary key clustered,
    ID_CVM_CATEGORY int            not null,
    SOURCE          tinyint        not null constraint CK_I_CVM_PRODUCT_CATEGORY_MANUAL_SRC check (SOURCE in (1, 2)),
    DATE_UPDATE     date           not null constraint DF_I_CVM_PRODUCT_CATEGORY_MANUAL_DU default (cast(getdate() as date)),
    NOTE            nvarchar(500)  null
);
go
if object_id(N'dbo.I_CVM_PRODUCT_CATEGORY') is null
create table dbo.I_CVM_PRODUCT_CATEGORY
(
    ID_PRODUCT        bigint       not null constraint PK_I_CVM_PRODUCT_CATEGORY primary key clustered,
    ID_CVM_CATEGORY   int          not null,
    ID_CVM_GROUP      int          not null,
    ID_CATEGORY_5_ext nvarchar(50) null,
    HOW               tinyint      not null constraint CK_I_CVM_PRODUCT_CATEGORY_HOW check (HOW in (0, 1, 2, 3)),
    DATE_UPDATE       date         not null
);
go
if object_id(N'dbo.I_CVM_PRODUCT_UNIT') is null
create table dbo.I_CVM_PRODUCT_UNIT
(
    ID_PRODUCT      bigint         not null constraint PK_I_CVM_PRODUCT_UNIT primary key clustered,
    QTY_PER_PIECE   decimal(12, 4) null,     -- объём одной штуки в единице полки (кг / л / шт)
    PIECES          int            null,     -- число штук в товаре (мультипак, «Х6», «10 ШТ»)
    IS_MULTIPACK    bit            not null constraint DF_I_CVM_PRODUCT_UNIT_MP default (0),
    QTY_TOTAL       decimal(12, 4) null,     -- всего в единице полки = QTY_PER_PIECE × PIECES; цена за единицу = цена / QTY_TOTAL
    IS_WEIGHT_NAME  bit            not null constraint DF_I_CVM_PRODUCT_UNIT_W default (0),  -- весовой по названию
    SOURCE          tinyint        not null constraint CK_I_CVM_PRODUCT_UNIT_SRC check (SOURCE in (1, 2)),
    DATE_UPDATE     date           not null constraint DF_I_CVM_PRODUCT_UNIT_DU default (cast(getdate() as date)),
    NOTE            nvarchar(500)  null
);
go
'''

CONTROL = '''
-- названия кодов, которых нет в файле, — из справочника ext-названий
update m
set    m.EXT_NAME = (select max(e.I_CATEGORY_5_ext) from dbo.I_CATEGORY_EXT as e (nolock)
                     where e.ID_COMPANY = 1 and e.ID_CATEGORY_5_ext = m.ID_CATEGORY_5_ext)
from   dbo.I_CVM_CATEGORY_MAP as m
where  m.EXT_NAME is null;

-- контроль ссылок: при любом нарушении — список нарушителей, откат, таблицы пустые
declare @bad table (PROBLEM nvarchar(100), KEY_VALUE nvarchar(50));
insert into @bad (PROBLEM, KEY_VALUE)
select N'категория без группы', cast(c.ID_CVM_CATEGORY as nvarchar(50))
from   dbo.I_CVM_CATEGORY as c (nolock)
where  c.NODE_TYPE = 2
  and  not exists (select 1 from dbo.I_CVM_CATEGORY as g (nolock) where g.ID_CVM_CATEGORY = c.ID_PARENT and g.NODE_TYPE = 1)
union all
select N'код ведёт не на категорию', m.ID_CATEGORY_5_ext
from   dbo.I_CVM_CATEGORY_MAP as m (nolock)
where  m.ID_CVM_CATEGORY is not null
  and  not exists (select 1 from dbo.I_CVM_CATEGORY as c (nolock) where c.ID_CVM_CATEGORY = m.ID_CVM_CATEGORY and c.NODE_TYPE = 2)
union all
select N'правило у кода не «по словам» или не на категорию', r.ID_CATEGORY_5_ext
from   dbo.I_CVM_CATEGORY_RULE as r (nolock)
where  not exists (select 1 from dbo.I_CVM_CATEGORY as c (nolock) where c.ID_CVM_CATEGORY = r.ID_CVM_CATEGORY and c.NODE_TYPE = 2)
   or  not exists (select 1 from dbo.I_CVM_CATEGORY_MAP as m (nolock) where m.ID_CATEGORY_5_ext = r.ID_CATEGORY_5_ext and m.MAP_METHOD = 2)
union all
select N'товар ведёт не на категорию', cast(p.ID_PRODUCT as nvarchar(50))
from   dbo.I_CVM_PRODUCT_CATEGORY_MANUAL as p (nolock)
where  not exists (select 1 from dbo.I_CVM_CATEGORY as c (nolock) where c.ID_CVM_CATEGORY = p.ID_CVM_CATEGORY and c.NODE_TYPE = 2);

if exists (select 1 from @bad)
begin
    rollback transaction;
    select PROBLEM, KEY_VALUE from @bad order by PROBLEM, KEY_VALUE;
    raiserror(N'СТОП: нарушены ссылки справочника (список выше) — заливка отменена, таблицы пустые.', 16, 1);
    return;
end;

commit transaction;
go
set noexec off;
go
'''

PROC = '''
-- 3. Ежедневная процедура: новые коды ДИКСИ → «несопоставлен»; каждый товар I_PRODUCT → наша категория
create or alter procedure dbo.cvm_category_refresh
as
begin
    set nocount on;
    set xact_abort on;

    declare @today date = cast(getdate() as date), @unmapped int, @new_codes int, @upd int, @ins int;

    select @unmapped = c.ID_CVM_CATEGORY
    from   dbo.I_CVM_CATEGORY as c (nolock)
    where  c.NODE_TYPE = 2 and c.NAME = N'Несопоставленные' and c.STATUS = 1;
    if @unmapped is null
    begin
        raiserror(N'СТОП: в справочнике нет категории «Несопоставленные» — ничего не изменено.', 16, 1);
        return;
    end;

    -- товары: одна строка на товар; название — верхний регистр, Ё = Е, пробел в начале для поиска «с начала слова»
    drop table if exists #p;
    select ID_PRODUCT        = cast(p.ID_PRODUCT as bigint)
         , ID_CATEGORY_5_ext = max(cast(p.ID_CATEGORY_5_ext as nvarchar(50)))
         , NNAME             = N' ' + replace(upper(max(cast(p.PRODUCT_NAME as nvarchar(500)))), N'Ё', N'Е')
    into   #p
    from   dbo.I_PRODUCT as p (nolock)
    where  p.ID_COMPANY = 1 and p.ID_PRODUCT is not null
    group by cast(p.ID_PRODUCT as bigint);
    create clustered index IX_p on #p (ID_CATEGORY_5_ext, ID_PRODUCT);

    -- слова правил по одному в строке: A — «любое из», N — «ни одного из»
    drop table if exists #w;
    select r.ID_CATEGORY_5_ext, r.RULE_ORDER, KIND = 'A', WORD = replace(upper(ltrim(rtrim(s.value))), N'Ё', N'Е')
    into   #w
    from   dbo.I_CVM_CATEGORY_RULE as r (nolock)
           cross apply string_split(r.WORDS_ANY, N';') as s
    where  ltrim(rtrim(s.value)) <> N''
    union all
    select r.ID_CATEGORY_5_ext, r.RULE_ORDER, 'N', replace(upper(ltrim(rtrim(s.value))), N'Ё', N'Е')
    from   dbo.I_CVM_CATEGORY_RULE as r (nolock)
           cross apply string_split(r.WORDS_NONE, N';') as s
    where  ltrim(rtrim(s.value)) <> N'';

    -- совпадения слов (только товары кодов «по словам» — несколько тысяч строк)
    drop table if exists #hit;
    select distinct p.ID_PRODUCT, w.RULE_ORDER, w.KIND
    into   #hit
    from   #p as p
           join #w as w on w.ID_CATEGORY_5_ext = p.ID_CATEGORY_5_ext
    where  p.NNAME collate Cyrillic_General_BIN2 like N'%[^А-ЯA-Z0-9]' + w.WORD + N'%';
    create clustered index IX_hit on #hit (ID_PRODUCT, RULE_ORDER, KIND);

    -- итог: поштучно (3) → код целиком (1) → первое подходящее правило (2) → «Несопоставленные» (0)
    drop table if exists #r;
    select p.ID_PRODUCT, p.ID_CATEGORY_5_ext
         , ID_CVM_CATEGORY = coalesce(man.ID_CVM_CATEGORY, case when m.MAP_METHOD = 1 then m.ID_CVM_CATEGORY end,
                                      rl.ID_CVM_CATEGORY, @unmapped)
         , HOW = case when coalesce(man.ID_CVM_CATEGORY, case when m.MAP_METHOD = 1 then m.ID_CVM_CATEGORY end,
                                    rl.ID_CVM_CATEGORY, @unmapped) = @unmapped then 0
                      when man.ID_CVM_CATEGORY is not null then 3
                      when m.MAP_METHOD = 1 then 1
                      else 2 end
    into   #r
    from   #p as p
           left join dbo.I_CVM_PRODUCT_CATEGORY_MANUAL as man (nolock) on man.ID_PRODUCT = p.ID_PRODUCT
           left join dbo.I_CVM_CATEGORY_MAP as m (nolock) on m.ID_CATEGORY_5_ext = p.ID_CATEGORY_5_ext
           outer apply (select top (1) r.ID_CVM_CATEGORY
                        from   dbo.I_CVM_CATEGORY_RULE as r (nolock)
                        where  m.MAP_METHOD = 2
                          and  r.ID_CATEGORY_5_ext = p.ID_CATEGORY_5_ext
                          and  (r.WORDS_ANY is null
                                or exists (select 1 from #hit as h where h.ID_PRODUCT = p.ID_PRODUCT and h.RULE_ORDER = r.RULE_ORDER and h.KIND = 'A'))
                          and  not exists (select 1 from #hit as h where h.ID_PRODUCT = p.ID_PRODUCT and h.RULE_ORDER = r.RULE_ORDER and h.KIND = 'N')
                        order by r.RULE_ORDER) as rl;

    begin transaction;

    -- новые коды ДИКСИ — «несопоставлен», к привязке
    insert into dbo.I_CVM_CATEGORY_MAP (ID_CATEGORY_5_ext, EXT_NAME, MAP_METHOD, ID_CVM_CATEGORY, STATUS, DATE_CREATE, NOTE)
    select x.ID_CATEGORY_5_ext
         , (select max(e.I_CATEGORY_5_ext) from dbo.I_CATEGORY_EXT as e (nolock)
            where e.ID_COMPANY = 1 and e.ID_CATEGORY_5_ext = x.ID_CATEGORY_5_ext)
         , null, null, 2, @today, N'новый код ДИКСИ'
    from   (select distinct ID_CATEGORY_5_ext from #p where ID_CATEGORY_5_ext is not null) as x
    where  not exists (select 1 from dbo.I_CVM_CATEGORY_MAP as m where m.ID_CATEGORY_5_ext = x.ID_CATEGORY_5_ext);
    set @new_codes = @@rowcount;

    -- привязка товаров: меняются только изменившиеся строки; товары, ушедшие из I_PRODUCT, остаются
    -- (история чеков читается по текущему справочнику)
    update t
    set    t.ID_CVM_CATEGORY = r.ID_CVM_CATEGORY, t.ID_CVM_GROUP = c.ID_PARENT,
           t.ID_CATEGORY_5_ext = r.ID_CATEGORY_5_ext, t.HOW = r.HOW, t.DATE_UPDATE = @today
    from   dbo.I_CVM_PRODUCT_CATEGORY as t
           join #r as r on r.ID_PRODUCT = t.ID_PRODUCT
           join dbo.I_CVM_CATEGORY as c (nolock) on c.ID_CVM_CATEGORY = r.ID_CVM_CATEGORY
    where  t.ID_CVM_CATEGORY <> r.ID_CVM_CATEGORY or t.ID_CVM_GROUP <> c.ID_PARENT or t.HOW <> r.HOW
       or  isnull(t.ID_CATEGORY_5_ext, N'') <> isnull(r.ID_CATEGORY_5_ext, N'');
    set @upd = @@rowcount;

    insert into dbo.I_CVM_PRODUCT_CATEGORY (ID_PRODUCT, ID_CVM_CATEGORY, ID_CVM_GROUP, ID_CATEGORY_5_ext, HOW, DATE_UPDATE)
    select r.ID_PRODUCT, r.ID_CVM_CATEGORY, c.ID_PARENT, r.ID_CATEGORY_5_ext, r.HOW, @today
    from   #r as r
           join dbo.I_CVM_CATEGORY as c (nolock) on c.ID_CVM_CATEGORY = r.ID_CVM_CATEGORY
    where  not exists (select 1 from dbo.I_CVM_PRODUCT_CATEGORY as t where t.ID_PRODUCT = r.ID_PRODUCT);
    set @ins = @@rowcount;

    commit transaction;

    declare @un int = (select count(*) from #r where HOW = 0);
    raiserror(N'cvm_category_refresh: новых кодов %d, товаров добавлено %d, изменено %d, несопоставлено %d.', 10, 1,
              @new_codes, @ins, @upd, @un) with nowait;
end;
go

-- 4. Первый прогон
exec dbo.cvm_category_refresh;
go
'''

STEP = f'''
-- 5. Шаг CVM_CATEGORY в «MCI Regular update» — сразу после «{AFTER_STEP}», «ош. → дальше».
-- Вставка на место в порядке выполнения: sp_add_jobstep с номером следующего шага сдвигает следующие шаги
-- и ссылки на них. Шаг ищется по имени; если уже есть — ничего не меняется.
declare @job sysname = N'MCI Regular update', @after int, @act int, @rc int, @new int;

select @after = s.step_id, @act = s.on_success_action
from   msdb.dbo.sysjobsteps as s (nolock)
       join msdb.dbo.sysjobs as j (nolock) on j.job_id = s.job_id
where  j.name = @job and s.step_name = N'{AFTER_STEP}';

if exists (select 1 from msdb.dbo.sysjobsteps as s (nolock) join msdb.dbo.sysjobs as j (nolock) on j.job_id = s.job_id
           where j.name = @job and s.step_name = N'CVM_CATEGORY')
    raiserror(N'Шаг CVM_CATEGORY уже стоит в задании — не меняю.', 10, 1) with nowait;
else if exists (select 1 from msdb.dbo.sysjobactivity as a (nolock)
                join msdb.dbo.sysjobs as j (nolock) on j.job_id = a.job_id
                where j.name = @job and a.start_execution_date is not null and a.stop_execution_date is null
                  and a.session_id = (select max(session_id) from msdb.dbo.syssessions (nolock)))
    -- шаги задания не переставляем во время его прохода: скрипт упадёт с 16 и повторится через 10 минут
    raiserror(N'Идёт «MCI Regular update» — шаг CVM_CATEGORY поставлю при следующем запуске, задание не изменено.', 16, 1);
else if not exists (select 1 from sys.partitions (nolock)
                    where object_id = object_id(N'dbo.I_CVM_CATEGORY') and index_id in (0, 1) and rows > 0)
    raiserror(N'СТОП по шагу: справочник пуст (заливка не прошла) — шаг не ставлю, задание не изменено.', 16, 1);
else if object_id(N'dbo.cvm_category_refresh', N'P') is null
    raiserror(N'СТОП по шагу: процедура cvm_category_refresh не создана — задание не изменено.', 16, 1);
else if @after is null or @act <> 3
    raiserror(N'СТОП по шагу: не найден шаг «{AFTER_STEP}» или он при успехе идёт не на следующий — задание не изменено.', 16, 1);
else
begin
    set @new = @after + 1;
    begin transaction;
    exec @rc = msdb.dbo.sp_add_jobstep @job_name = @job, @step_id = @new, @step_name = N'CVM_CATEGORY',
         @subsystem = N'TSQL', @database_name = N'mci_model', @command = N'exec dbo.cvm_category_refresh',
         @on_success_action = 3, @on_fail_action = 3, @retry_attempts = 0;
    if @@error <> 0 or @rc <> 0
    begin
        rollback transaction;
        raiserror(N'СТОП по шагу: sp_add_jobstep не прошёл — задание не изменено.', 16, 1);
    end
    else
    begin
        commit transaction;
        raiserror(N'Шаг %d CVM_CATEGORY добавлен после «{AFTER_STEP}».', 10, 1, @new) with nowait;
    end;
end;
go
'''

RESULT = '''
-- 6. Итог
select TBL = N'Группы',                 ROWS_CNT = count(*) from dbo.I_CVM_CATEGORY (nolock) where NODE_TYPE = 1
union all select N'Категории',             count(*) from dbo.I_CVM_CATEGORY (nolock) where NODE_TYPE = 2
union all select N'  из них без шкалы',    count(*) from dbo.I_CVM_CATEGORY (nolock) where NODE_TYPE = 2 and NO_SCALE = 1
union all select N'Коды ДИКСИ',            count(*) from dbo.I_CVM_CATEGORY_MAP (nolock)
union all select N'  несопоставлены',      count(*) from dbo.I_CVM_CATEGORY_MAP (nolock) where STATUS = 2
union all select N'  без названия',        count(*) from dbo.I_CVM_CATEGORY_MAP (nolock) where EXT_NAME is null
union all select N'Правила разрезов',      count(*) from dbo.I_CVM_CATEGORY_RULE (nolock)
union all select N'Товары поштучно',       count(*) from dbo.I_CVM_PRODUCT_CATEGORY_MANUAL (nolock)
union all select N'Товары в справочнике',  count(*) from dbo.I_CVM_PRODUCT_CATEGORY (nolock)
union all select N'  несопоставлены',      count(*) from dbo.I_CVM_PRODUCT_CATEGORY (nolock) where HOW = 0;

-- несопоставленные коды ДИКСИ — к привязке
select m.ID_CATEGORY_5_ext, m.EXT_NAME, m.DATE_CREATE, PRODUCTS = count(t.ID_PRODUCT)
from   dbo.I_CVM_CATEGORY_MAP as m (nolock)
       left join dbo.I_CVM_PRODUCT_CATEGORY as t (nolock) on t.ID_CATEGORY_5_ext = m.ID_CATEGORY_5_ext
where  m.STATUS = 2
group by m.ID_CATEGORY_5_ext, m.EXT_NAME, m.DATE_CREATE
order by PRODUCTS desc;

-- шаги задания вокруг нового (по именам) и переходы при ошибке: должны остаться «send error mail» и «дальше»
declare @job_id uniqueidentifier = (select job_id from msdb.dbo.sysjobs (nolock) where name = N'MCI Regular update');
select s.step_id, s.step_name, s.command, s.on_success_action, s.on_fail_action, s.on_fail_step_id
from   msdb.dbo.sysjobsteps as s (nolock)
where  s.job_id = @job_id
  and  s.step_id between (select step_id - 1 from msdb.dbo.sysjobsteps (nolock) where job_id = @job_id and step_name = N'Check Product Update')
                     and isnull((select step_id + 2 from msdb.dbo.sysjobsteps (nolock) where job_id = @job_id and step_name = N'CVM_CATEGORY'),
                                (select step_id + 2 from msdb.dbo.sysjobsteps (nolock) where job_id = @job_id and step_name = N'Check Product Update'))
order by s.step_id;

select ON_FAIL = case s.on_fail_action when 4 then N'на шаг ' + isnull(t.step_name, N'?') when 3 then N'дальше'
                      when 2 then N'выход «ошибка»' else N'выход «успех»' end
     , STEPS = count(*)
from   msdb.dbo.sysjobsteps as s (nolock)
       left join msdb.dbo.sysjobsteps as t (nolock) on t.job_id = s.job_id and t.step_id = s.on_fail_step_id
where  s.job_id = @job_id
group by case s.on_fail_action when 4 then N'на шаг ' + isnull(t.step_name, N'?') when 3 then N'дальше'
              when 2 then N'выход «ошибка»' else N'выход «успех»' end;
'''


def main():
    wb = openpyxl.load_workbook(SRC, read_only=True)
    rd = lambda s: list(wb[s].iter_rows(values_only=True))[1:]
    groups, cats, codes, rules, prods = (rd('Группы'), rd('Категории'), rd('Маппинг кодов'),
                                         rd('Правила разрезов'), rd('Товарный маппинг'))

    cat_rows = [(g[0], g[1], 1, None, None, 0) for g in groups]
    cat_rows += [(c[0], c[3], 2, c[1], c[4], 1 if c[5] == 'да' else 0) for c in cats]
    code_rows = [(code(c[0]), c[1], METHOD[c[2]], c[3], 1) for c in codes]
    rule_rows = [(code(r[0]), r[1], None if r[2].startswith('—') else r[2].replace(', ', ';'),
                  r[3].replace(', ', ';') if r[3] else None, r[4]) for r in rules]
    prod_rows = [(p[0], p[3], 2 if p[6] == 'вручную' else 1) for p in prods]

    # те же условия, что в контроле SQL, — до выдачи скрипта
    cat_type = {r[0]: r[2] for r in cat_rows}
    method = {r[0]: r[2] for r in code_rows}
    assert all(cat_type.get(r[3]) == 1 for r in cat_rows if r[2] == 2), 'категория без группы'
    assert all(cat_type.get(r[3]) == 2 for r in code_rows if r[3] is not None), 'код ведёт не на категорию'
    assert all(cat_type.get(r[4]) == 2 and method.get(r[0]) == 2 for r in rule_rows), 'правило у кода не «по словам»'
    assert all(any(r[0] == c for r in rule_rows) for c, m in method.items() if m == 2), 'код «по словам» без правил'
    assert all(cat_type.get(r[1]) == 2 for r in prod_rows), 'товар ведёт не на категорию'
    assert len({r[0] for r in code_rows}) == len(code_rows) and len({r[0] for r in prod_rows}) == len(prod_rows)
    assert sum(1 for r in cat_rows if r[1] == 'Несопоставленные' and r[2] == 2) == 1

    head = f'''-- ============================================================================================
-- CVM_CATEGORY_REFERENCE — собственный справочник категорий CVM в mci_model: таблицы, первая заливка,
--   ежедневная процедура dbo.cvm_category_refresh и её шаг CVM_CATEGORY в задании «MCI Regular update».
-- Запустить целиком в SSMS (F5). Повторный запуск безопасен: заливка пропускается, если справочник уже заполнен
--   (наши ID однажды выданы и не перевыдаются); процедура пересоздаётся; шаг не дублируется.
--
-- Действия по объектам:
--   dbo.I_CVM_CATEGORY                — создаётся; {len(cat_rows)} строк: {len(groups)} групп (ID 1–{len(groups)}) и {len(cats)} категорий (ID 1001+).
--   dbo.I_CVM_CATEGORY_MAP            — создаётся; {len(code_rows)} кодов ДИКСИ (ID_CATEGORY_5_ext) → как относится и наша категория;
--                                       названия кодов, которых нет в файле, — из dbo.I_CATEGORY_EXT.
--   dbo.I_CVM_CATEGORY_RULE           — создаётся; {len(rule_rows)} правил разрезов по словам названия.
--   dbo.I_CVM_PRODUCT_CATEGORY_MANUAL — создаётся; {len(prod_rows)} товаров с привязкой поштучно (перекрывает правило кода).
--   dbo.I_CVM_PRODUCT_CATEGORY        — создаётся; итог «товар → наша категория» по всем товарам I_PRODUCT —
--                                       это читают все витрины, отчёты и модели. Заполняет процедура.
--   dbo.I_CVM_PRODUCT_UNIT            — создаётся пустой; разметка единиц из названия — следующим шагом.
--   dbo.cvm_category_refresh          — процедура (create or alter): новые коды ДИКСИ заводит со статусом
--                                       «несопоставлен»; пересчитывает привязку товаров, меняет только изменившиеся.
--   «MCI Regular update»              — новый шаг CVM_CATEGORY сразу после «{AFTER_STEP}» (ош. → дальше);
--                                       следующие шаги сдвигаются на 1, порядок не меняется.
--   Ничего не удаляется; I_PRODUCT и I_CATEGORY_EXT только читаются.
--   Шаг ставится только когда «MCI Regular update» не идёт; иначе скрипт падает с 16 и повторяется (обменник, 10 мин).
--
-- Как товар получает категорию (HOW в I_CVM_PRODUCT_CATEGORY):
--   3 — поштучно: товар есть в I_CVM_PRODUCT_CATEGORY_MANUAL;
--   1 — код целиком: MAP_METHOD = 1;
--   2 — правило слов: MAP_METHOD = 2, первое по RULE_ORDER правило кода, где в названии есть одно из WORDS_ANY
--       и нет ни одного из WORDS_NONE; слово ищется с начала слова, Ё = Е; пустой WORDS_ANY = «всё остальное»;
--   0 — итог «Несопоставленные» (новый код, код «по товарам» без привязки, правило без полки, неясный товар): в шкалу не входит.
--   Товары, ушедшие из I_PRODUCT, остаются со своей категорией: историю чеков читать по текущему справочнику.
--
-- Справочники значений:
--   I_CVM_CATEGORY.NODE_TYPE: 1 группа, 2 категория; UNIT: кг / л / шт; NO_SCALE = 1 — в ценовую шкалу не входит;
--   STATUS: 1 активна / сопоставлен, 0 закрыта; в MAP ещё 2 — несопоставлен (новый код ДИКСИ);
--   I_CVM_CATEGORY_MAP.MAP_METHOD: 1 целиком, 2 по словам, 3 по товарам;
--   SOURCE поштучной привязки и разметки единиц: 1 по названию, 2 вручную.
-- ============================================================================================
use mci_model;
go
set nocount on;
set xact_abort on;
go

-- 0. Запуск только когда «MCI Regular update» не идёт (I_PRODUCT перезаливается, шаги задания нельзя переставлять)
--    и у учётки есть sysadmin (sp_add_jobstep на задании sa). Иначе — ошибка 16, ничего не изменено, обменник повторит.
if is_srvrolemember(N'sysadmin') = 0
    raiserror(N'Нет прав sysadmin у учётки запуска — ничего не изменено.', 16, 1);
if exists (select 1 from msdb.dbo.sysjobactivity as a (nolock)
           join msdb.dbo.sysjobs as j (nolock) on j.job_id = a.job_id
           where j.name = N'MCI Regular update' and a.start_execution_date is not null and a.stop_execution_date is null
             and a.session_id = (select max(session_id) from msdb.dbo.syssessions (nolock)))
    raiserror(N'Идёт «MCI Regular update» — справочник и шаг поставлю при следующем запуске, ничего не изменено.', 16, 1);
go
''' + TABLES + '''
-- 2. Первая заливка — одной транзакцией; пропускается, если справочник уже заполнен
if exists (select 1 from sys.partitions (nolock)
           where object_id = object_id(N'dbo.I_CVM_CATEGORY') and index_id in (0, 1) and rows > 0)
begin
    raiserror(N'Справочник dbo.I_CVM_CATEGORY уже заполнен — первая заливка пропущена.', 10, 1) with nowait;
    set noexec on;
end;
go
begin transaction;

'''
    body = (inserts('dbo.I_CVM_CATEGORY', ['ID_CVM_CATEGORY', 'NAME', 'NODE_TYPE', 'ID_PARENT', 'UNIT', 'NO_SCALE'], cat_rows)
            + '\n' + inserts('dbo.I_CVM_CATEGORY_MAP', ['ID_CATEGORY_5_ext', 'EXT_NAME', 'MAP_METHOD', 'ID_CVM_CATEGORY', 'STATUS'], code_rows)
            + '\n' + inserts('dbo.I_CVM_CATEGORY_RULE', ['ID_CATEGORY_5_ext', 'RULE_ORDER', 'WORDS_ANY', 'WORDS_NONE', 'ID_CVM_CATEGORY'], rule_rows)
            + '\n' + inserts('dbo.I_CVM_PRODUCT_CATEGORY_MANUAL', ['ID_PRODUCT', 'ID_CVM_CATEGORY', 'SOURCE'], prod_rows))
    DST.write_text(head + body + CONTROL + PROC + STEP + RESULT, encoding='utf-8-sig')
    print(DST.name, len(cat_rows), len(code_rows), len(rule_rows), len(prod_rows), round(DST.stat().st_size / 1e6, 2), 'MB')


if __name__ == '__main__':
    main()
