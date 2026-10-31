-- ============================================================================================
-- CVM_TRENDS_V2_20261101 — новая версия расчёта трендов (dbo.trends, шаг 55 I_TRENDS «MCI Regular update») по методике
-- Т1, согласованной Еленой 31.10.2026 («1а 2а»): output/Тренды_новый_расчёт_2026-10-31/Т1_методика_трендов_2026-10-31.txt.
-- Что меняется:
--   1. Категории — справочник CVM (товар → I_CVM_PRODUCT_CATEGORY, название — I_CVM_CATEGORY) вместо кодов ДИКСИ; вся
--      история — по текущей привязке товаров. Группа «Служебное» и товары без категории в тренды не входят. Покупатель
--      категории — клиент со строкой покупки (количество больше 0); возврат покупателем не делает.
--   2. Ценовой сегмент клиента за месяц — готовый PRICE_SEGMENT витрины ценового индекса (I_CVM_CLIENT_WEEK: строка на
--      последнюю покупку месяца, окно 56 дней, границы прошлого года); своих порогов (0,359 / 0,25) у трендов больше нет.
--   3. Сегменты по чеку — средний чек клиента за месяц (оплата чеков покупки / их число), пересчитанный в цены того же
--      месяца прошлого года: делится на индекс цен тех же товаров (Фишер, сеть и канал, I_CVM_PRODUCT_MONTH). Границы —
--      трети клиентов того же месяца прошлого года по среднему чеку (было 1 500 / 500 ₽ офлайн).
--   4. Над-индекс у всех сегментов — доля категории в продажах (₽) сегмента / доля в продажах базы. Маркер — только
--      надёжный: нижняя граница 95 % ≥ ×1,10 (верхняя ≤ 1/1,10 — «ниже») с поправкой Бенджамини–Хохберга внутри сегмента,
--      города, канала и месяца. Порядок — рубли сверх ожидаемого. В I_TREND — 8 новых колонок (раздел 1); прежние колонки
--      считаются как раньше, но на категориях CVM и строках покупки.
--   5. I_TREND_PRICE_SEGMENT — запись по именам колонок: бонусы больше не ложатся в колонку UNIQUE_SKU (она остаётся пустой).
--   6. Прежние строки удаляются не всем диапазоном вызова заранее, а по месяцу — в транзакции месяца перед записью:
--      непосчитанный месяц (витрины индекса не готовы, ошибка) остаётся как был.
--   7. Ночной расчёт — по принципу месячных витрин MCI (Елена 01.11.2026: «очистка определяет, сколько затереть в витрине,
--      а скрипт смотрит, сколько пересчитывать»): clear_data_update после перезаливки чеков стирает тренды с первого
--      перезалитого месяца (первая суббота — 3 месяца, по просьбе — хоть 2 года); процедура считает все месяцы после
--      последнего месяца своих сегментов в I_TREND. Граница — месяц окна ежедневной перезаливки (10 дней), не включая: его
--      очистка стирает каждую ночь до 10-го числа (31.10: «1 раз в месяц, не тратить время каждый день») — новый месяц
--      считается 11-го. В ночи без стёртых месяцев считать нечего (выход за секунды). Версия от утра 01.11 с границей по
--      первой субботе заменяется этой.
-- Остаётся как было: сегменты жизненного цикла (смысл прежний), знаменатель — свой сегмент, месяц пишется в витрины двумя
--   вставками перед COMMIT.
-- Месяц считается, только если посчитаны месячные задания, от которых он зависит: LFL-магазины месяца и прошлого месяца
--   (I_LFL_SHOPS, процедура MCI LFL_contacts_shops) и витрины ценового индекса за месяц и тот же месяц год назад (журнал
--   I_CVM_PM_LOG). Иначе ночью — сообщение без ошибки (шаг I_TRENDS при ошибке перескакивает на шаг 59 и пропустил бы
--   56–58; месяц посчитается в следующую ночь), при вызове с @YM_START (пересчёт истории) — ошибка.
-- Новых таблиц, процедур и заданий не создаёт. Повтор безопасен: уже обновлённая процедура не трогается, колонки
--   добавляются только недостающие.
-- Проверка: SELECT * FROM dbo.CVM_API_JOB_LOG (nolock) WHERE JOB LIKE N'CVM_TRENDS_V2%';
-- ============================================================================================

USE mci_model
GO
SET NOCOUNT ON;

-- ============================== 0. ПРОВЕРКИ ==============================
-- Раннер обменника вызывает sqlcmd с -b: RAISERROR 16 обрывает скрипт, он повторяется через 10 минут.
DECLARE @def nvarchar(max) = OBJECT_DEFINITION(OBJECT_ID('dbo.trends'));
DECLARE @state nvarchar(10) =
    CASE WHEN @def IS NULL THEN N'none'
         WHEN @def LIKE N'%версия Т3 — категории CVM, над-индекс по доле в продажах (CVM_TRENDS_V2_20261101.sql)%'
              AND @def LIKE N'%сколько пересчитывать — по последнему месяцу в I_TREND (01.11.2026)%' THEN N'v2'
         -- выложенная ночью 01.11 версия Т3 с границей по первой субботе — заменяется
         WHEN @def LIKE N'%версия Т3 — категории CVM, над-индекс по доле в продажах (CVM_TRENDS_V2_20261101.sql)%' THEN N'v2sat'
         WHEN @def LIKE N'%ночной режим — один раз в месяц (CVM_TRENDS_MONTHLY_20261031.sql)%' THEN N'monthly'
         ELSE N'other'
    END;

IF @state = N'none'
BEGIN
    RAISERROR(N'СТОП: dbo.trends не найдена или нет прав на её текст — ничего не меняю.', 16, 1);
    SET NOEXEC ON;
END
ELSE IF @state = N'other'
BEGIN
    RAISERROR(N'СТОП: dbo.trends — не версия CVM_TRENDS_MONTHLY_20261031 и не Т3 (её текст меняли) — ничего не меняю.', 16, 1);
    SET NOEXEC ON;
END
-- ALTER при идущем расчёте: идущий вызов может упасть с ошибкой 2801 — ждём его конца (повтор через 10 минут)
ELSE IF @state IN (N'monthly', N'v2sat')
     AND EXISTS (SELECT 1
                 FROM sys.dm_exec_requests AS r
                      CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
                 WHERE t.dbid = DB_ID() AND t.objectid = OBJECT_ID('dbo.trends'))
BEGIN
    RAISERROR(N'СТОП (подождать): сейчас выполняется dbo.trends — повторю после окончания расчёта, ничего не меняю.', 16, 1);
    SET NOEXEC ON;
END
ELSE IF @state = N'v2'
BEGIN
    PRINT N'dbo.trends уже новой версии (Т3) — колонки и ALTER пропускаются.';
    SET NOEXEC ON;   -- пропустить разделы 1–2; раздел 3 снова включает выполнение
END
GO

-- ============================== 1. НОВЫЕ КОЛОНКИ I_TREND ==============================
-- Колонки, допускающие NULL, — изменение только метаданных, таблица не переписывается. У прошлых строк пустые до пересчёта
-- истории (Т4). Ждать блокировку схемы не дольше 30 с (портал читает таблицу): по истечении — ошибка, повтор через 10 минут.
SET LOCK_TIMEOUT 30000;
IF COL_LENGTH('dbo.I_TREND', 'ID_CVM_CATEGORY')  IS NULL ALTER TABLE dbo.I_TREND ADD ID_CVM_CATEGORY int NULL;          -- категория CVM
IF COL_LENGTH('dbo.I_TREND', 'SALES_SHARE')      IS NULL ALTER TABLE dbo.I_TREND ADD SALES_SHARE float NULL;            -- доля категории в продажах (₽) сегмента
IF COL_LENGTH('dbo.I_TREND', 'SALES_SHARE_BASE') IS NULL ALTER TABLE dbo.I_TREND ADD SALES_SHARE_BASE float NULL;       -- доля категории в продажах базы сравнения
IF COL_LENGTH('dbo.I_TREND', 'SALES_INDEX')      IS NULL ALTER TABLE dbo.I_TREND ADD SALES_INDEX float NULL;            -- над-индекс: SALES_SHARE / SALES_SHARE_BASE
IF COL_LENGTH('dbo.I_TREND', 'SALES_INDEX_LOW')  IS NULL ALTER TABLE dbo.I_TREND ADD SALES_INDEX_LOW float NULL;        -- нижняя граница 95 %
IF COL_LENGTH('dbo.I_TREND', 'SALES_INDEX_HIGH') IS NULL ALTER TABLE dbo.I_TREND ADD SALES_INDEX_HIGH float NULL;       -- верхняя граница 95 %
IF COL_LENGTH('dbo.I_TREND', 'SALES_MARK')       IS NULL ALTER TABLE dbo.I_TREND ADD SALES_MARK smallint NULL;          -- 1 надёжно выше, -1 надёжно ниже, 0 нет
IF COL_LENGTH('dbo.I_TREND', 'SALES_EXTRA')      IS NULL ALTER TABLE dbo.I_TREND ADD SALES_EXTRA decimal(19, 2) NULL;   -- продажи сверх ожидаемого, ₽
SET LOCK_TIMEOUT -1;
GO

-- ============================== 2. ПРОЦЕДУРА ТРЕНДОВ ==============================
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- ============================== 2. dbo.trends — ext-пересборка ==============================
-- Источник: dixy-dashboard/данные/sql_i_trend_rebuild_ext.sql (согласовано 20.07, чековые 24.08); правки 11.10.2026
--   (CVM_TRENDS_REBUILD.sql): знаменатель доли покупателей — по своему сегменту, ночной режим, граница @YM_STOP; правки 31.10.2026 (CVM_TRENDS_FIX_20261031.sql): месяц пишется в витрины двумя вставками перед COMMIT, границы недель — переменными; ночной режим — один раз в месяц (CVM_TRENDS_MONTHLY_20261031.sql).
-- версия Т3 — категории CVM, над-индекс по доле в продажах (CVM_TRENDS_V2_20261101.sql): методика Т1 от 31.10.2026 —
--   категории CVM, ценовой сегмент из витрины ценового индекса, чековые сегменты в ценах того же месяца прошлого года,
--   над-индекс по доле категории в продажах с надёжным маркером; месяц — только при готовых витринах индекса.
-- сколько пересчитывать — по последнему месяцу в I_TREND (01.11.2026): как месячные процедуры MCI (LFL_contacts_shops,
--   for_i_report) — сколько стереть, решает очистка clear_data_update после перезаливки чеков, процедура досчитывает всё
--   после последнего месяца своих сегментов в таблице.
-- @YM_START: NULL (джоб, шаг 55 I_TRENDS) = месяцы со следующего после последнего месяца своих сегментов в I_TREND до
--   месяца окна ежедневной перезаливки чеков (10 дней), не включая (дни 1-10 → прошлый месяц очистка стирает каждую
--   ночь); 202401 = полная пересборка.
CREATE OR ALTER PROCEDURE [dbo].[trends] @YM_START int = NULL
    , @YM_STOP int = NULL   -- граница окна, месяц НЕ включается; NULL — до текущего месяца
as
set nocount on;
set quoted_identifier on;

DECLARE @nightly bit = IIF(@YM_START IS NULL, 1, 0);   -- ночной режим: вызов без @YM_START (шаг 55 I_TRENDS)
DECLARE @last_month int;
DECLARE @msg nvarchar(1000);

-- Граница ночного окна — месяц начала ежедневной перезаливки чеков («MCI Reload every day»: последние 10 дней), не включая
IF @YM_START IS NULL
    SET @YM_START = (SELECT YEAR_MONTH FROM dbo.I_CALENDAR (nolock)
                     WHERE [DATE] = convert(date, dateadd(day, -10, getdate())));

-- Сегменты, которые генерирует ИМЕННО этот скрипт (чужие не удаляем!)
-- I_TREND:               статусные + ценовые + чековые тренды
-- I_TREND_PRICE_SEGMENT: 12-16 (статусные), 1-3 (ценовые),
--                        1000-1002 (чековые BIG/AVG/LOW CHECK).

drop table if exists #c;
select  ID_COMPANY
      , RN = ROW_NUMBER() over (order by ID_COMPANY)
into    #c
from    dbo.I_ORGANIZATION (nolock)
group by ID_COMPANY;

DECLARE @c   int = 1;
DECLARE @idc int = (select ID_COMPANY from #c where RN = @c);

WHILE @c <= 1   -- (select max(RN) from #c) — пока только первая компания
BEGIN

    -- ── 1. Категории CVM (Т1, 31.10.2026): товар → категория справочника CVM; вся история — по текущей привязке товаров.
    -- Только категории (NODE_TYPE 2); группа 20 «Служебное» (пакеты, услуги) и товары без категории в тренды не входят —
    -- ни в продажи категории, ни в продажи клиента. Название категории — из I_CVM_CATEGORY при записи.
    drop table if exists #pc;
    select  pc.ID_PRODUCT
          , pc.ID_CVM_CATEGORY
    into    #pc
    from    dbo.I_CVM_PRODUCT_CATEGORY as pc (nolock)
            inner join dbo.I_CVM_CATEGORY as k (nolock)
                on  k.ID_CVM_CATEGORY = pc.ID_CVM_CATEGORY
    where   k.NODE_TYPE = 2
        and isnull(k.ID_PARENT, 0) <> 20;
    create unique clustered index cx_pc on #pc (ID_PRODUCT);

    -- Название города отчёта — одно на ID_CITY_REPORT (в агрегат чеков его не тянем)
    drop table if exists #city;
    select  ID_CITY_REPORT
          , CITY_REPORT = max(CITY_REPORT)
    into    #city
    from    dbo.I_SHOP (nolock)
    where   ID_COMPANY = @idc
    group by ID_CITY_REPORT;

    -- Семейства сегментов (Т1 п. 5): сегмент, его база сравнения и имя в I_TREND.
    -- 1 — жизненный цикл (новые, случайные, активные LFL — против активных 13); 2 — отток (16 — против оставшихся
    -- покупателей прошлого месяца, 1); 3 — ценовой (высокий 1 и низкий 3 — против среднего 2); 4 — по чеку (большой 1000
    -- и малый 1002 — против среднего 1001).
    drop table if exists #sm;
    create table #sm (FAM tinyint not null, SEGMENT int not null, BASE int not null, SEGMENT_NAME nvarchar(100) not null);
    insert into #sm (FAM, SEGMENT, BASE, SEGMENT_NAME)
    values  (1, 12, 13, N'NEW TRENDS'), (1, 15, 13, N'RANDOM TRENDS'), (1, 14, 13, N'ACTIVE LFL TRENDS')
          , (2, 16, 1, N'CHURN TRENDS')
          , (3, 1, 2, N'HIGH-PRICE SEGMENT'), (3, 3, 2, N'LOW-PRICE SEGMENT')
          , (4, 1000, 1001, N'BIG CHECK'), (4, 1002, 1001, N'LOW CHECK');

    -- ── Ночной режим (31.10.2026 Елена: «только 1 раз в месяц и не тратить время каждый день»; 01.11.2026: «очистка
    -- определяет, сколько затереть в витрине, а скрипт смотрит, сколько пересчитывать»). Считаются месяцы со следующего
    -- после последнего месяца своих сегментов в I_TREND до месяца окна ежедневной перезаливки (не включая): всё, что
    -- стёрла очистка clear_data_update после перезаливки чеков (первая суббота — с 1-го числа три месяца назад; по
    -- просьбе — хоть 2 года), досчитывается в ту же ночь; новый месяц — один раз, 11-го (до 10-го его стирает ежедневная
    -- очистка). В остальные ночи считать нечего — выход за секунды. Пустая витрина — ничего (историю считает пересчёт).
    IF @nightly = 1
    BEGIN
        IF @YM_STOP IS NULL OR @YM_STOP > @YM_START
            SET @YM_STOP = @YM_START;   -- граница окна — не включается (явная граница раньше неё — остаётся)
        SET @last_month = (SELECT max(YEAR_MONTH) FROM dbo.I_TREND (nolock)
                           WHERE ID_COMPANY = @idc
                             AND YEAR_MONTH < @YM_START
                             AND SEGMENT in ('NEW TRENDS','RANDOM TRENDS','CHURN TRENDS',
                                             'ACTIVE LFL TRENDS','HIGH-PRICE SEGMENT','LOW-PRICE SEGMENT',
                                             'BIG CHECK','LOW CHECK'));
        IF @last_month IS NOT NULL
            SET @YM_START = (SELECT min(YEAR_MONTH) FROM dbo.I_CALENDAR (nolock) WHERE YEAR_MONTH > @last_month);
    END

    -- ── 2. Т3: прежние строки месяца удаляются не всем диапазоном заранее, а по одному месяцу — в транзакции месяца перед
    -- записью (в конце цикла): месяц, который не посчитан (витрины индекса не готовы, ошибка), остаётся как был

    drop table if exists #ym;
    SELECT  YEAR_MONTH
          , LAST_DAY_IN_MONTH
          , RN = ROW_NUMBER() OVER (order by YEAR_MONTH)
    into    #ym
    FROM    dbo.I_CALENDAR as a (nolock)
    group by YEAR_MONTH, LAST_DAY_IN_MONTH;

    declare @YM        int  = @YM_START;
    declare @YM_before int  = (select YEAR_MONTH from #ym where RN = (select RN from #ym where YEAR_MONTH = @YM) - 1);
    declare @act_date  date = (select LAST_DAY_IN_MONTH from #ym where YEAR_MONTH = @YM);

    -- ── ОПТИМИЗАЦИЯ (пункт M шапки): предрасчёты вне месячного цикла

    -- M1: верхняя граница цикла — один раз; пустой диапазон — чеки не читаем (@YM_MAX = NULL: цикл и предрасчёты
    -- не выполняются)
    declare @YM_MAX int;
    if @YM < isnull(@YM_STOP, 2147483647)
        set @YM_MAX = (select max(YEAR_MONTH) from dbo.I_CHECKHEADER (nolock) where ID_COMPANY = @idc);

    -- Т3: предрасчёты M2–M3 — в первой итерации месячного цикла, после проверки готовности витрин ценового индекса за
    -- первый месяц (иначе ночью чеки читались бы впустую). #wk_all, #buyers, #chk_agg используются только внутри цикла.
    declare @prep    bit = 0;
    declare @m_from  date;   -- первый день @YM
    declare @w_first date;   -- понедельник недели, в которую попадает первый день @YM
    declare @w_last  date;   -- понедельник недели, в которую попадает последний день @YM
    declare @wk_from date;
    declare @wk_to   date;

    -- 31.10.2026: строки месяца копятся во временных таблицах той же структуры (#tr — I_TREND, #tps —
    -- I_TREND_PRICE_SEGMENT) и пишутся в витрины двумя вставками перед COMMIT: витрины заняты секунды, а не весь расчёт
    -- месяца (~25 мин), и портал, читающий их, не ждёт. Внутри месяца витрины не читаются — результат тот же.
    drop table if exists #tr;
    select top (0) * into #tr from dbo.I_TREND (nolock);
    drop table if exists #tps;
    select top (0) * into #tps from dbo.I_TREND_PRICE_SEGMENT (nolock);

    -- Текущий (неполный) месяц не считаем — условие «строго меньше max»
    while @YM < @YM_MAX and (@YM_STOP is null or @YM < @YM_STOP)   -- и не дальше @YM_STOP (не включается)
    BEGIN

        -- Т3: месячные задания, от которых зависит месяц, посчитаны: LFL-магазины @YM и @YM_before (I_LFL_SHOPS — процедура
        -- MCI LFL_contacts_shops; периметр чеков месяца и оттока); витрины ценового индекса (журнал I_CVM_PM_LOG): клиент ×
        -- неделя — все недели, в которые попадают дни @YM (ценовой сегмент на последнюю покупку месяца); товар × месяц —
        -- @YM и тот же месяц год назад (индекс цен для чековых сегментов); границы ценовых сегментов прошлого года.
        -- Понедельник — как в витринах (1900-01-01 — понедельник, от DATEFIRST не зависит).
        set @m_from  = datefromparts(@YM / 100, @YM % 100, 1);
        set @w_first = dateadd(day, -(datediff(day, '19000101', @m_from) % 7), @m_from);
        set @w_last  = dateadd(day, -(datediff(day, '19000101', @act_date) % 7), @act_date);
        if not exists (select 1 from dbo.I_LFL_SHOPS (nolock) where ID_COMPANY = @idc and YEAR_month = @YM)
           or (@YM_before is not null
               and not exists (select 1 from dbo.I_LFL_SHOPS (nolock) where ID_COMPANY = @idc and YEAR_month = @YM_before))
        begin
            set @msg = concat(N'Тренды за ', @YM, N' не посчитаны: нет LFL-магазинов (I_LFL_SHOPS) за ', @YM, N' или ', @YM_before
                            , N' — месячное задание LFL_contacts_shops ещё не досчитало.');
            if @nightly = 1
            begin
                raiserror(@msg, 10, 1) with nowait;   -- ночью — без ошибки (см. ниже); следующая ночь начнёт с этого месяца
                break;
            end;
            throw 50002, @msg, 1;
        end;
        if (select count(distinct PERIOD_START) from dbo.I_CVM_PM_LOG (nolock)
            where KIND = 'W' and STATUS = 'ok' and PERIOD_START >= @w_first and PERIOD_START <= @w_last)
               < datediff(day, @w_first, @w_last) / 7 + 1
           or (select count(distinct PERIOD_START) from dbo.I_CVM_PM_LOG (nolock)
               where KIND = 'M' and STATUS = 'ok' and PERIOD_START in (@m_from, dateadd(year, -1, @m_from))) < 2
           or not exists (select 1 from dbo.I_CVM_PM_LOG (nolock)
                          where KIND = 'B' and STATUS = 'ok' and PERIOD_START = datefromparts(year(@m_from) - 1, 1, 1))
        begin
            set @msg = concat(N'Тренды за ', @YM, N' не посчитаны: витрины ценового индекса не готовы (клиент × неделя за месяц, '
                            , N'товар × месяц за ', @YM, N' и ', @YM - 100, N', границы ценовых сегментов ', year(@m_from) - 1
                            , N' — журнал I_CVM_PM_LOG).');
            if @nightly = 1
            begin
                -- ночью — без ошибки: шаг I_TRENDS при ошибке перескакивает на шаг 59 (пропустил бы 56–58); месяц не
                -- записан — следующая ночь начнёт с него же
                raiserror(@msg, 10, 1) with nowait;
                break;
            end;
            throw 50001, @msg, 1;
        end;

        if @prep = 0
        begin

    -- M2: недели покупок для RANDOM — один скан всего диапазона.
    -- Эквивалентность помесячному окну: группа (клиент, орг, неделя)
    -- попадала в окно, если ХОТЯ БЫ ОДНА покупка лежит в
    -- [начало окна; @act_date]. Даты недели укладываются в <=7 дней,
    -- окно длиной 56 недель — неделя не может накрыть обе границы,
    -- поэтому «MAX_DATA >= начала AND MIN_DATA <= конца» ⇔ «есть
    -- покупка внутри окна» (обе граничные даты — реальные покупки).
    -- 31.10.2026: границы — переменными (не подзапросами к #ym в условии скана чеков) и option (recompile): план видит
    -- конкретный диапазон дат
    set @wk_from = dateadd(day, +1, dateadd(week, -56,
                (select LAST_DAY_IN_MONTH from #ym where YEAR_MONTH = @YM_START)));
    -- 11.10.2026: верхняя граница — последний день последнего месяца этого вызова (без неё пересчёт по одному месяцу
    -- читал чеки до сегодня). Срез по месяцам тот же: неделя целиком за границей и так не проходила MIN_DATA <= @act_date,
    -- а у недели, пересекающей границу, MIN_DATA сохраняется и MAX_DATA остаётся >= начала окна (7 дней против 56 недель)
    set @wk_to = (select max(LAST_DAY_IN_MONTH) from #ym
                  where YEAR_MONTH < @YM_MAX and YEAR_MONTH < isnull(@YM_STOP, 2147483647));

    drop table if exists #wk_all;
    select  ID_CONTACT
          , ID_ORGANIZATION
          , ID_COMPANY
          , WEEKNUMBER
          , MIN_DATA = min(DATA)
          , MAX_DATA = max(DATA)
    into    #wk_all
    from    dbo.I_CHECKHEADER (nolock)
    where   ID_COMPANY = @idc
        and ID_CONTACT != 0
        and DATA >= @wk_from
        and DATA <= @wk_to
    group by ID_CONTACT, ID_ORGANIZATION, ID_COMPANY, WEEKNUMBER
    option (recompile);

    create clustered index cx_wk_all on #wk_all (ID_CONTACT, ID_ORGANIZATION, WEEKNUMBER);

    -- M3: carry-over таблицы на два месяца (@YM_before + @YM).
    -- Затравка месяцем перед @YM_START — для CHURN первой итерации
    -- (при @YM_before IS NULL обе затравки корректно пусты).

    -- Покупатели месяца (дистинкт клиент × орг; ID_CONTACT != 0)
    drop table if exists #buyers;
    select  ID_CONTACT, ID_ORGANIZATION, ID_COMPANY, YEAR_MONTH
    into    #buyers
    from    dbo.I_CHECKHEADER (nolock)
    where   ID_COMPANY = @idc and YEAR_MONTH = @YM_before and ID_CONTACT != 0
    group by ID_CONTACT, ID_ORGANIZATION, ID_COMPANY, YEAR_MONTH;

    -- Агрегат I_CHECK: клиент × город × категория CVM × орг, LFL-периметр месяца. Т3: только строки покупки
    -- (количество > 0) и категории CVM без «Служебного»; строка клиента и категории — только с продажами больше 0
    drop table if exists #chk_agg;
    select  a.YEAR_MONTH
          , a.ID_CONTACT
          , c.ID_CITY_REPORT
          , k.ID_CVM_CATEGORY
          , QUANTITY = sum(a.QUANTITY)
          , COST_DISCOUNT = sum(a.COST_DISCOUNT)
          , REAL_DISCOUNT = sum(a.REAL_DISCOUNT)
          , DISCOUNT = sum(a.DISCOUNT)
          , CHECKS = count(distinct a.ID_CHECK)
          , a.ID_COMPANY
          , a.ID_ORGANIZATION
    into    #chk_agg
    from    dbo.I_CHECK as a (nolock)
            inner join dbo.I_SHOP as c (nolock)
                on  a.ID_SHOP = c.ID_SHOP
                and a.ID_COMPANY = c.ID_COMPANY
            inner join dbo.I_LFL_SHOPS as d (nolock)
                on  a.ID_SHOP = d.ID_SHOP
                and a.ID_COMPANY = d.ID_COMPANY
                and d.YEAR_month = @YM_before
            inner join #pc as k
                on  k.ID_PRODUCT = a.ID_PRODUCT
    where   a.YEAR_MONTH = @YM_before
        and a.ID_COMPANY = @idc
        and a.ID_CONTACT != 0
        and a.QUANTITY > 0
    group by a.YEAR_MONTH
          , a.ID_CONTACT
          , c.ID_CITY_REPORT
          , k.ID_CVM_CATEGORY
          , a.ID_COMPANY
          , a.ID_ORGANIZATION
    having  sum(a.COST_DISCOUNT) > 0;

            set @prep = 1;
        end;   -- предрасчёты M2–M3

    BEGIN TRY
    BEGIN TRANSACTION;

        -- M3: carry-over храним ровно два месяца
        delete from #buyers  where YEAR_MONTH < @YM_before;
        delete from #chk_agg where YEAR_MONTH < @YM_before;
        truncate table #tr;
        truncate table #tps;

        -- ============================================================
        -- Т3: ИНДЕКС ЦЕН ТЕХ ЖЕ ТОВАРОВ и ГРАНИЦЫ СЕГМЕНТОВ ПО ЧЕКУ
        -- ============================================================
        -- Индекс цен @YM к тому же месяцу год назад — Фишер (среднее геометрическое Ласпейреса и Пааше) по товарам,
        -- купленным в обоих месяцах, сеть, канал (I_CVM_PRODUCT_MONTH — витрина ценового индекса). Товары, цена которых
        -- изменилась больше чем втрое в любую сторону, не участвуют (ошибки цены, смена фасовки). Только прямое сравнение
        -- с тем же месяцем: цепочка «месяц к месяцу» на акционных ценах дрейфует (Т1: ×1,006 против ×1,078).
        drop table if exists #pm;
        select  ID_ORGANIZATION
              , ID_PRODUCT
              , YEAR_MONTH
              , T = cast(sum(TURNOVER_BUY) as float)
              , Q = cast(sum(QTY_BUY) as float)
        into    #pm
        from    dbo.I_CVM_PRODUCT_MONTH (nolock)
        where   YEAR_MONTH in (@YM - 100, @YM)
            and ID_COMPANY = @idc
        group by ID_ORGANIZATION, ID_PRODUCT, YEAR_MONTH
        having  sum(QTY_BUY) > 0 and sum(TURNOVER_BUY) > 0
        option (recompile);

        drop table if exists #defl;
        select  a.ID_ORGANIZATION
              , DEFL = sqrt( (sum(a.Q * b.T / b.Q) / sum(a.T))      -- Ласпейрес: корзина прошлого года в новых ценах
                           * (sum(b.T) / sum(b.Q * a.T / a.Q)) )    -- Пааше: корзина этого года в ценах прошлого
        into    #defl
        from    #pm as a
                inner join #pm as b
                    on  b.ID_ORGANIZATION = a.ID_ORGANIZATION
                    and b.ID_PRODUCT = a.ID_PRODUCT
                    and b.YEAR_MONTH = @YM
        where   a.YEAR_MONTH = @YM - 100
            and b.T / b.Q <= 3 * (a.T / a.Q)
            and 3 * (b.T / b.Q) >= a.T / a.Q
        group by a.ID_ORGANIZATION;

        -- Покупатели того же месяца год назад (Активные LFL ниже) и их средний чек (границы сегментов по чеку) — один скан
        -- шапки. Средний чек — по чекам покупки (оплата больше 0); клиенты только с возвратами в границы не входят.
        drop table if exists #ly;
        select  ID_CONTACT
              , ID_ORGANIZATION
              , ID_COMPANY
              , N_BUY = sum(iif(COST_DISCOUNT > 0, 1, 0))
              , S_BUY = sum(iif(COST_DISCOUNT > 0, COST_DISCOUNT, 0))
        into    #ly
        from    dbo.I_CHECKHEADER (nolock)
        where   ID_COMPANY = @idc
            and YEAR_MONTH = @YM - 100
            and ID_CONTACT != 0
        group by ID_CONTACT, ID_ORGANIZATION, ID_COMPANY;

        -- Границы — трети клиентов того же месяца прошлого года по среднему чеку, по каналу (в месяце базы — ровно по трети)
        drop table if exists #ck_bounds;
        ;with n as
        (
            select  ID_ORGANIZATION, CNT = count(*)
            from    #ly
            where   N_BUY > 0
            group by ID_ORGANIZATION
        ), r as
        (
            select  ID_ORGANIZATION
                  , A   = S_BUY / nullif(N_BUY, 0)
                  , RN  = row_number() over (partition by ID_ORGANIZATION order by S_BUY / nullif(N_BUY, 0))
            from    #ly
            where   N_BUY > 0
        )
        select  r.ID_ORGANIZATION
              , A1 = max(case when r.RN = ceiling(n.CNT / 3.0) then r.A end)
              , A2 = max(case when r.RN = ceiling(n.CNT * 2 / 3.0) then r.A end)
        into    #ck_bounds
        from    r
                inner join n
                    on  n.ID_ORGANIZATION = r.ID_ORGANIZATION
        group by r.ID_ORGANIZATION;

        -- Покупатели @YM (все чеки, как раньше) и их средний чек по чекам покупки — один скан шапки
        drop table if exists #hdr;
        select  ID_CONTACT
              , ID_ORGANIZATION
              , ID_COMPANY
              , N_BUY = sum(iif(COST_DISCOUNT > 0, 1, 0))
              , S_BUY = sum(iif(COST_DISCOUNT > 0, COST_DISCOUNT, 0))
        into    #hdr
        from    dbo.I_CHECKHEADER (nolock)
        where   YEAR_MONTH = @YM
            and ID_COMPANY = @idc
            and ID_CONTACT != 0
        group by ID_CONTACT, ID_ORGANIZATION, ID_COMPANY;

        -- M3: покупатели @YM
        insert into #buyers (ID_CONTACT, ID_ORGANIZATION, ID_COMPANY, YEAR_MONTH)
        select  ID_CONTACT, ID_ORGANIZATION, ID_COMPANY, @YM
        from    #hdr;

        -- ЧЕКОВЫЕ СЕГМЕНТЫ клиентов (Т3, вместо порогов FIX K 1 500 / 500 ₽): средний чек @YM в ценах того же месяца
        -- прошлого года против третей прошлого года; границы нестрогие (как раньше). Нет индекса цен или границ по каналу —
        -- клиенты канала без сегмента по чеку.
        drop table if exists #ckclients;
        select  h.ID_CONTACT
              , SEGMENT = case when h.S_BUY / nullif(h.N_BUY, 0) / d.DEFL >= b.A2 then 1000
                               when h.S_BUY / nullif(h.N_BUY, 0) / d.DEFL <= b.A1 then 1002
                               else 1001
                          end   -- 1000 - BIG, 1001 - AVG, 1002 - LOW
              , h.ID_COMPANY
              , h.ID_ORGANIZATION
        into    #ckclients
        from    #hdr as h
                inner join #defl as d
                    on  d.ID_ORGANIZATION = h.ID_ORGANIZATION
                inner join #ck_bounds as b
                    on  b.ID_ORGANIZATION = h.ID_ORGANIZATION
        where   h.N_BUY > 0
            and d.DEFL > 0
            and b.A1 is not null
            and b.A2 is not null;

        -- ============================================================
        -- СЕГМЕНТЫ МЕСЯЦА (приоритет: NEW → RANDOM → CHURN → LFL → ACTIVE)
        -- ============================================================

        -- NEW (12): первый месяц покупки = @YM
        drop table if exists #all_segm;
        select  ID_CONTACT
              , ID_ORGANIZATION
              , ID_COMPANY
              , SEGMENT = 12
        into    #all_segm
        from    dbo.I_CVM_CONTACT as b (nolock)
        where   ID_CONTACT != 0
            and b.FIRST_YM in (@YM)
            and b.ID_COMPANY = @idc                       -- FIX J: фильтр компании
        group by ID_CONTACT, ID_ORGANIZATION, ID_COMPANY;

        -- RANDOM (15): средний интервал покупок > 6 недель
        drop table if exists #weeks;
        -- M2: срез 56-недельного окна из предрасчитанного #wk_all
        ;with a as
        (
            select  ID_CONTACT
                  , ID_ORGANIZATION
                  , WEEKNUMBER
                  , ID_COMPANY
            from    #wk_all
            where   MAX_DATA >= dateadd(day, +1, dateadd(week, -56, @act_date))
                and MIN_DATA <= @act_date
        ), one_purch as
        (
            select  ID_CONTACT
                  , ID_ORGANIZATION
            from    a
            group by ID_CONTACT, ID_ORGANIZATION
            having  count(*) = 1
        ), with_first_purch as
        (
            select  a.ID_CONTACT
                  , a.ID_ORGANIZATION
                  , a.WEEKNUMBER
                  , a.ID_COMPANY
                  , DELTA_WEEK = case when c.ID_CONTACT is not null then 56
                                      -- FIX F: партиция и по организации
                                      else a.WEEKNUMBER - lag(a.WEEKNUMBER) over(partition by a.ID_CONTACT, a.ID_ORGANIZATION order by a.WEEKNUMBER)
                                 end
            from    a
                    left join one_purch c
                        on  a.ID_CONTACT = c.ID_CONTACT
                        and a.ID_ORGANIZATION = c.ID_ORGANIZATION
        )
        select  a.ID_CONTACT
              , a.ID_ORGANIZATION
              , a.ID_COMPANY
              , AVG_DELTA_WEEK = round(avg(DELTA_WEEK * 1.0), 0)
        into    #weeks
        from    with_first_purch a
        where   DELTA_WEEK is not null
        group by a.ID_CONTACT, a.ID_ORGANIZATION, a.ID_COMPANY;

        -- M2: джойн на «покупателей за 52 недели» удалён — покупатели @YM
        -- (f) всегда внутри этого окна, он не отсекал ни одной строки
        ;with f as
        (
            select  ID_CONTACT
                  , ID_ORGANIZATION
                  , ID_COMPANY
            from    #buyers
            where   YEAR_MONTH = @YM
        )
        insert into #all_segm
        select  a.ID_CONTACT
              , a.ID_ORGANIZATION
              , a.ID_COMPANY
              , SEGMENT = 15
        from    #weeks a
                left join #all_segm b
                    on  a.ID_CONTACT = b.ID_CONTACT
                    and a.ID_ORGANIZATION = b.ID_ORGANIZATION
                    and a.ID_COMPANY = b.ID_COMPANY
                inner join f
                    on  a.ID_CONTACT = f.ID_CONTACT
                    and a.ID_ORGANIZATION = f.ID_ORGANIZATION
                    and a.ID_COMPANY = f.ID_COMPANY
        where   AVG_DELTA_WEEK > 6
            and b.ID_CONTACT is null
            and a.ID_COMPANY = @idc
        group by a.ID_CONTACT, a.ID_ORGANIZATION, a.ID_COMPANY;

        -- CHURN (16): покупал в @YM_before, не покупал в @YM
        -- M3: оба списка из carry-over #buyers, без сканов I_CHECKHEADER
        ;with t as
        (
            select  ID_CONTACT, ID_ORGANIZATION, ID_COMPANY
            from    #buyers
            where   YEAR_MONTH = @YM_before
        ), a as
        (
            select  ID_CONTACT, ID_ORGANIZATION, ID_COMPANY
            from    #buyers
            where   YEAR_MONTH = @YM
        )
        insert into #all_segm
        select  t.ID_CONTACT
              , t.ID_ORGANIZATION
              , t.ID_COMPANY
              , SEGMENT = 16
        from    t
                left join a
                    on  a.ID_CONTACT = t.ID_CONTACT
                    and a.ID_ORGANIZATION = t.ID_ORGANIZATION
                    and a.ID_COMPANY = t.ID_COMPANY
                left join #all_segm c                     -- FIX D: дедуп от t, не от a
                    on  t.ID_CONTACT = c.ID_CONTACT
                    and t.ID_ORGANIZATION = c.ID_ORGANIZATION
                    and t.ID_COMPANY = c.ID_COMPANY
        where   a.ID_CONTACT is null
            and c.ID_CONTACT is null
        group by t.ID_CONTACT, t.ID_COMPANY, t.ID_ORGANIZATION;

        -- ACTIVE-LFL (14): покупал в @YM и в том же месяце год назад (Т3: покупатели прошлого года — из #ly)
        insert into #all_segm
        select  a.ID_CONTACT
              , a.ID_ORGANIZATION
              , a.ID_COMPANY
              , SEGMENT = 14
        from    #buyers a                                   -- M3: покупатели @YM
                inner join #ly b
                    on  a.ID_CONTACT = b.ID_CONTACT
                    and a.ID_COMPANY = b.ID_COMPANY
                    and a.ID_ORGANIZATION = b.ID_ORGANIZATION
                left join #all_segm c
                    on  a.ID_CONTACT = c.ID_CONTACT
                    and a.ID_ORGANIZATION = c.ID_ORGANIZATION
                    and a.ID_COMPANY = c.ID_COMPANY
        where   c.ID_CONTACT is null
            and a.YEAR_MONTH = @YM
        group by a.ID_CONTACT, a.ID_ORGANIZATION, a.ID_COMPANY;

        -- ACTIVE (13): остальные покупатели @YM
        insert into #all_segm
        select  a.ID_CONTACT
              , a.ID_ORGANIZATION
              , a.ID_COMPANY
              , SEGMENT = 13
        from    #buyers a                                   -- M3: покупатели @YM
                left join #all_segm b
                    on  a.ID_CONTACT = b.ID_CONTACT
                    and a.ID_COMPANY = b.ID_COMPANY
                    and a.ID_ORGANIZATION = b.ID_ORGANIZATION   -- FIX C: было ID_CONTACT дважды
        where   b.ID_CONTACT is null
            and a.YEAR_MONTH = @YM
        group by a.ID_CONTACT, a.ID_ORGANIZATION, a.ID_COMPANY;

        -- ЦЕНОВОЙ СЕГМЕНТ клиента за @YM (Т3, вместо порогов 0,359 / 0,25 по I_CONT_ASSORT_M): готовый PRICE_SEGMENT
        -- витрины ценового индекса — строка клиента на последнюю покупку месяца (индекс за 56 дней, границы прошлого
        -- года, по каналу). Индекса нет (мало покупок) — клиент без ценового сегмента.
        drop table if exists #psclients;
        select  w.ID_CONTACT
              , SEGMENT = max(w.PRICE_SEGMENT)   -- 1 - high, 3 - low, 2 - avg; строка одна, max — защита от дубля недели
              , ID_COMPANY = @idc                -- витрины индекса — одна компания
              , w.ID_ORGANIZATION
        into    #psclients
        from    (
                    select  ID_CONTACT
                          , ID_ORGANIZATION
                          , LD = max(LAST_DATE)
                    from    dbo.I_CVM_CLIENT_WEEK (nolock)
                    where   LAST_DATE >= @m_from
                        and LAST_DATE <= @act_date
                    group by ID_CONTACT, ID_ORGANIZATION
                ) as m
                inner join dbo.I_CVM_CLIENT_WEEK as w (nolock)
                    on  w.ID_CONTACT = m.ID_CONTACT
                    and w.ID_ORGANIZATION = m.ID_ORGANIZATION
                    and w.LAST_DATE = m.LD
        where   w.LAST_DATE >= @m_from
            and w.LAST_DATE <= @act_date
            and w.PRICE_SEGMENT is not null
            and w.ID_CONTACT != 0
        group by w.ID_CONTACT, w.ID_ORGANIZATION
        option (recompile);

        -- ============================================================
        -- I_TREND_PRICE_SEGMENT: статусные сегменты
        -- ============================================================
        -- Т3: вставки в #tps — по именам колонок (раньше BONUS_PAY ложился в колонку UNIQUE_SKU; теперь она пустая)

        -- CHURN: метрики по последнему активному месяцу (@YM_before),
        -- записываются с YEAR_MONTH = @YM (семантика сохранена)
        ;with t as
        (
            select  id_organization, ALL_CLIENTS = count(distinct a.ID_CONTACT)
            from    dbo.I_CHECKHEADER a (nolock)
            where   ID_COMPANY = @idc and YEAR_MONTH = @YM_before
            group by id_organization
        )
        insert into #tps
              ( SEGMENT, SEGMENT_NAME, YEAR_MONTH, CLIENTS, SHARE_CLIENTS, BUDGET, COUNT_CHECK, AVG_CHECK, AVG_COST_SKU, AVG_SKU
              , LOYALTY_INDEX, REAL_SALE, SALE, PRICE_INDEX, ID_COMPANY, ID_ORGANIZATION, BONUS_ACCRUAL )
        select  SEGMENT
              , SEGMENT_NAME = 'CHURN'
              , YEAR_MONTH = @YM
              , CLIENTS = count(distinct a.ID_CONTACT)
              , SHARE_CLIENTS = (count(distinct a.ID_CONTACT) * 1.00) / (ALL_CLIENTS * 1.00)
              , [BUDGET] = sum(COST_DISCOUNT) / count(distinct a.ID_CONTACT)
              , [COUNT_CHECK] = (count(a.ID_CHECK) * 1.0) / count(distinct a.ID_CONTACT)
              , [AVG_CHECK] = sum(COST_DISCOUNT) / count(a.ID_CHECK)
              , [AVG_COST_SKU] = sum(COST_DISCOUNT) / sum(SKU)
              , [AVG_SKU] = (sum(SKU) * 1.) / count(a.ID_CHECK)
              , LOYALTY_INDEX = avg([PENETRATION_CHECK])
              , REAL_SALE = sum(REAL_DISCOUNT) / (sum(COST_DISCOUNT) + sum(REAL_DISCOUNT))
              , SALE = sum(DISCOUNT) / (sum(COST_DISCOUNT) + sum(REAL_DISCOUNT))
              , PRICE_INDEX = avg(PRICE_INDEX)
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
              , BONUS_ACCRUAL = sum(BONUS_ACCRUAL)
        from    dbo.I_CHECKHEADER as a (nolock)
                inner join #all_segm as b
                    on  a.ID_CONTACT = b.ID_CONTACT
                    and a.ID_COMPANY = b.ID_COMPANY
                    and a.ID_ORGANIZATION = b.ID_ORGANIZATION
                inner join t
                    on  t.ID_ORGANIZATION = a.ID_ORGANIZATION
                inner join dbo.I_CH_INDEXES as c (nolock)
                    on  a.ID_CHECK = c.ID_CHECK
                    and a.ID_COMPANY = c.ID_COMPANY
                    and a.ID_ORGANIZATION = c.ID_ORGANIZATION
        where   SEGMENT = 16 and a.YEAR_MONTH = @YM_before and a.ID_COMPANY = @idc
        group by SEGMENT, ALL_CLIENTS, YEAR_MONTH, a.ID_COMPANY, a.ID_ORGANIZATION;

        -- NEW / ACTIVE / ACTIVE_LFL / RANDOM: метрики по @YM
        ;with t as
        (
            select  ID_ORGANIZATION, ALL_CLIENTS = count(distinct a.ID_CONTACT)
            from    #all_segm a
            where   SEGMENT <> 16
            group by ID_ORGANIZATION
        )
        insert into #tps
              ( SEGMENT, SEGMENT_NAME, YEAR_MONTH, CLIENTS, SHARE_CLIENTS, BUDGET, COUNT_CHECK, AVG_CHECK, AVG_COST_SKU, AVG_SKU
              , LOYALTY_INDEX, REAL_SALE, SALE, PRICE_INDEX, ID_COMPANY, ID_ORGANIZATION, BONUS_ACCRUAL )
        select  SEGMENT
              , SEGMENT_NAME = iif(SEGMENT = 12, 'NEW', iif(SEGMENT = 13, 'ACTIVE', iif(SEGMENT = 14, 'ACTIVE_LFL', 'RANDOM')))
              , YEAR_MONTH
              , CLIENTS = count(distinct a.ID_CONTACT)
              , SHARE_CLIENTS = (count(distinct a.ID_CONTACT) * 1.00) / (ALL_CLIENTS * 1.00)
              , [BUDGET] = sum(COST_DISCOUNT) / count(distinct a.ID_CONTACT)
              , [COUNT_CHECK] = (count(a.ID_CHECK) * 1.0) / count(distinct a.ID_CONTACT)
              , [AVG_CHECK] = sum(COST_DISCOUNT) / count(a.ID_CHECK)
              , [AVG_COST_SKU] = sum(COST_DISCOUNT) / sum(SKU)
              , [AVG_SKU] = (sum(SKU) * 1.) / count(a.ID_CHECK)
              , LOYALTY_INDEX = avg([PENETRATION_CHECK])
              , REAL_SALE = sum(REAL_DISCOUNT) / (sum(COST_DISCOUNT) + sum(REAL_DISCOUNT))
              , SALE = sum(DISCOUNT) / (sum(COST_DISCOUNT) + sum(REAL_DISCOUNT))
              , PRICE_INDEX = avg(PRICE_INDEX)
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
              , BONUS_ACCRUAL = sum(BONUS_ACCRUAL)
        from    dbo.I_CHECKHEADER as a (nolock)
                inner join #all_segm as b
                    on  a.ID_CONTACT = b.ID_CONTACT
                    and a.ID_COMPANY = b.ID_COMPANY
                    and a.ID_ORGANIZATION = b.ID_ORGANIZATION
                inner join t
                    on  t.ID_ORGANIZATION = a.ID_ORGANIZATION
                inner join dbo.I_CH_INDEXES as c (nolock)
                    on  a.ID_CHECK = c.ID_CHECK
                    and a.ID_COMPANY = c.ID_COMPANY
                    and a.ID_ORGANIZATION = c.ID_ORGANIZATION
        where   a.YEAR_MONTH = @YM
            and SEGMENT != 16
            and a.ID_COMPANY = @idc                              -- FIX J
        group by SEGMENT, ALL_CLIENTS, YEAR_MONTH, a.ID_COMPANY, a.ID_ORGANIZATION;

        -- Сводка по ценовым сегментам (HIGH / MIDDLE / LOW)
        ;with t as
        (
            select  ALL_CLIENTS = count(ID_CONTACT), ID_ORGANIZATION
            from    #psclients
            group by ID_ORGANIZATION
        )
        insert into #tps
              ( SEGMENT, SEGMENT_NAME, YEAR_MONTH, CLIENTS, SHARE_CLIENTS, BUDGET, COUNT_CHECK, AVG_CHECK, AVG_COST_SKU, AVG_SKU
              , LOYALTY_INDEX, REAL_SALE, SALE, PRICE_INDEX, ID_COMPANY, ID_ORGANIZATION, BONUS_ACCRUAL )
        select  SEGMENT
              , SEGMENT_NAME = iif(SEGMENT = 1, 'HIGH', iif(SEGMENT = 2, 'MIDDLE', 'LOW'))
              , YEAR_MONTH = @YM
              , CLIENTS = count(distinct a.ID_CONTACT)
              , SHARE_CLIENTS = (count(distinct a.ID_CONTACT) * 1.00) / (ALL_CLIENTS * 1.00)
              , [BUDGET] = sum(COST_DISCOUNT) / count(distinct a.ID_CONTACT)
              , [COUNT_CHECK] = (count(a.ID_CHECK) * 1.0) / count(distinct a.ID_CONTACT)
              , [AVG_CHECK] = sum(COST_DISCOUNT) / count(a.ID_CHECK)
              , [AVG_COST_SKU] = sum(COST_DISCOUNT) / sum(SKU)
              , [AVG_SKU] = (sum(SKU) * 1.) / count(a.ID_CHECK)
              , LOYALTY_INDEX = avg([PENETRATION_CHECK])
              , REAL_SALE = sum(REAL_DISCOUNT) / (sum(COST_DISCOUNT) + sum(REAL_DISCOUNT))
              , SALE = sum(DISCOUNT) / (sum(COST_DISCOUNT) + sum(REAL_DISCOUNT))
              , PRICE_INDEX = avg(PRICE_INDEX)
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
              , BONUS_ACCRUAL = sum(BONUS_ACCRUAL)
        from    dbo.I_CHECKHEADER as a (nolock)
                inner join #psclients as b
                    on  a.ID_CONTACT = b.ID_CONTACT
                    and a.ID_COMPANY = b.ID_COMPANY
                    and a.ID_ORGANIZATION = b.ID_ORGANIZATION
                inner join t
                    on  t.ID_ORGANIZATION = a.ID_ORGANIZATION
                inner join dbo.I_CH_INDEXES as c (nolock)
                    on  a.ID_CHECK = c.ID_CHECK
                    and a.ID_COMPANY = c.ID_COMPANY
                    and a.ID_ORGANIZATION = c.ID_ORGANIZATION
        where   a.YEAR_MONTH = @YM
            and a.ID_COMPANY = @idc
        group by SEGMENT, ALL_CLIENTS, YEAR_MONTH, a.ID_COMPANY, a.ID_ORGANIZATION;

        -- Сводка по чековым сегментам (BIG / AVG / LOW CHECK)
        ;with t as
        (
            select  ALL_CLIENTS = count(ID_CONTACT), ID_ORGANIZATION
            from    #ckclients
            group by ID_ORGANIZATION
        )
        insert into #tps
              ( SEGMENT, SEGMENT_NAME, YEAR_MONTH, CLIENTS, SHARE_CLIENTS, BUDGET, COUNT_CHECK, AVG_CHECK, AVG_COST_SKU, AVG_SKU
              , LOYALTY_INDEX, REAL_SALE, SALE, PRICE_INDEX, ID_COMPANY, ID_ORGANIZATION, BONUS_ACCRUAL )
        select  SEGMENT
              , SEGMENT_NAME = iif(SEGMENT = 1000, 'BIG_CHECK', iif(SEGMENT = 1001, 'AVG CHECK', 'LOW_CHECK'))
              , YEAR_MONTH = @YM
              , CLIENTS = count(distinct a.ID_CONTACT)
              , SHARE_CLIENTS = (count(distinct a.ID_CONTACT) * 1.00) / (ALL_CLIENTS * 1.00)
              , [BUDGET] = sum(COST_DISCOUNT) / count(distinct a.ID_CONTACT)
              , [COUNT_CHECK] = (count(a.ID_CHECK) * 1.0) / count(distinct a.ID_CONTACT)
              , [AVG_CHECK] = sum(COST_DISCOUNT) / count(a.ID_CHECK)
              , [AVG_COST_SKU] = sum(COST_DISCOUNT) / sum(SKU)
              , [AVG_SKU] = (sum(SKU) * 1.) / count(a.ID_CHECK)
              , LOYALTY_INDEX = avg([PENETRATION_CHECK])
              , REAL_SALE = sum(REAL_DISCOUNT) / (sum(COST_DISCOUNT) + sum(REAL_DISCOUNT))
              , SALE = sum(DISCOUNT) / (sum(COST_DISCOUNT) + sum(REAL_DISCOUNT))
              , PRICE_INDEX = avg(PRICE_INDEX)
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
              , BONUS_ACCRUAL = sum(BONUS_ACCRUAL)
        from    dbo.I_CHECKHEADER as a (nolock)
                inner join #ckclients as b
                    on  a.ID_CONTACT = b.ID_CONTACT
                    and a.ID_COMPANY = b.ID_COMPANY
                    and a.ID_ORGANIZATION = b.ID_ORGANIZATION
                inner join t
                    on  t.ID_ORGANIZATION = a.ID_ORGANIZATION
                inner join dbo.I_CH_INDEXES as c (nolock)
                    on  a.ID_CHECK = c.ID_CHECK
                    and a.ID_COMPANY = c.ID_COMPANY
                    and a.ID_ORGANIZATION = c.ID_ORGANIZATION
        where   a.YEAR_MONTH = @YM
            and a.ID_COMPANY = @idc
        group by SEGMENT, ALL_CLIENTS, YEAR_MONTH, a.ID_COMPANY, a.ID_ORGANIZATION;

        -- ============================================================
        -- I_TREND (Т3): все восемь трендов одним расчётом — чеки LFL-магазинов, категории CVM
        -- ============================================================

        -- M3: единственный скан I_CHECK за месяц — общий агрегат (клиент × город × категория × орг, LFL-периметр @YM)
        -- для всех трендов + CHURN следующей итерации. Т3: только строки покупки и категории CVM без «Служебного»
        insert into #chk_agg
              ( YEAR_MONTH, ID_CONTACT, ID_CITY_REPORT, ID_CVM_CATEGORY
              , QUANTITY, COST_DISCOUNT, REAL_DISCOUNT, DISCOUNT
              , CHECKS, ID_COMPANY, ID_ORGANIZATION )
        select  a.YEAR_MONTH
              , a.ID_CONTACT
              , c.ID_CITY_REPORT
              , k.ID_CVM_CATEGORY
              , QUANTITY = sum(a.QUANTITY)
              , COST_DISCOUNT = sum(a.COST_DISCOUNT)
              , REAL_DISCOUNT = sum(a.REAL_DISCOUNT)
              , DISCOUNT = sum(a.DISCOUNT)
              , CHECKS = count(distinct a.ID_CHECK)
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
        from    dbo.I_CHECK as a (nolock)
                inner join dbo.I_SHOP as c (nolock)
                    on  a.ID_SHOP = c.ID_SHOP
                    and a.ID_COMPANY = c.ID_COMPANY
                inner join dbo.I_LFL_SHOPS as d (nolock)
                    on  a.ID_SHOP = d.ID_SHOP
                    and a.ID_COMPANY = d.ID_COMPANY
                    and d.YEAR_month = @YM
                inner join #pc as k
                    on  k.ID_PRODUCT = a.ID_PRODUCT
        where   a.YEAR_MONTH = @YM
            and a.ID_COMPANY = @idc
            and a.ID_CONTACT != 0
            and a.QUANTITY > 0
        group by a.YEAR_MONTH
              , a.ID_CONTACT
              , c.ID_CITY_REPORT
              , k.ID_CVM_CATEGORY
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
        having  sum(a.COST_DISCOUNT) > 0;

        -- Клиент × город × орг × месяц: продажи клиента по категориям CVM (TT, ₽) и его сегмент в каждом семействе.
        -- Строки @YM — жизненный цикл, ценовой, по чеку; строки @YM_before — отток (16 — не вернулся в @YM, 1 — остальные).
        -- Знаменатель доли — свой сегмент (11.10.2026): клиенты сегмента с покупками в LFL-магазинах города.
        drop table if exists #clf;
        select  c.YEAR_MONTH
              , c.ID_CONTACT
              , c.ID_CITY_REPORT
              , c.ID_COMPANY
              , c.ID_ORGANIZATION
              , c.TT
              , SEG_LC = iif(c.YEAR_MONTH = @YM and s.SEGMENT <> 16, s.SEGMENT, null)
              , SEG_CH = iif(c.YEAR_MONTH = @YM_before, iif(s.SEGMENT = 16, 16, 1), null)
              , SEG_P  = iif(c.YEAR_MONTH = @YM, p.SEGMENT, null)
              , SEG_CK = iif(c.YEAR_MONTH = @YM, k.SEGMENT, null)
        into    #clf
        from    (
                    select  YEAR_MONTH
                          , ID_CONTACT
                          , ID_CITY_REPORT
                          , ID_COMPANY
                          , ID_ORGANIZATION
                          , TT = sum(cast(COST_DISCOUNT as float))
                    from    #chk_agg
                    group by YEAR_MONTH, ID_CONTACT, ID_CITY_REPORT, ID_COMPANY, ID_ORGANIZATION
                ) as c
                left join #all_segm as s
                    on  s.ID_CONTACT = c.ID_CONTACT
                    and s.ID_COMPANY = c.ID_COMPANY
                    and s.ID_ORGANIZATION = c.ID_ORGANIZATION
                left join #psclients as p
                    on  p.ID_CONTACT = c.ID_CONTACT
                    and p.ID_COMPANY = c.ID_COMPANY
                    and p.ID_ORGANIZATION = c.ID_ORGANIZATION
                left join #ckclients as k
                    on  k.ID_CONTACT = c.ID_CONTACT
                    and k.ID_COMPANY = c.ID_COMPANY
                    and k.ID_ORGANIZATION = c.ID_ORGANIZATION;

        -- Сегмент × город × орг × категория: прежние показатели (суммы) и суммы для доли категории в продажах сегмента и её
        -- разброса по клиентам (оценка отношения: T1 = Σt, T2 = Σt², TX = Σt·TT по клиентам, купившим категорию)
        drop table if exists #st;
        select  x.FAM
              , x.SEGMENT
              , a.ID_CITY_REPORT
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
              , a.ID_CVM_CATEGORY
              , BUYERS = count(*)
              , QUANTITY = sum(a.QUANTITY)
              , COST_DISCOUNT = sum(a.COST_DISCOUNT)
              , REAL_DISCOUNT = sum(a.REAL_DISCOUNT)
              , DISCOUNT = sum(a.DISCOUNT)
              , CHECKS = sum(a.CHECKS)
              , T1 = sum(v.T)
              , T2 = sum(v.T * v.T)
              , TX = sum(v.T * c.TT)
        into    #st
        from    #chk_agg as a
                inner join #clf as c
                    on  c.YEAR_MONTH = a.YEAR_MONTH
                    and c.ID_CONTACT = a.ID_CONTACT
                    and c.ID_CITY_REPORT = a.ID_CITY_REPORT
                    and c.ID_COMPANY = a.ID_COMPANY
                    and c.ID_ORGANIZATION = a.ID_ORGANIZATION
                cross apply (values (cast(a.COST_DISCOUNT as float))) as v (T)
                cross apply (values (1, c.SEG_LC), (2, c.SEG_CH), (3, c.SEG_P), (4, c.SEG_CK)) as x (FAM, SEGMENT)
        where   x.SEGMENT is not null
        group by x.FAM, x.SEGMENT, a.ID_CITY_REPORT, a.ID_COMPANY, a.ID_ORGANIZATION, a.ID_CVM_CATEGORY;

        -- Сегмент × город × орг: клиенты (N) и их продажи (S1 = ΣTT, S2 = ΣTT²)
        drop table if exists #stt;
        select  x.FAM
              , x.SEGMENT
              , c.ID_CITY_REPORT
              , c.ID_COMPANY
              , c.ID_ORGANIZATION
              , N  = count(*)
              , S1 = sum(c.TT)
              , S2 = sum(c.TT * c.TT)
        into    #stt
        from    #clf as c
                cross apply (values (1, c.SEG_LC), (2, c.SEG_CH), (3, c.SEG_P), (4, c.SEG_CK)) as x (FAM, SEGMENT)
        where   x.SEGMENT is not null
        group by x.FAM, x.SEGMENT, c.ID_CITY_REPORT, c.ID_COMPANY, c.ID_ORGANIZATION;

        -- Показатели сегмента по категории. Прежние — как в #lfl_trend до Т3 (доля покупателей, доля скидок, чеков и ₽ на
        -- покупателя, цена единицы); новые — доля категории в продажах сегмента SH = ΣT / ΣTT и дисперсия этой оценки
        -- по клиентам SH_VAR = (T2 − 2·SH·TX + SH²·S2) / (N·(N − 1)·(S1 / N)²); у сегмента из одного клиента в городе
        -- дисперсии нет — прежние колонки считаются, маркера нет
        drop table if exists #e;
        select  s.FAM
              , s.SEGMENT
              , s.ID_CITY_REPORT
              , s.ID_COMPANY
              , s.ID_ORGANIZATION
              , s.ID_CVM_CATEGORY
              , CONTACTS = (s.BUYERS * 1.0) / (t.N * 1.00)
              , REAL_DISCOUNT = s.REAL_DISCOUNT / nullif(s.REAL_DISCOUNT + s.COST_DISCOUNT, 0)
              , DISCOUNT = s.DISCOUNT / nullif(s.REAL_DISCOUNT + s.COST_DISCOUNT, 0)
              , CHECKS = (s.CHECKS * 1.00) / (s.BUYERS * 1.00)
              , PRICE = s.COST_DISCOUNT / nullif(s.QUANTITY, 0)
              , COST_DISCOUNT = s.COST_DISCOUNT / s.BUYERS
              , SH = s.T1 / t.S1
              , SH_VAR = case when t.N > 1
                              then (s.T2 - 2e0 * (s.T1 / t.S1) * s.TX + square(s.T1 / t.S1) * t.S2)
                                   / nullif(t.N * (t.N - 1e0) * square(t.S1 / t.N), 0)
                         end
              , S1 = t.S1
        into    #e
        from    #st as s
                inner join #stt as t
                    on  t.FAM = s.FAM
                    and t.SEGMENT = s.SEGMENT
                    and t.ID_CITY_REPORT = s.ID_CITY_REPORT
                    and t.ID_COMPANY = s.ID_COMPANY
                    and t.ID_ORGANIZATION = s.ID_ORGANIZATION
        where   t.S1 > 0;

        -- Тренд = сегмент против своей базы (#sm). Прежние колонки — как раньше (PER_DELTA_* — относительная разница
        -- с базой, REAL_DISCOUNT — разность долей). Над-индекс — доля категории в продажах сегмента / в продажах базы;
        -- ошибка его логарифма — из ошибок обеих долей; рубли сверх ожидаемого — (доля сегмента − доля базы) × продажи
        -- сегмента. Категории нет у базы — индекса нет.
        drop table if exists #ix;
        select  m.SEGMENT_NAME
              , a.ID_CITY_REPORT
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
              , a.ID_CVM_CATEGORY
              , CONTACTS = a.CONTACTS
              , PER_DELTA_CONTACTS = IIF(isnull(b.CONTACTS, 0) = 0, NULL, ((a.CONTACTS - b.CONTACTS) * 1.00) / (b.CONTACTS * 1.00))
              , PRICE = a.PRICE
              , PER_DELTA_PRICE = IIF(isnull(b.PRICE, 0) = 0, NULL, ((a.PRICE - b.PRICE) * 1.00) / (b.PRICE * 1.00))
              , COST_DISCOUNT = a.COST_DISCOUNT
              , REAL_DISCOUNT = (a.REAL_DISCOUNT - b.REAL_DISCOUNT)
              , PER_DELTA_REAL_DISCOUNT = IIF(isnull(b.REAL_DISCOUNT, 0) = 0, NULL, ((a.REAL_DISCOUNT - b.REAL_DISCOUNT) * 1.00) / (b.REAL_DISCOUNT * 1.00))
              , DISCOUNT = a.DISCOUNT
              , PER_DELTA_DISCOUNT = IIF(isnull(b.DISCOUNT, 0) = 0, NULL, ((a.DISCOUNT - b.DISCOUNT) * 1.00) / (b.DISCOUNT * 1.00))
              , CHECKS_PER_CLIENT = a.CHECKS
              , PER_DELTA_CHECKS = IIF(isnull(b.CHECKS, 0) = 0, NULL, ((a.CHECKS - b.CHECKS) * 1.00) / (b.CHECKS * 1.00))
              , SALES_SHARE = a.SH
              , SALES_SHARE_BASE = b.SH
              , SALES_INDEX = a.SH / nullif(b.SH, 0)
              , SE = case when a.SH_VAR is not null and b.SH_VAR is not null   -- дисперсия есть у обеих долей
                          then sqrt(iif(a.SH_VAR > 0, a.SH_VAR, 0e0) / nullif(square(a.SH), 0)
                                  + iif(b.SH_VAR > 0, b.SH_VAR, 0e0) / nullif(square(b.SH), 0))
                     end
              , SALES_EXTRA = (a.SH - b.SH) * a.S1
              , SALES_MARK = cast(null as smallint)
        into    #ix
        from    #e as a
                inner join #sm as m
                    on  m.FAM = a.FAM
                    and m.SEGMENT = a.SEGMENT
                left join #e as b
                    on  b.FAM = a.FAM
                    and b.SEGMENT = m.BASE
                    and b.ID_CITY_REPORT = a.ID_CITY_REPORT
                    and b.ID_COMPANY = a.ID_COMPANY
                    and b.ID_ORGANIZATION = a.ID_ORGANIZATION
                    and b.ID_CVM_CATEGORY = a.ID_CVM_CATEGORY;

        -- Надёжный маркер (Т1): две односторонние проверки на категорию — «индекс выше ×1,10» и «ниже 1/1,10» (логарифм
        -- индекса — нормальное приближение); p-значение — хвост нормального распределения (Абрамовиц–Стиган 26.2.17,
        -- ошибка < 1e-7). Поправка Бенджамини–Хохберга на 5 % ложных маркеров — по всем проверкам сегмента, города и канала
        -- за месяц: проходят p ≤ p(k), где k — наибольший номер в порядке возрастания p с p(k) ≤ 0,05·k / m.
        drop table if exists #tests;
        select  i.SEGMENT_NAME
              , i.ID_CITY_REPORT
              , i.ID_COMPANY
              , i.ID_ORGANIZATION
              , i.ID_CVM_CATEGORY
              , z.KIND
              , P = case when z.Z > 37 then 0e0
                         when z.Z < -37 then 1e0
                         when z.Z >= 0 then q.Q
                         else 1e0 - q.Q
                    end
        into    #tests
        from    #ix as i
                cross apply (values ( 1, (log(nullif(i.SALES_INDEX, 0)) - log(1.1e0)) / nullif(i.SE, 0))
                                  , (-1, (-log(1.1e0) - log(nullif(i.SALES_INDEX, 0))) / nullif(i.SE, 0))) as z (KIND, Z)
                cross apply (select ZA = iif(abs(z.Z) > 37, 37e0, abs(z.Z))) as za
                cross apply (select T = 1e0 / (1e0 + 0.2316419e0 * za.ZA)) as t
                cross apply (select Q = exp(-za.ZA * za.ZA / 2e0) / 2.506628274631e0 * t.T
                                        * (0.319381530e0 + t.T * (-0.356563782e0 + t.T * (1.781477937e0
                                        + t.T * (-1.821255978e0 + t.T * 1.330274429e0))))) as q
        where   i.SALES_INDEX > 0
            and i.SE > 0;

        update  i
        set     SALES_MARK = 0
        from    #ix as i
        where   i.SALES_INDEX > 0
            and i.SE > 0;

        ;with r as
        (
            select  SEGMENT_NAME, ID_CITY_REPORT, ID_COMPANY, ID_ORGANIZATION, P
                  , RN = row_number() over (partition by SEGMENT_NAME, ID_CITY_REPORT, ID_COMPANY, ID_ORGANIZATION order by P)
                  , M  = count(*)     over (partition by SEGMENT_NAME, ID_CITY_REPORT, ID_COMPANY, ID_ORGANIZATION)
            from    #tests
        ), cut as
        (
            select  SEGMENT_NAME, ID_CITY_REPORT, ID_COMPANY, ID_ORGANIZATION
                  , P_CUT = max(P)
            from    r
            where   P <= 0.05e0 * RN / M
            group by SEGMENT_NAME, ID_CITY_REPORT, ID_COMPANY, ID_ORGANIZATION
        )
        update  i
        set     SALES_MARK = t.KIND
        from    #ix as i
                inner join #tests as t
                    on  t.SEGMENT_NAME = i.SEGMENT_NAME
                    and t.ID_CITY_REPORT = i.ID_CITY_REPORT
                    and t.ID_COMPANY = i.ID_COMPANY
                    and t.ID_ORGANIZATION = i.ID_ORGANIZATION
                    and t.ID_CVM_CATEGORY = i.ID_CVM_CATEGORY
                inner join cut as c
                    on  c.SEGMENT_NAME = t.SEGMENT_NAME
                    and c.ID_CITY_REPORT = t.ID_CITY_REPORT
                    and c.ID_COMPANY = t.ID_COMPANY
                    and c.ID_ORGANIZATION = t.ID_ORGANIZATION
        where   t.P <= c.P_CUT;

        insert into #tr
              ( ID_CITY_REPORT, CITY_REPORT, YEAR_MONTH, SEGMENT
              , I_CATEGORY_5
              , CONTACTS, PER_DELTA_CONTACTS, PRICE, PER_DELTA_PRICE
              , COST_DISCOUNT, REAL_DISCOUNT, PER_DELTA_REAL_DISCOUNT
              , DISCOUNT, PER_DELTA_DISCOUNT, CHECKS_PER_CLIENT, PER_DELTA_CHECKS
              , ID_COMPANY, ID_ORGANIZATION
              , ID_CVM_CATEGORY, SALES_SHARE, SALES_SHARE_BASE, SALES_INDEX, SALES_INDEX_LOW, SALES_INDEX_HIGH
              , SALES_MARK, SALES_EXTRA )
        select  i.ID_CITY_REPORT
              , ct.CITY_REPORT
              , YEAR_MONTH = @YM       -- у оттока — месяц фиксации (чеки — @YM_before)
              , i.SEGMENT_NAME
              , k.NAME
              , i.CONTACTS, i.PER_DELTA_CONTACTS, i.PRICE, i.PER_DELTA_PRICE
              , i.COST_DISCOUNT, i.REAL_DISCOUNT, i.PER_DELTA_REAL_DISCOUNT
              , i.DISCOUNT, i.PER_DELTA_DISCOUNT, i.CHECKS_PER_CLIENT, i.PER_DELTA_CHECKS
              , i.ID_COMPANY
              , i.ID_ORGANIZATION
              , i.ID_CVM_CATEGORY
              , i.SALES_SHARE
              , i.SALES_SHARE_BASE
              , i.SALES_INDEX
              , SALES_INDEX_LOW  = exp(log(nullif(i.SALES_INDEX, 0)) - 1.959964e0 * i.SE)
              , SALES_INDEX_HIGH = exp(log(nullif(i.SALES_INDEX, 0)) + 1.959964e0 * i.SE)
              , i.SALES_MARK
              , i.SALES_EXTRA
        from    #ix as i
                inner join dbo.I_CVM_CATEGORY as k (nolock)
                    on  k.ID_CVM_CATEGORY = i.ID_CVM_CATEGORY
                left join #city as ct
                    on  ct.ID_CITY_REPORT = i.ID_CITY_REPORT;

        -- 31.10.2026: месяц — в витрины; Т3: прежние строки месяца (только свои сегменты!) удаляются здесь же, в транзакции
        -- месяца — витрины заняты секунды; запись — по именам колонок
        delete from dbo.I_TREND
        where   ID_COMPANY = @idc
            and YEAR_MONTH = @YM
            and SEGMENT in ('NEW TRENDS','RANDOM TRENDS','CHURN TRENDS',
                            'ACTIVE LFL TRENDS','HIGH-PRICE SEGMENT','LOW-PRICE SEGMENT',
                            'BIG CHECK','LOW CHECK');

        delete from dbo.I_TREND_PRICE_SEGMENT
        where   ID_COMPANY = @idc
            and YEAR_MONTH = @YM
            and SEGMENT in (1,2,3,12,13,14,15,16,1000,1001,1002);

        insert into dbo.I_TREND_PRICE_SEGMENT
              ( SEGMENT, SEGMENT_NAME, YEAR_MONTH, CLIENTS, SHARE_CLIENTS, BUDGET, COUNT_CHECK, AVG_CHECK, AVG_COST_SKU, AVG_SKU
              , LOYALTY_INDEX, REAL_SALE, SALE, UNIQUE_SKU, PRICE_INDEX, ID_COMPANY, ID_ORGANIZATION, BONUS_ACCRUAL )
        select  SEGMENT, SEGMENT_NAME, YEAR_MONTH, CLIENTS, SHARE_CLIENTS, BUDGET, COUNT_CHECK, AVG_CHECK, AVG_COST_SKU, AVG_SKU
              , LOYALTY_INDEX, REAL_SALE, SALE, UNIQUE_SKU, PRICE_INDEX, ID_COMPANY, ID_ORGANIZATION, BONUS_ACCRUAL
        from    #tps;

        insert into dbo.I_TREND
              ( ID_CITY_REPORT, CITY_REPORT, YEAR_MONTH, SEGMENT, ID_CATEGORY_5, I_CATEGORY_5
              , CONTACTS, PER_DELTA_CONTACTS, PRICE, PER_DELTA_PRICE
              , COST_DISCOUNT, REAL_DISCOUNT, PER_DELTA_REAL_DISCOUNT
              , DISCOUNT, PER_DELTA_DISCOUNT, CHECKS_PER_CLIENT, PER_DELTA_CHECKS
              , ID_COMPANY, ID_ORGANIZATION, ID_CATEGORY_5_ext
              , ID_CVM_CATEGORY, SALES_SHARE, SALES_SHARE_BASE, SALES_INDEX, SALES_INDEX_LOW, SALES_INDEX_HIGH
              , SALES_MARK, SALES_EXTRA )
        select  ID_CITY_REPORT, CITY_REPORT, YEAR_MONTH, SEGMENT, ID_CATEGORY_5, I_CATEGORY_5
              , CONTACTS, PER_DELTA_CONTACTS, PRICE, PER_DELTA_PRICE
              , COST_DISCOUNT, REAL_DISCOUNT, PER_DELTA_REAL_DISCOUNT
              , DISCOUNT, PER_DELTA_DISCOUNT, CHECKS_PER_CLIENT, PER_DELTA_CHECKS
              , ID_COMPANY, ID_ORGANIZATION, ID_CATEGORY_5_ext
              , ID_CVM_CATEGORY, SALES_SHARE, SALES_SHARE_BASE, SALES_INDEX, SALES_INDEX_LOW, SALES_INDEX_HIGH
              , SALES_MARK, SALES_EXTRA
        from    #tr;

    COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        THROW;   -- месяц падает целиком; уже записанные месяцы не страдают
    END CATCH;

        set @YM        = (select YEAR_MONTH from #ym where RN = (select RN from #ym where YEAR_MONTH = @YM) + 1);
        set @YM_before = (select YEAR_MONTH from #ym where RN = (select RN from #ym where YEAR_MONTH = @YM) - 1);
        set @act_date  = (select LAST_DAY_IN_MONTH from #ym where YEAR_MONTH = @YM);

    END;   -- месячный цикл

    drop table if exists #ym;

    set @c = @c + 1;
    set @idc = (select ID_COMPANY from #c where RN = @c);

END;   -- цикл по компаниям
GO

SET NOEXEC OFF;
GO

-- ============================== 3. ИТОГ ==============================
IF ISNULL(OBJECT_DEFINITION(OBJECT_ID('dbo.trends')), N'') NOT LIKE N'%версия Т3 — категории CVM, над-индекс по доле в продажах (CVM_TRENDS_V2_20261101.sql)%'
   OR ISNULL(OBJECT_DEFINITION(OBJECT_ID('dbo.trends')), N'') NOT LIKE N'%сколько пересчитывать — по последнему месяцу в I_TREND (01.11.2026)%'
    RAISERROR(N'СТОП: dbo.trends не обновлена (нет версии Т3 с пересчётом по последнему месяцу в таблице).', 16, 1);
ELSE IF COL_LENGTH('dbo.I_TREND', 'ID_CVM_CATEGORY') IS NULL OR COL_LENGTH('dbo.I_TREND', 'SALES_SHARE') IS NULL
     OR COL_LENGTH('dbo.I_TREND', 'SALES_SHARE_BASE') IS NULL OR COL_LENGTH('dbo.I_TREND', 'SALES_INDEX') IS NULL
     OR COL_LENGTH('dbo.I_TREND', 'SALES_INDEX_LOW') IS NULL OR COL_LENGTH('dbo.I_TREND', 'SALES_INDEX_HIGH') IS NULL
     OR COL_LENGTH('dbo.I_TREND', 'SALES_MARK') IS NULL OR COL_LENGTH('dbo.I_TREND', 'SALES_EXTRA') IS NULL
    RAISERROR(N'СТОП: в I_TREND нет новых колонок Т3.', 16, 1);
ELSE
    SELECT PROC_NAME = N'dbo.trends', o.modify_date, STATUS = N'версия Т3: категории CVM, над-индекс по доле в продажах'
    FROM sys.objects AS o (nolock)
    WHERE o.object_id = OBJECT_ID('dbo.trends');
GO
