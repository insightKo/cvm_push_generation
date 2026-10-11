-- ============================================================================================
-- CVM_TRENDS_REBUILD — тренды (I_TREND, I_TREND_PRICE_SEGMENT): знаменатель доли покупателей — по своему сегменту,
-- ночной расчёт — от последнего месяца витрины, пересчёт истории 202401–202609 по вечерам после пересчёта миссий.
-- Запустить ОДИН РАЗ целиком в SSMS (F5) — после CVM_MISSIONS_REBUILD.sql: проход трендов ждёт конца пересчёта миссий
-- по его таблице отметки dbo.CVM_MISSIONS_REBUILD (поставлен раньше миссий — считает, а с установкой миссий ждёт их конца).
--
-- Почему:
--   1. Знаменатель доли покупателей (CONTACTS). В статусных (NEW, ACTIVE, ACTIVE LFL, RANDOM), ценовых (HIGH, MIDDLE, LOW)
--      и чековых (BIG, AVG, LOW CHECK) группах покупатели категории в сегменте делились на покупателей ВСЕХ групп города,
--      а не своей. Поэтому PER_DELTA_CONTACTS (сегмент против базы сравнения — ACTIVE, MIDDLE, AVG CHECK) = отношение
--      размеров сегмента и базы × отношение их долей покупателей категории − 1: размер сегмента смешан с выбором категорий.
--      Замер 11.10.2026 (I_TREND / I_TREND_PRICE_SEGMENT, ID_COMPANY = 1): медиана PER_DELTA_CONTACTS (все орг. / офлайн)
--      против отношения размеров сегмент / база − 1 (офлайн):
--        202609  NEW          −0,77 / −0,93   против −0,89
--                RANDOM       −0,76 / −0,89   против −0,74
--                ACTIVE LFL   +0,97 / +1,61   против +0,63
--                BIG CHECK    −0,74 / −0,74   против −0,78
--                LOW CHECK    −0,69 / −0,67   против −0,18
--                HIGH-PRICE   −0,71 / −0,76   против −0,52
--                LOW-PRICE    −0,36 / −0,28   против +0,06
--                CHURN        −0,53 / −0,61   (знаменатель по группе — верный)
--        202512  BIG CHECK    −0,70 / −0,64   против −0,76
--                LOW CHECK    −0,70 / −0,67   против −0,36
--      Теперь CONTACTS = покупатели категории в сегменте / покупатели сегмента (город × организация), и PER_DELTA_CONTACTS =
--      отношение долей − 1. CHURN не меняется.
--   2. Ночной расчёт (шаг 43, вызов без параметров) считал только окно перезаливки (месяц «сегодня − 10 дней»), а ночная
--      очистка clear_data_update удаляет I_TREND и I_TREND_PRICE_SEGMENT с месяца начала перезаливки — в первую пятницу
--      месяца с 1-го числа три месяца назад: 06.11.2026 пропали бы август и сентябрь. Решение Елены 11.10.2026: как по
--      остальным витринам — со следующего месяца после последнего месяца в витрине, но не позже окна перезаливки.
--   3. История 202401–202609 посчитана со старым знаменателем (сентябрь 2026 — тоже) — её пересчитывают по вечерам:
--      месяц ~1 ч, 33 месяца — больше суток; одним запуском это пересеклось бы с ночными джобами MCI.
-- Что делает:
--   1. dbo.trends (текст — из базы на 11.10.2026; остальной расчёт не меняется):
--      — знаменатель доли покупателей — по своему сегменту (статусные, ценовые, чековые группы);
--      — ночной вызов без параметров (шаг 43) считает со следующего месяца после последнего месяца своих сегментов
--        в I_TREND, но не позже окна перезаливки; I_TREND и I_TREND_PRICE_SEGMENT удаляются и пишутся с этого же месяца.
--        Пустая витрина — только окно перезаливки. Ночь первой пятницы месяца считает три месяца (~3 ч вместо ~1 ч);
--      — необязательный параметр @YM_STOP (граница окна, не включается) — для пересчёта истории по месяцам;
--      — две правки производительности (решение Елены 11.10.2026, результат расчёта не меняется): недели покупок (#wk_all)
--        читаются из чеков не дальше последнего месяца вызова; предрасчёты перед месячным циклом — только если есть месяц для расчёта;
--      — чтение I_ORGANIZATION и I_TREND в процедуре — с (nolock) (чек-лист п. 13, Елена 11.10.2026); результат не меняется.
--   2. Служебная таблица dbo.CVM_TRENDS_REBUILD — с какого месяца продолжать и сколько месяцев за вечер (PORTION).
--   3. Процедура dbo.cvm_trends_rebuild_step — вечерний проход, как у миссий: месяцы по одному, не больше PORTION за проход;
--      отметка сдвигается после каждого посчитанного месяца (упал месяц — нет). Месяц не начинается, если идёт ночной
--      джоб MCI или CVM REBUILD ONCE; если не закончен пересчёт миссий (в dbo.CVM_MISSIONS_REBUILD DONE = 0) или идёт джоб
--      «CVM MISSIONS REBUILD» — оба пересчёта тяжёлые и идут друг за другом; если сейчас не вечер (17:00–00:30) или месяц
--      не успеет до 00:30 (оценка — время прошлого месяца этого прохода, для первого — 1 ч). Второй экземпляр прохода
--      не стартует. Дошла до конца — выключает свой джоб.
--   4. Джоб «CVM TRENDS REBUILD» — каждый день в 17:00.
--   5. Разовый джоб «CVM REBUILD ONCE» выключается, если скрипт миссий его ещё не выключил: его шаг 1 — trends с 202401
--      без границы. Не удаляется.
--   6. Первый проход стартует сразу, если сейчас вечер, ночной джоб MCI не идёт и пересчёт миссий закончен.
-- Сроки: месяц ~1 ч (журнал 10.10: шаг 43 — 59 мин), 33 месяца 202401–202609, PORTION = 5 (~5 ч за вечер) — ~7 вечеров.
--   Начало — в 17:00 после окончания пересчёта миссий (у миссий тоже ~7 вечеров).
--   Сменить: UPDATE dbo.CVM_TRENDS_REBUILD SET PORTION = 3;  Ход и ошибки: SELECT * FROM dbo.CVM_TRENDS_REBUILD;
--   Внеплановый проход: EXEC msdb.dbo.sp_start_job N'CVM TRENDS REBUILD';  (не EXEC процедуры в окне SSMS:
--   прерванный по Stop, он держит замок прохода, пока окно открыто, и вечерние проходы пропускаются)
--   Ночной джоб MCI шёл после 17:00 или миссии ещё считаются — вечер пропускается (LAST_NOTE), следующий проход — завтра
--   в 17:00 или вручную.
-- На портале после пересчёта: над-индекс категории = 1 + PER_DELTA_CONTACTS — во сколько раз доля покупателей категории
--   в сегменте выше, чем в базе сравнения; маркеры — от ×1,10, уникальные — от ×1,50. Пока пересчёт идёт, ещё
--   не пересчитанные месяцы — со старым знаменателем (в значениях — размер сегмента), пересчитанные и ночные — с новым.
-- Пока месяц пересчитывается (~1 ч), его трендов нет в отчёте; упал месяц — его нет до следующего успешного прохода.
-- ============================================================================================

USE mci_model
GO
SET NOCOUNT ON;

-- ============================== 0. ПРОВЕРКИ ==============================
DECLARE @abort bit = 0;
DECLARE @def nvarchar(max) = OBJECT_DEFINITION(OBJECT_ID('dbo.trends'));
IF OBJECT_ID('dbo.CVM_TRENDS_REBUILD') IS NOT NULL
BEGIN
    RAISERROR(N'СТОП: пересчёт трендов уже заведён (таблица dbo.CVM_TRENDS_REBUILD есть) — ничего не меняю. Если джоба «CVM TRENDS REBUILD» нет или он неполный (без шага или расписания) — выдели и выполни разделы 3–6: процедура прохода (CREATE OR ALTER) и выключение разового джоба повторяются без вреда, раздел 4 сам удаляет остаток джоба.', 16, 1);
    SET @abort = 1;
END
-- isnull: нет процедуры / нет прав на определение → NULL, и без isnull проверка молча проходила бы
ELSE IF ISNULL(@def, N'') NOT LIKE N'%dbo.trends — ext-пересборка%'
BEGIN
    RAISERROR(N'СТОП: dbo.trends в базе не ext-версия — текст процедуры изменился, ничего не меняю.', 16, 1);
    SET @abort = 1;
END
-- раздел 1 собран из текста, снятого из базы 11.10.2026 (последнее изменение процедуры 07.10.2026 23:57; SHA-256 ниже):
-- если текст с тех пор меняли, ALTER затёр бы эти правки
ELSE IF HASHBYTES('SHA2_256', @def) <> 0x5E506B37BDDF279D2A0CE59EE8500BC72D15ABFCDE458942EFF5D2A1B6BF2FAD
BEGIN
    RAISERROR(N'СТОП: текст dbo.trends в базе изменился после 11.10.2026 — новая версия собрана из прежнего текста и затёрла бы правки, ничего не меняю. Если это повтор после частичного прогона и процедуру уже обновил этот скрипт (у dbo.trends есть параметр @YM_STOP, таблицы CVM_TRENDS_REBUILD нет) — выдели и выполни разделы 2–7.', 16, 1);
    SET @abort = 1;
END
-- выключение (раздел 5) уже идущий джоб не останавливает: его шаг 1 (trends с 202401 без границы) удалил бы
-- и пересчитал все месяцы с 202401 параллельно порциям
IF EXISTS (SELECT 1
           FROM msdb.dbo.sysjobactivity AS a (nolock)
                JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id
           WHERE j.name = N'CVM REBUILD ONCE'
             AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL
             AND a.session_id = (SELECT max(session_id) FROM msdb.dbo.syssessions (nolock)))
BEGIN
    RAISERROR(N'СТОП: сейчас выполняется разовый джоб «CVM REBUILD ONCE» — выключение его не остановит, ничего не меняю.', 16, 1);
    SET @abort = 1;
END
-- ALTER (раздел 1) не ждёт идущего расчёта: если trends сейчас выполняется (ночной шаг 43, ~1 ч на месяц), идущий вызов
-- при компиляции очередного оператора (операторы с временными таблицами компилируются по ходу) может упасть
-- с ошибкой 2801 «определение объекта изменилось после компиляции» — шаг 43 ночного джоба упадёт
IF EXISTS (SELECT 1
           FROM sys.dm_exec_requests AS r
                CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
           WHERE t.dbid = DB_ID() AND t.objectid = OBJECT_ID('dbo.trends'))
BEGIN
    RAISERROR(N'СТОП: сейчас выполняется dbo.trends (ночной шаг 43 или другой запуск) — запустить скрипт после окончания расчёта, ничего не меняю.', 16, 1);
    SET @abort = 1;
END
IF @abort = 1 SET NOEXEC ON;

-- ============================== 1. ПРОЦЕДУРА ТРЕНДОВ: знаменатель по сегменту, ночной режим, граница окна ==============================
-- (заголовок раздела — до GO: в текст процедуры в базе попадает только её собственный заголовок, как в исходнике;
-- ANSI_NULLS и QUOTED_IDENTIFIER — как при установке CVM_REPLACE_PROCEDURES.sql)
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- ============================== 2. dbo.trends — ext-пересборка ==============================
-- Источник: dixy-dashboard/данные/sql_i_trend_rebuild_ext.sql (согласовано 20.07, чековые 24.08); правки 11.10.2026
--   (CVM_TRENDS_REBUILD.sql): знаменатель доли покупателей — по своему сегменту, ночной режим, граница @YM_STOP.
-- @YM_START: NULL (джоб, шаг 43) = со следующего месяца после последнего месяца своих сегментов в I_TREND, но не позже
--   месяца окна перезаливки чеков (дни 1-10 → прошлый месяц); 202401 = полная пересборка.
CREATE OR ALTER PROCEDURE [dbo].[trends] @YM_START int = NULL
    , @YM_STOP int = NULL   -- граница окна, месяц НЕ включается; NULL — до текущего месяца
as
set nocount on;
set quoted_identifier on;

DECLARE @nightly bit = IIF(@YM_START IS NULL, 1, 0);   -- ночной режим: вызов без @YM_START (шаг 43)
DECLARE @last_month int;

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

    -- ── 1. Словарь категорий: внутренний id → ext-код Дикси + имя
    -- ПРЕРЕКВИЗИТ: сначала выполнить sql/справочник_ext_названия.sql — он
    -- строит dbo.I_CATEGORY_EXT (ключ ID_CATEGORY_5_ext — ТЕКСТ, ровно как
    -- в I_PRODUCT, без конвертаций; название I_CATEGORY_5_ext, SOURCE =
    -- AUTO/MANUAL/EMPTY). Здесь имя берём ИЗ НЕГО; фолбэк на старый
    -- справочник — для категорий без ext и кодов без названия (EMPTY).
    -- Ключ CAT_KEY (nvarchar(50)): ext-текст, если он есть; иначе метка
    -- '-<внутренний id>' (минус первым символом — с ext не пересекается).
    drop table if exists #cat_dict;
    -- NB: ext-код НЕ конвертируем в число (запрещено пользователем) —
    -- сравниваем текстом, ровно как он лежит в I_PRODUCT (без trim,
    -- зеркально справочнику). Пустые/NULL значения → ветка «без ext».
    -- Если у внутренней категории несколько ext-кодов — берём max(ext),
    -- как в исходной версии. (Выбор ветки по наибольшему CNT_SKU обсуждён,
    -- но НЕ согласован — правка отложена до решения пользователя.)
    ;with p as
    (
        select  ID_CATEGORY_5
              , ID_CATEGORY_5_ext = max(cast(ID_CATEGORY_5_ext as nvarchar(50)))
        from    dbo.I_PRODUCT (nolock)
        where   ID_COMPANY = @idc
            and ID_CATEGORY_5_ext is not null
        group by ID_CATEGORY_5
    )
    select  c5.ID_CATEGORY_5
          , ID_CATEGORY_5_ext = p.ID_CATEGORY_5_ext
          , CAT_KEY      = coalesce(p.ID_CATEGORY_5_ext, N'-' + cast(c5.ID_CATEGORY_5 as nvarchar(20)))
          , I_CATEGORY_5 = coalesce(e.I_CATEGORY_5_ext, c5.I_CATEGORY_5)
    into    #cat_dict
    from    dbo.I_CATEGORY_5 as c5 (nolock)
            left join p
                on p.ID_CATEGORY_5 = c5.ID_CATEGORY_5
            left join dbo.I_CATEGORY_EXT as e (nolock)
                on  e.ID_CATEGORY_5_ext = p.ID_CATEGORY_5_ext
                and e.ID_COMPANY = @idc
    where   c5.ID_COMPANY = @idc;

    -- Агрегат по CAT_KEY: одно имя на группу (внутренние id наружу не выносим)
    drop table if exists #cat_key;
    select  CAT_KEY
          , ID_CATEGORY_5_ext = max(ID_CATEGORY_5_ext)
          , I_CATEGORY_5      = max(I_CATEGORY_5)
    into    #cat_key
    from    #cat_dict
    group by CAT_KEY;

    -- ── Ночной режим (11.10.2026) — как for_i_report: со следующего месяца после последнего месяца своих сегментов
    -- в I_TREND, но не позже окна перезаливки. Так возвращаются и месяцы, удалённые ночной очисткой clear_data_update
    -- (в первую пятницу месяца — с 1-го числа три месяца назад). Пустая витрина — только окно перезаливки (историю
    -- считает вечерний пересчёт). Удаление ниже (I_TREND и I_TREND_PRICE_SEGMENT) и расчёт — с этого же месяца.
    IF @nightly = 1
    BEGIN
        SET @last_month = (SELECT max(YEAR_MONTH) FROM dbo.I_TREND (nolock)
                           WHERE ID_COMPANY = @idc
                             AND YEAR_MONTH < @YM_START
                             AND SEGMENT in ('NEW TRENDS','RANDOM TRENDS','CHURN TRENDS',
                                             'ACTIVE LFL TRENDS','HIGH-PRICE SEGMENT','LOW-PRICE SEGMENT',
                                             'BIG CHECK','LOW CHECK'));
        IF @last_month IS NOT NULL
            SET @YM_START = (SELECT min(YEAR_MONTH) FROM dbo.I_CALENDAR (nolock) WHERE YEAR_MONTH > @last_month);
    END

    -- ── 2. Чистим диапазон пересборки (только свои сегменты!)
    delete from dbo.I_TREND
    where   ID_COMPANY = @idc
        and YEAR_MONTH >= @YM_START
        and YEAR_MONTH < isnull(@YM_STOP, 2147483647)   -- @YM_STOP не включается
        and SEGMENT in ('NEW TRENDS','RANDOM TRENDS','CHURN TRENDS',
                        'ACTIVE LFL TRENDS','HIGH-PRICE SEGMENT','LOW-PRICE SEGMENT',
                        'BIG CHECK','LOW CHECK');

    delete from dbo.I_TREND_PRICE_SEGMENT
    where   ID_COMPANY = @idc
        and YEAR_MONTH >= @YM_START
        and YEAR_MONTH < isnull(@YM_STOP, 2147483647)
        and SEGMENT in (1,2,3,12,13,14,15,16,1000,1001,1002);

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

    -- M1: верхняя граница цикла — один раз
    declare @YM_MAX int = (select max(YEAR_MONTH) from dbo.I_CHECKHEADER (nolock) where ID_COMPANY = @idc);

    -- 11.10.2026: предрасчёты M2–M3 — только если месячный цикл ниже сделает хотя бы одну итерацию (условие то же, что
    -- у while): в ночном вызове 11–31 числа и 1-го окно = @YM_MAX, цикл пуст, а чеки читались без потребителя.
    -- #wk_all, #buyers, #chk_agg используются только внутри цикла.
    if @YM < @YM_MAX and (@YM_STOP is null or @YM < @YM_STOP)
    begin

    -- M2: недели покупок для RANDOM — один скан всего диапазона.
    -- Эквивалентность помесячному окну: группа (клиент, орг, неделя)
    -- попадала в окно, если ХОТЯ БЫ ОДНА покупка лежит в
    -- [начало окна; @act_date]. Даты недели укладываются в <=7 дней,
    -- окно длиной 56 недель — неделя не может накрыть обе границы,
    -- поэтому «MAX_DATA >= начала AND MIN_DATA <= конца» ⇔ «есть
    -- покупка внутри окна» (обе граничные даты — реальные покупки).
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
        and DATA >= dateadd(day, +1, dateadd(week, -56,
                (select LAST_DAY_IN_MONTH from #ym where YEAR_MONTH = @YM_START)))
        -- 11.10.2026: верхняя граница — последний день последнего месяца этого вызова (без неё пересчёт по одному месяцу
        -- читал чеки до сегодня). Срез по месяцам тот же: неделя целиком за границей и так не проходила MIN_DATA <= @act_date,
        -- а у недели, пересекающей границу, MIN_DATA сохраняется и MAX_DATA остаётся >= начала окна (7 дней против 56 недель)
        and DATA <= (select max(LAST_DAY_IN_MONTH) from #ym
                     where YEAR_MONTH < @YM_MAX and YEAR_MONTH < isnull(@YM_STOP, 2147483647))
    group by ID_CONTACT, ID_ORGANIZATION, ID_COMPANY, WEEKNUMBER;

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

    -- Агрегат I_CHECK: клиент × категория × город × орг, LFL-периметр месяца
    drop table if exists #chk_agg;
    select  a.YEAR_MONTH
          , a.YEAR
          , a.ID_CONTACT
          , ID_CITY_REPORT
          , CITY_REPORT
          , CAT_KEY = coalesce(k.CAT_KEY, N'-' + cast(a.ID_CATEGORY_5 as nvarchar(20)))
          , QUANTITY = sum(a.QUANTITY)
          , COST_DISCOUNT = sum(a.COST_DISCOUNT)
          , REAL_DISCOUNT = sum(a.REAL_DISCOUNT)
          , DISCOUNT = sum(a.DISCOUNT)
          , CHECKS = count(distinct ID_CHECK)
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
            left join #cat_dict as k
                on  k.ID_CATEGORY_5 = a.ID_CATEGORY_5
    where   a.YEAR_MONTH = @YM_before
        and a.ID_COMPANY = @idc
        and a.ID_CONTACT != 0
    group by a.YEAR_MONTH
          , a.ID_CONTACT
          , ID_CITY_REPORT
          , coalesce(k.CAT_KEY, N'-' + cast(a.ID_CATEGORY_5 as nvarchar(20)))
          , a.YEAR
          , CITY_REPORT
          , a.ID_COMPANY
          , a.ID_ORGANIZATION;

    end;   -- предрасчёты M2–M3

    -- Текущий (неполный) месяц не считаем — условие «строго меньше max»
    while @YM < @YM_MAX and (@YM_STOP is null or @YM < @YM_STOP)   -- и не дальше @YM_STOP (не включается)
    BEGIN
    BEGIN TRY
    BEGIN TRANSACTION;

        -- M3: carry-over храним ровно два месяца
        delete from #buyers  where YEAR_MONTH < @YM_before;
        delete from #chk_agg where YEAR_MONTH < @YM_before;

        -- ЧЕКОВЫЕ СЕГМЕНТЫ клиентов (FIX K): BIG/AVG/LOW по среднему чеку
        -- клиента за @YM. Пороги: офлайн BIG >= 1500 ₽ / LOW <= 500 ₽,
        -- e-com BIG >= 3500 ₽ / LOW <= 1500 ₽ (границы нестрогие,
        -- подтверждено 2026-08-24). Считается в начале итерации: из этого
        -- же скана получаем «покупателей месяца» для #buyers.
        drop table if exists #ckclients;
        select  a.ID_CONTACT
              , SEGMENT = case when sum(COST_DISCOUNT) / count(a.ID_CHECK) >= IIF(a.ID_ORGANIZATION = 1, 1500, 3500) then 1000
                               when sum(COST_DISCOUNT) / count(a.ID_CHECK) <= IIF(a.ID_ORGANIZATION = 1, 500, 1500)  then 1002
                               else 1001
                          end   -- 1000 - BIG, 1001 - AVG, 1002 - LOW
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
        into    #ckclients
        from    dbo.I_CHECKHEADER as a (nolock)
        where   a.YEAR_MONTH = @YM
            and a.ID_COMPANY = @idc
            and a.ID_CONTACT != 0
        group by a.ID_CONTACT, a.ID_ORGANIZATION, a.ID_COMPANY;

        -- M3: покупатели @YM — проекция #ckclients (чек есть у каждого)
        insert into #buyers (ID_CONTACT, ID_ORGANIZATION, ID_COMPANY, YEAR_MONTH)
        select  ID_CONTACT, ID_ORGANIZATION, ID_COMPANY, @YM
        from    #ckclients;

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

        -- ACTIVE-LFL (14): покупал в @YM и в том же месяце год назад
        ;with b as
        (
            select  a.ID_CONTACT
                  , a.ID_ORGANIZATION
                  , a.ID_COMPANY
            from    dbo.I_CHECKHEADER (nolock) as a
            where   a.ID_COMPANY = @idc and YEAR_MONTH = (@YM - 100)
            group by a.ID_CONTACT, a.ID_ORGANIZATION, a.ID_COMPANY
        )
        insert into #all_segm
        select  a.ID_CONTACT
              , a.ID_ORGANIZATION
              , a.ID_COMPANY
              , SEGMENT = 14
        from    #buyers a                                   -- M3: покупатели @YM
                inner join b
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

        -- ============================================================
        -- I_TREND_PRICE_SEGMENT: статусные сегменты
        -- ============================================================

        -- CHURN: метрики по последнему активному месяцу (@YM_before),
        -- записываются с YEAR_MONTH = @YM (семантика сохранена)
        ;with t as
        (
            select  id_organization, ALL_CLIENTS = count(distinct a.ID_CONTACT)
            from    dbo.I_CHECKHEADER a (nolock)
            where   ID_COMPANY = @idc and YEAR_MONTH = @YM_before
            group by id_organization
        )
        insert into dbo.I_TREND_PRICE_SEGMENT
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
              , BONUS_PAY = sum(BONUS_PAY)
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
        insert into dbo.I_TREND_PRICE_SEGMENT
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
              , BONUS_PAY = sum(BONUS_PAY)
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

        -- ============================================================
        -- I_TREND: статусные тренды (чеки @YM, LFL-магазины, ext-категории)
        -- ============================================================

        -- M3: единственный скан I_CHECK за месяц — общий агрегат
        -- (клиент × категория × город × орг, LFL-периметр @YM) для
        -- статусных, ценовых и чековых трендов + CHURN следующей итерации.
        -- FIX A: ext-ключ; фолбэк -id, если категории нет в словаре,
        -- чтобы не терять строки на этом шаге (как в исходнике, такие
        -- строки отсеются на вставке — контроль в отчёте №1)
        insert into #chk_agg
              ( YEAR_MONTH, YEAR, ID_CONTACT, ID_CITY_REPORT, CITY_REPORT
              , CAT_KEY, QUANTITY, COST_DISCOUNT, REAL_DISCOUNT, DISCOUNT
              , CHECKS, ID_COMPANY, ID_ORGANIZATION )
        select  a.YEAR_MONTH
              , a.YEAR
              , a.ID_CONTACT
              , ID_CITY_REPORT
              , CITY_REPORT
              , CAT_KEY = coalesce(k.CAT_KEY, N'-' + cast(a.ID_CATEGORY_5 as nvarchar(20)))
              , QUANTITY = sum(a.QUANTITY)
              , COST_DISCOUNT = sum(a.COST_DISCOUNT)
              , REAL_DISCOUNT = sum(a.REAL_DISCOUNT)
              , DISCOUNT = sum(a.DISCOUNT)
              , CHECKS = count(distinct ID_CHECK)
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
                left join #cat_dict as k
                    on  k.ID_CATEGORY_5 = a.ID_CATEGORY_5
        where   a.YEAR_MONTH in (@YM)
            and a.ID_COMPANY = @idc
            and a.ID_CONTACT != 0
        group by a.YEAR_MONTH
              , a.ID_CONTACT
              , ID_CITY_REPORT
              , coalesce(k.CAT_KEY, N'-' + cast(a.ID_CATEGORY_5 as nvarchar(20)))
              , a.YEAR
              , CITY_REPORT
              , a.ID_COMPANY
              , a.ID_ORGANIZATION;

        -- Статусный разрез: агрегат + сегмент клиента (1:1 по клиент×орг)
        drop table if exists #cat_segm;
        select  a.YEAR_MONTH
              , a.YEAR
              , a.ID_CONTACT
              , a.ID_CITY_REPORT
              , a.CITY_REPORT
              , a.CAT_KEY
              , b.SEGMENT
              , a.QUANTITY
              , a.COST_DISCOUNT
              , a.REAL_DISCOUNT
              , a.DISCOUNT
              , a.CHECKS
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
        into    #cat_segm
        from    #chk_agg as a
                inner join #all_segm as b
                    on  a.ID_CONTACT = b.ID_CONTACT
                    and a.ID_COMPANY = b.ID_COMPANY
                    and a.ID_ORGANIZATION = b.ID_ORGANIZATION
        where   a.YEAR_MONTH = @YM;

        drop table if exists #lfl_trend;
        ;with cont as
        (
            select  ALL_CONTACTS = count(distinct ID_CONTACT)
                  , YEAR_MONTH
                  , ID_COMPANY
                  , ID_CITY_REPORT
                  , ID_ORGANIZATION
                  , SEGMENT   -- 11.10.2026: знаменатель — покупатели своего сегмента
            from    #cat_segm as t
            where   SEGMENT in (12, 14, 13, 15)
            group by YEAR_MONTH, ID_COMPANY, ID_CITY_REPORT, ID_ORGANIZATION, SEGMENT
        )
        select  YEAR
              , t.YEAR_MONTH
              , CITY_REPORT
              , t.SEGMENT
              , t.ID_CITY_REPORT
              , t.CAT_KEY
              , CONTACTS = (count(ID_CONTACT) * 1.0) / (ALL_CONTACTS * 1.00)
              , REAL_DISCOUNT = sum(REAL_DISCOUNT) / nullif((sum(REAL_DISCOUNT) + sum(COST_DISCOUNT)), 0)
              , DISCOUNT = sum(DISCOUNT) / nullif((sum(REAL_DISCOUNT) + sum(COST_DISCOUNT)), 0)
              , CHECKS = (sum(CHECKS) * 1.00) / (count(ID_CONTACT) * 1.00)
              , PRICE = sum(COST_DISCOUNT) / nullif(sum(QUANTITY), 0)
              , COST_DISCOUNT = sum(COST_DISCOUNT) / count(ID_CONTACT)
              , t.ID_COMPANY
              , t.ID_ORGANIZATION
        into    #lfl_trend
        from    #cat_segm as t
                inner join cont as a
                    on  a.ID_CITY_REPORT = t.ID_CITY_REPORT
                    and a.YEAR_MONTH = t.YEAR_MONTH
                    and a.ID_COMPANY = t.ID_COMPANY
                    and t.ID_ORGANIZATION = a.ID_ORGANIZATION
                    and a.SEGMENT = t.SEGMENT
        group by t.YEAR_MONTH
              , t.ID_CITY_REPORT
              , t.CAT_KEY
              , YEAR
              , CITY_REPORT
              , ALL_CONTACTS
              , t.ID_COMPANY
              , t.SEGMENT
              , t.ID_ORGANIZATION;

        -- NEW TRENDS (база сравнения — ACTIVE 13)
        insert into dbo.I_TREND
              ( ID_CITY_REPORT, CITY_REPORT, YEAR_MONTH, SEGMENT
              , I_CATEGORY_5
              , CONTACTS, PER_DELTA_CONTACTS, PRICE, PER_DELTA_PRICE
              , COST_DISCOUNT, REAL_DISCOUNT, PER_DELTA_REAL_DISCOUNT
              , DISCOUNT, PER_DELTA_DISCOUNT, CHECKS_PER_CLIENT, PER_DELTA_CHECKS
              , ID_COMPANY, ID_ORGANIZATION, ID_CATEGORY_5_ext )
        select  a.ID_CITY_REPORT
              , a.CITY_REPORT
              , YEAR_MONTH = @YM
              , SEGMENT = 'NEW TRENDS'
              , k.I_CATEGORY_5
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
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
              , k.ID_CATEGORY_5_ext
        from    #lfl_trend as a
                left join #lfl_trend as b                          -- FIX G: база в ON
                    on  a.CAT_KEY = b.CAT_KEY
                    and a.ID_CITY_REPORT = b.ID_CITY_REPORT
                    and a.ID_COMPANY = b.ID_COMPANY
                    and a.ID_ORGANIZATION = b.ID_ORGANIZATION
                    and b.SEGMENT = 13
                inner join #cat_key as k
                    on  k.CAT_KEY = a.CAT_KEY
        where   a.SEGMENT = 12;

        -- RANDOM TRENDS
        insert into dbo.I_TREND
              ( ID_CITY_REPORT, CITY_REPORT, YEAR_MONTH, SEGMENT
              , I_CATEGORY_5
              , CONTACTS, PER_DELTA_CONTACTS, PRICE, PER_DELTA_PRICE
              , COST_DISCOUNT, REAL_DISCOUNT, PER_DELTA_REAL_DISCOUNT
              , DISCOUNT, PER_DELTA_DISCOUNT, CHECKS_PER_CLIENT, PER_DELTA_CHECKS
              , ID_COMPANY, ID_ORGANIZATION, ID_CATEGORY_5_ext )
        select  a.ID_CITY_REPORT
              , a.CITY_REPORT
              , YEAR_MONTH = @YM
              , SEGMENT = 'RANDOM TRENDS'
              , k.I_CATEGORY_5
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
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
              , k.ID_CATEGORY_5_ext
        from    #lfl_trend as a
                left join #lfl_trend as b
                    on  a.CAT_KEY = b.CAT_KEY
                    and a.ID_CITY_REPORT = b.ID_CITY_REPORT
                    and a.ID_COMPANY = b.ID_COMPANY
                    and a.ID_ORGANIZATION = b.ID_ORGANIZATION
                    and b.SEGMENT = 13
                inner join #cat_key as k
                    on  k.CAT_KEY = a.CAT_KEY
        where   a.SEGMENT = 15;

        -- ACTIVE LFL TRENDS
        insert into dbo.I_TREND
              ( ID_CITY_REPORT, CITY_REPORT, YEAR_MONTH, SEGMENT
              , I_CATEGORY_5
              , CONTACTS, PER_DELTA_CONTACTS, PRICE, PER_DELTA_PRICE
              , COST_DISCOUNT, REAL_DISCOUNT, PER_DELTA_REAL_DISCOUNT
              , DISCOUNT, PER_DELTA_DISCOUNT, CHECKS_PER_CLIENT, PER_DELTA_CHECKS
              , ID_COMPANY, ID_ORGANIZATION, ID_CATEGORY_5_ext )
        select  a.ID_CITY_REPORT
              , a.CITY_REPORT
              , YEAR_MONTH = @YM
              , SEGMENT = 'ACTIVE LFL TRENDS'
              , k.I_CATEGORY_5
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
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
              , k.ID_CATEGORY_5_ext
        from    #lfl_trend as a
                left join #lfl_trend as b
                    on  a.CAT_KEY = b.CAT_KEY
                    and a.ID_CITY_REPORT = b.ID_CITY_REPORT
                    and a.ID_COMPANY = b.ID_COMPANY
                    and a.ID_ORGANIZATION = b.ID_ORGANIZATION
                    and b.SEGMENT = 13
                inner join #cat_key as k
                    on  k.CAT_KEY = a.CAT_KEY
        where   a.SEGMENT = 14;

        -- ============================================================
        -- CHURN TRENDS (FIX E): чеки @YM_before; отток (16) vs
        -- «покупатели @YM_before, оставшиеся активными в @YM» (база 1)
        -- ============================================================
        -- M3: чеки @YM_before в LFL-периметре @YM_before уже лежат в
        -- carry-over #chk_agg (построены прошлой итерацией / затравкой) —
        -- повторный скан I_CHECK не нужен
        drop table if exists #cat_segm_churn;
        select  a.YEAR_MONTH
              , a.YEAR
              , a.ID_CONTACT
              , a.ID_CITY_REPORT
              , a.CITY_REPORT
              , a.CAT_KEY
              , GRP = iif(s.ID_CONTACT is not null, 16, 1)   -- 16 = отток, 1 = остались
              , a.QUANTITY
              , a.COST_DISCOUNT
              , a.REAL_DISCOUNT
              , a.DISCOUNT
              , a.CHECKS
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
        into    #cat_segm_churn
        from    #chk_agg as a
                left join #all_segm as s
                    on  a.ID_CONTACT = s.ID_CONTACT
                    and a.ID_COMPANY = s.ID_COMPANY
                    and a.ID_ORGANIZATION = s.ID_ORGANIZATION
                    and s.SEGMENT = 16
        where   a.YEAR_MONTH = @YM_before;

        drop table if exists #churn_trend;
        ;with cont as
        (
            select  ALL_CONTACTS = count(distinct ID_CONTACT)
                  , GRP
                  , ID_COMPANY
                  , ID_CITY_REPORT
                  , ID_ORGANIZATION
            from    #cat_segm_churn
            group by GRP, ID_COMPANY, ID_CITY_REPORT, ID_ORGANIZATION
        )
        select  YEAR
              , t.YEAR_MONTH
              , CITY_REPORT
              , t.GRP
              , t.ID_CITY_REPORT
              , t.CAT_KEY
              , CONTACTS = (count(ID_CONTACT) * 1.0) / (ALL_CONTACTS * 1.00)
              , REAL_DISCOUNT = sum(REAL_DISCOUNT) / nullif((sum(REAL_DISCOUNT) + sum(COST_DISCOUNT)), 0)
              , DISCOUNT = sum(DISCOUNT) / nullif((sum(REAL_DISCOUNT) + sum(COST_DISCOUNT)), 0)
              , CHECKS = (sum(CHECKS) * 1.00) / (count(ID_CONTACT) * 1.00)
              , PRICE = sum(COST_DISCOUNT) / nullif(sum(QUANTITY), 0)
              , COST_DISCOUNT = sum(COST_DISCOUNT) / count(ID_CONTACT)
              , t.ID_COMPANY
              , t.ID_ORGANIZATION
        into    #churn_trend
        from    #cat_segm_churn as t
                inner join cont as a
                    on  a.ID_CITY_REPORT = t.ID_CITY_REPORT
                    and a.GRP = t.GRP
                    and a.ID_COMPANY = t.ID_COMPANY
                    and a.ID_ORGANIZATION = t.ID_ORGANIZATION
        group by t.YEAR_MONTH
              , t.ID_CITY_REPORT
              , t.CAT_KEY
              , YEAR
              , CITY_REPORT
              , ALL_CONTACTS
              , t.ID_COMPANY
              , t.GRP
              , t.ID_ORGANIZATION;

        insert into dbo.I_TREND
              ( ID_CITY_REPORT, CITY_REPORT, YEAR_MONTH, SEGMENT
              , I_CATEGORY_5
              , CONTACTS, PER_DELTA_CONTACTS, PRICE, PER_DELTA_PRICE
              , COST_DISCOUNT, REAL_DISCOUNT, PER_DELTA_REAL_DISCOUNT
              , DISCOUNT, PER_DELTA_DISCOUNT, CHECKS_PER_CLIENT, PER_DELTA_CHECKS
              , ID_COMPANY, ID_ORGANIZATION, ID_CATEGORY_5_ext )
        select  a.ID_CITY_REPORT
              , a.CITY_REPORT
              , YEAR_MONTH = @YM       -- месяц фиксации оттока (чеки — @YM_before)
              , SEGMENT = 'CHURN TRENDS'
              , k.I_CATEGORY_5
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
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
              , k.ID_CATEGORY_5_ext
        from    #churn_trend as a
                left join #churn_trend as b
                    on  a.CAT_KEY = b.CAT_KEY
                    and a.ID_CITY_REPORT = b.ID_CITY_REPORT
                    and a.ID_COMPANY = b.ID_COMPANY
                    and a.ID_ORGANIZATION = b.ID_ORGANIZATION
                    and b.GRP = 1
                inner join #cat_key as k
                    on  k.CAT_KEY = a.CAT_KEY
        where   a.GRP = 16;

        -- ============================================================
        -- ЦЕНОВЫЕ СЕГМЕНТЫ (пороги сохранены; периметр теперь LFL — FIX H)
        -- ============================================================
        drop table if exists #psclients;
        ;with rev as
        (
            -- FIX I: только последняя загрузка месяца (в таблице есть дубли)
            select  ID_ORGANIZATION, MAX_REV = max(revision_date)
            from    dbo.I_CONT_ASSORT_M (nolock)
            where   YEAR_MONTH = @YM and ID_COMPANY = @idc
            group by ID_ORGANIZATION
        )
        select  b.ID_CONTACT
              , SEGMENT = case when avg(AVG_PRICE_INDEX) >= IIF(b.ID_ORGANIZATION = 1, 0.359, 0.405) then 1
                               when avg(AVG_PRICE_INDEX) <= IIF(b.ID_ORGANIZATION = 1, 0.25, 0.28) then 3
                               else 2
                          end   -- 1 - high, 3 - low, 2 - avg
              , b.ID_COMPANY
              , b.ID_ORGANIZATION
        into    #psclients
        from    dbo.I_CONT_ASSORT_M as b (nolock)
                inner join rev as r
                    on  r.ID_ORGANIZATION = b.ID_ORGANIZATION
                    and (b.revision_date = r.MAX_REV
                         or (b.revision_date is null and r.MAX_REV is null))
        where   b.YEAR_MONTH = @YM
            and b.ID_COMPANY = @idc
        group by b.ID_CONTACT, b.ID_ORGANIZATION, b.ID_COMPANY;

        drop table if exists #price_segment;
        -- M3: ценовой разрез — из общего агрегата #chk_agg (FIX H: LFL-периметр
        -- уже применён при построении агрегата)
        ;with t as
        (
            select  a.YEAR_MONTH
                  , a.YEAR
                  , a.ID_CONTACT
                  , a.ID_CITY_REPORT
                  , a.CITY_REPORT
                  , a.CAT_KEY
                  , b.SEGMENT
                  , a.QUANTITY
                  , a.COST_DISCOUNT
                  , a.REAL_DISCOUNT
                  , a.DISCOUNT
                  , a.CHECKS
                  , a.ID_COMPANY
                  , a.ID_ORGANIZATION
            from    #chk_agg as a
                    inner join #psclients as b
                        on  a.ID_CONTACT = b.ID_CONTACT
                        and a.ID_COMPANY = b.ID_COMPANY
                        and a.ID_ORGANIZATION = b.ID_ORGANIZATION
            where   a.YEAR_MONTH = @YM
        ), cont as
        (
            select  ALL_CONTACTS = count(distinct ID_CONTACT)
                  , YEAR_MONTH
                  , ID_CITY_REPORT
                  , ID_COMPANY
                  , ID_ORGANIZATION
                  , SEGMENT   -- 11.10.2026: знаменатель — покупатели своего сегмента
            from    t
            group by YEAR_MONTH, ID_COMPANY, ID_CITY_REPORT, ID_ORGANIZATION, SEGMENT
        )
        select  YEAR
              , t.YEAR_MONTH
              , CITY_REPORT
              , t.SEGMENT
              , t.ID_CITY_REPORT
              , t.CAT_KEY
              , CONTACTS = (count(ID_CONTACT) * 1.0) / (ALL_CONTACTS * 1.00)
              , REAL_DISCOUNT = sum(REAL_DISCOUNT) / nullif((sum(REAL_DISCOUNT) + sum(COST_DISCOUNT)), 0)
              , DISCOUNT = sum(DISCOUNT) / nullif((sum(REAL_DISCOUNT) + sum(COST_DISCOUNT)), 0)
              , CHECKS = (sum(CHECKS) * 1.00) / (count(ID_CONTACT) * 1.00)
              , PRICE = sum(COST_DISCOUNT) / nullif(sum(QUANTITY), 0)
              , COST_DISCOUNT = sum(COST_DISCOUNT) / count(ID_CONTACT)
              , t.ID_COMPANY
              , t.ID_ORGANIZATION
        into    #price_segment
        from    t
                inner join cont as a
                    on  a.ID_CITY_REPORT = t.ID_CITY_REPORT
                    and a.YEAR_MONTH = t.YEAR_MONTH
                    and a.ID_COMPANY = t.ID_COMPANY
                    and a.ID_ORGANIZATION = t.ID_ORGANIZATION
                    and a.SEGMENT = t.SEGMENT
        group by t.YEAR_MONTH
              , t.ID_CITY_REPORT
              , t.CAT_KEY
              , YEAR
              , CITY_REPORT
              , ALL_CONTACTS
              , t.SEGMENT
              , t.ID_COMPANY
              , t.ID_ORGANIZATION;

        -- HIGH-PRICE SEGMENT (база — MIDDLE 2)
        insert into dbo.I_TREND
              ( ID_CITY_REPORT, CITY_REPORT, YEAR_MONTH, SEGMENT
              , I_CATEGORY_5
              , CONTACTS, PER_DELTA_CONTACTS, PRICE, PER_DELTA_PRICE
              , COST_DISCOUNT, REAL_DISCOUNT, PER_DELTA_REAL_DISCOUNT
              , DISCOUNT, PER_DELTA_DISCOUNT, CHECKS_PER_CLIENT, PER_DELTA_CHECKS
              , ID_COMPANY, ID_ORGANIZATION, ID_CATEGORY_5_ext )
        select  a.ID_CITY_REPORT
              , a.CITY_REPORT
              , YEAR_MONTH = @YM
              , SEGMENT = 'HIGH-PRICE SEGMENT'
              , k.I_CATEGORY_5
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
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
              , k.ID_CATEGORY_5_ext
        from    #price_segment as a
                left join #price_segment as b
                    on  a.CAT_KEY = b.CAT_KEY
                    and a.ID_CITY_REPORT = b.ID_CITY_REPORT
                    and a.ID_COMPANY = b.ID_COMPANY
                    and a.ID_ORGANIZATION = b.ID_ORGANIZATION
                    and b.SEGMENT = 2
                inner join #cat_key as k
                    on  k.CAT_KEY = a.CAT_KEY
        where   a.SEGMENT = 1;

        -- LOW-PRICE SEGMENT (база — MIDDLE 2)
        insert into dbo.I_TREND
              ( ID_CITY_REPORT, CITY_REPORT, YEAR_MONTH, SEGMENT
              , I_CATEGORY_5
              , CONTACTS, PER_DELTA_CONTACTS, PRICE, PER_DELTA_PRICE
              , COST_DISCOUNT, REAL_DISCOUNT, PER_DELTA_REAL_DISCOUNT
              , DISCOUNT, PER_DELTA_DISCOUNT, CHECKS_PER_CLIENT, PER_DELTA_CHECKS
              , ID_COMPANY, ID_ORGANIZATION, ID_CATEGORY_5_ext )
        select  a.ID_CITY_REPORT
              , a.CITY_REPORT
              , YEAR_MONTH = @YM
              , SEGMENT = 'LOW-PRICE SEGMENT'
              , k.I_CATEGORY_5
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
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
              , k.ID_CATEGORY_5_ext
        from    #price_segment as a
                left join #price_segment as b
                    on  a.CAT_KEY = b.CAT_KEY
                    and a.ID_CITY_REPORT = b.ID_CITY_REPORT
                    and a.ID_COMPANY = b.ID_COMPANY
                    and a.ID_ORGANIZATION = b.ID_ORGANIZATION
                    and b.SEGMENT = 2
                inner join #cat_key as k
                    on  k.CAT_KEY = a.CAT_KEY
        where   a.SEGMENT = 3;

        -- Сводка по ценовым сегментам (HIGH / MIDDLE / LOW)
        ;with t as
        (
            select  ALL_CLIENTS = count(ID_CONTACT), ID_ORGANIZATION
            from    #psclients
            group by ID_ORGANIZATION
        )
        insert into dbo.I_TREND_PRICE_SEGMENT
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
              , BONUS_PAY = sum(BONUS_PAY)
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

        -- ============================================================
        -- ЧЕКОВЫЕ СЕГМЕНТЫ (FIX K): тренды BIG/LOW CHECK и сводка.
        -- Карта клиентов #ckclients построена в начале итерации.
        -- ============================================================
        drop table if exists #check_segment;
        -- M3: чековый разрез — из общего агрегата #chk_agg (FIX H:
        -- LFL-периметр уже применён при построении агрегата)
        ;with t as
        (
            select  a.YEAR_MONTH
                  , a.YEAR
                  , a.ID_CONTACT
                  , a.ID_CITY_REPORT
                  , a.CITY_REPORT
                  , a.CAT_KEY
                  , b.SEGMENT
                  , a.QUANTITY
                  , a.COST_DISCOUNT
                  , a.REAL_DISCOUNT
                  , a.DISCOUNT
                  , a.CHECKS
                  , a.ID_COMPANY
                  , a.ID_ORGANIZATION
            from    #chk_agg as a
                    inner join #ckclients as b
                        on  a.ID_CONTACT = b.ID_CONTACT
                        and a.ID_COMPANY = b.ID_COMPANY
                        and a.ID_ORGANIZATION = b.ID_ORGANIZATION
            where   a.YEAR_MONTH = @YM
        ), cont as
        (
            select  ALL_CONTACTS = count(distinct ID_CONTACT)
                  , YEAR_MONTH
                  , ID_CITY_REPORT
                  , ID_COMPANY
                  , ID_ORGANIZATION
                  , SEGMENT   -- 11.10.2026: знаменатель — покупатели своего сегмента
            from    t
            group by YEAR_MONTH, ID_COMPANY, ID_CITY_REPORT, ID_ORGANIZATION, SEGMENT
        )
        select  YEAR
              , t.YEAR_MONTH
              , CITY_REPORT
              , t.SEGMENT
              , t.ID_CITY_REPORT
              , t.CAT_KEY
              , CONTACTS = (count(ID_CONTACT) * 1.0) / (ALL_CONTACTS * 1.00)
              , REAL_DISCOUNT = sum(REAL_DISCOUNT) / nullif((sum(REAL_DISCOUNT) + sum(COST_DISCOUNT)), 0)
              , DISCOUNT = sum(DISCOUNT) / nullif((sum(REAL_DISCOUNT) + sum(COST_DISCOUNT)), 0)
              , CHECKS = (sum(CHECKS) * 1.00) / (count(ID_CONTACT) * 1.00)
              , PRICE = sum(COST_DISCOUNT) / nullif(sum(QUANTITY), 0)
              , COST_DISCOUNT = sum(COST_DISCOUNT) / count(ID_CONTACT)
              , t.ID_COMPANY
              , t.ID_ORGANIZATION
        into    #check_segment
        from    t
                inner join cont as a
                    on  a.ID_CITY_REPORT = t.ID_CITY_REPORT
                    and a.YEAR_MONTH = t.YEAR_MONTH
                    and a.ID_COMPANY = t.ID_COMPANY
                    and a.ID_ORGANIZATION = t.ID_ORGANIZATION
                    and a.SEGMENT = t.SEGMENT
        group by t.YEAR_MONTH
              , t.ID_CITY_REPORT
              , t.CAT_KEY
              , YEAR
              , CITY_REPORT
              , ALL_CONTACTS
              , t.SEGMENT
              , t.ID_COMPANY
              , t.ID_ORGANIZATION;

        -- BIG CHECK (база — AVG 1001)
        insert into dbo.I_TREND
              ( ID_CITY_REPORT, CITY_REPORT, YEAR_MONTH, SEGMENT
              , I_CATEGORY_5
              , CONTACTS, PER_DELTA_CONTACTS, PRICE, PER_DELTA_PRICE
              , COST_DISCOUNT, REAL_DISCOUNT, PER_DELTA_REAL_DISCOUNT
              , DISCOUNT, PER_DELTA_DISCOUNT, CHECKS_PER_CLIENT, PER_DELTA_CHECKS
              , ID_COMPANY, ID_ORGANIZATION, ID_CATEGORY_5_ext )
        select  a.ID_CITY_REPORT
              , a.CITY_REPORT
              , YEAR_MONTH = @YM
              , SEGMENT = 'BIG CHECK'
              , k.I_CATEGORY_5
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
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
              , k.ID_CATEGORY_5_ext
        from    #check_segment as a
                left join #check_segment as b                      -- FIX G: база в ON
                    on  a.CAT_KEY = b.CAT_KEY
                    and a.ID_CITY_REPORT = b.ID_CITY_REPORT
                    and a.ID_COMPANY = b.ID_COMPANY
                    and a.ID_ORGANIZATION = b.ID_ORGANIZATION
                    and b.SEGMENT = 1001
                inner join #cat_key as k
                    on  k.CAT_KEY = a.CAT_KEY
        where   a.SEGMENT = 1000;

        -- LOW CHECK (база — AVG 1001)
        insert into dbo.I_TREND
              ( ID_CITY_REPORT, CITY_REPORT, YEAR_MONTH, SEGMENT
              , I_CATEGORY_5
              , CONTACTS, PER_DELTA_CONTACTS, PRICE, PER_DELTA_PRICE
              , COST_DISCOUNT, REAL_DISCOUNT, PER_DELTA_REAL_DISCOUNT
              , DISCOUNT, PER_DELTA_DISCOUNT, CHECKS_PER_CLIENT, PER_DELTA_CHECKS
              , ID_COMPANY, ID_ORGANIZATION, ID_CATEGORY_5_ext )
        select  a.ID_CITY_REPORT
              , a.CITY_REPORT
              , YEAR_MONTH = @YM
              , SEGMENT = 'LOW CHECK'
              , k.I_CATEGORY_5
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
              , a.ID_COMPANY
              , a.ID_ORGANIZATION
              , k.ID_CATEGORY_5_ext
        from    #check_segment as a
                left join #check_segment as b
                    on  a.CAT_KEY = b.CAT_KEY
                    and a.ID_CITY_REPORT = b.ID_CITY_REPORT
                    and a.ID_COMPANY = b.ID_COMPANY
                    and a.ID_ORGANIZATION = b.ID_ORGANIZATION
                    and b.SEGMENT = 1001
                inner join #cat_key as k
                    on  k.CAT_KEY = a.CAT_KEY
        where   a.SEGMENT = 1002;

        -- Сводка по чековым сегментам (BIG / AVG / LOW CHECK)
        ;with t as
        (
            select  ALL_CLIENTS = count(ID_CONTACT), ID_ORGANIZATION
            from    #ckclients
            group by ID_ORGANIZATION
        )
        insert into dbo.I_TREND_PRICE_SEGMENT
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
              , BONUS_PAY = sum(BONUS_PAY)
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

-- процедура не обновилась (ALTER упал) — дальше ничего не создаём: иначе таблица отметки уже есть, повтор скрипта
-- остановится на охране, а шаг звал бы trends с лишним параметром
IF NOT EXISTS (SELECT 1 FROM sys.parameters WHERE object_id = OBJECT_ID('dbo.trends') AND name = N'@YM_STOP')
BEGIN
    RAISERROR(N'СТОП: dbo.trends не обновилась (нет параметра @YM_STOP) — таблицу, шаг и джоб не создаю.', 16, 1);
    SET NOEXEC ON;
END
GO

-- ============================== 2. ОТМЕТКА ХОДА ==============================
CREATE TABLE dbo.CVM_TRENDS_REBUILD (
      NEXT_YM   int           NOT NULL   -- с какого месяца следующая порция
    , STOP_YM   int           NOT NULL   -- до какого месяца (не включается)
    , PORTION   int           NOT NULL   -- месяцев за вечер
    , DONE      bit           NOT NULL
    , UPDATED   datetime2(0)  NOT NULL
    , LAST_NOTE nvarchar(300) NULL
);
-- 202609 тоже: посчитан ночью 10.10 со старым знаменателем
INSERT INTO dbo.CVM_TRENDS_REBUILD (NEXT_YM, STOP_YM, PORTION, DONE, UPDATED, LAST_NOTE)
VALUES (202401, 202610, 5, 0, sysdatetime(), N'заведено');
GO

-- ============================== 3. ВЕЧЕРНИЙ ПРОХОД ==============================
CREATE OR ALTER PROCEDURE dbo.cvm_trends_rebuild_step
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @next int, @stop int, @portion int, @done bit, @to int, @n int = 0, @lock int, @missions_open bit
          , @t0 datetime2(0), @now datetime2(0), @deadline datetime2(0), @last_sec int
          , @msg nvarchar(300), @note nvarchar(300);

    -- второй экземпляр (ручной EXEC при идущем джобе) не начинает: тот же месяц посчитался бы дважды.
    -- Замок сессионный: EXEC, прерванный в окне SSMS (Stop — это отмена, а не ошибка, CATCH её не ловит), держит его,
    -- пока окно открыто, и все проходы до закрытия окна пропускаются — внеплановый проход только через sp_start_job.
    EXEC @lock = sp_getapplock @Resource = N'cvm_trends_rebuild_step', @LockMode = N'Exclusive',
                               @LockOwner = N'Session', @LockTimeout = 0;
    IF @lock < 0
    BEGIN
        UPDATE dbo.CVM_TRENDS_REBUILD
        SET UPDATED = sysdatetime(),
            LAST_NOTE = N'пропуск: идёт другой проход — джоб или EXEC в окне SSMS (прерванный EXEC держит замок, пока окно открыто)';
        RETURN;
    END

    SELECT TOP (1) @next = NEXT_YM, @stop = STOP_YM, @portion = PORTION, @done = DONE FROM dbo.CVM_TRENDS_REBUILD (nolock);

    -- граница вечера: ближайшие 00:30 — за полчаса до перезагрузки «MCI Reload every day» в 01:00
    SET @now = sysdatetime();
    SET @deadline = dateadd(minute, 30, convert(datetime2(0), convert(date, @now)));   -- 00:30 сегодня
    IF @deadline <= @now SET @deadline = dateadd(day, 1, @deadline);                   -- прошло → 00:30 завтра

    WHILE @done = 0 AND @next < @stop AND @n < @portion
    BEGIN
        -- Проверки, граница месяца и расчёт — в одном TRY: любая ошибка обрывает проход — отметка не сдвигается,
        -- шаг джоба падает, текст — в LAST_NOTE. Без TRY ошибка, которая обрывает один оператор, а не батч,
        -- не останавливала бы процедуру: нечитаемая msdb (права владельца джоба) пропустила бы проверки MCI и миссий —
        -- IF с ошибкой в условии не выполняет ни одну ветку — и месяц пошёл бы при идущем джобе; неверный NEXT_YM (ручная
        -- правка, например 202413) оставил бы @to = NULL — trends без верхней границы: удаление всех месяцев с NEXT_YM;
        -- сбой внутри trends (усечение, NULL в NOT NULL, нехватка места, «слишком много аргументов», если процедуру
        -- заменят) — UPDATE ниже сдвинул бы отметку через недосчитанный месяц, и он остался бы дырой.
        BEGIN TRY
            SET @now = sysdatetime();
            -- с 00:30 до 17:00 идут ночные джобы MCI (перезагрузка с 01:00, ночной проход ~03:15–15:30)
            IF convert(time(0), @now) >= '00:30' AND convert(time(0), @now) < '17:00'
            BEGIN
                SET @note = N'остановлено: не вечер (месяцы считаются с 17:00 до 00:30)';
                BREAK;
            END
            -- ночной джоб MCI или старый разовый догон идёт — месяц не начинаем (шаг 43 и CVM REBUILD ONCE пишут те же таблицы;
            -- все три задания MCI Reload шагом 3 запускают MCI Regular update)
            IF EXISTS (SELECT 1
                       FROM msdb.dbo.sysjobactivity AS a (nolock)
                            JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id
                       WHERE j.name IN (N'MCI Regular update', N'MCI Reload every day', N'MCI Reload one time', N'MCI Reload',
                                        N'CVM REBUILD ONCE')
                         AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL
                         AND a.session_id = (SELECT max(session_id) FROM msdb.dbo.syssessions (nolock)))
            BEGIN
                SET @note = N'остановлено: идёт ночной джоб MCI или CVM REBUILD ONCE';
                BREAK;
            END
            -- пересчёт миссий не закончен или его джоб идёт — месяц не начинаем: оба пересчёта тяжёлые (~1 ч на месяц),
            -- идут друг за другом. Нет таблицы отметки миссий (скрипт миссий не ставили) — не ждём. Чтение таблицы —
            -- отдельным оператором под IF: в одном условии с OBJECT_ID отсутствующая таблица дала бы ошибку 208
            -- при компиляции оператора, а её TRY этого же уровня не ловит
            SET @missions_open = 0;
            IF OBJECT_ID(N'dbo.CVM_MISSIONS_REBUILD', N'U') IS NOT NULL
                IF EXISTS (SELECT 1 FROM dbo.CVM_MISSIONS_REBUILD (nolock) WHERE DONE = 0)
                    SET @missions_open = 1;
            IF @missions_open = 1
               OR EXISTS (SELECT 1
                          FROM msdb.dbo.sysjobactivity AS a (nolock)
                               JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id
                          WHERE j.name = N'CVM MISSIONS REBUILD'
                            AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL
                            AND a.session_id = (SELECT max(session_id) FROM msdb.dbo.syssessions (nolock)))
            BEGIN
                SET @note = N'остановлено: не закончен пересчёт миссий (CVM_MISSIONS_REBUILD, DONE = 0) или идёт его джоб';
                BREAK;
            END
            -- месяц не успеет до 00:30: оценка — время прошлого месяца этого прохода, для первого — 1 ч (журнал 10.10: шаг 43 — 59 мин)
            IF dateadd(second, ISNULL(@last_sec, 3600), @now) > @deadline
            BEGIN
                SET @note = N'остановлено: следующий месяц не успеет до 00:30';
                BREAK;
            END

            SET @to = year(dateadd(month, 1, datefromparts(@next / 100, @next % 100, 1))) * 100
                    + month(dateadd(month, 1, datefromparts(@next / 100, @next % 100, 1)));
            SET @t0 = sysdatetime();
            EXEC dbo.trends @YM_START = @next, @YM_STOP = @to;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK;
            SET @msg = concat(N'ошибка в месяце ', @next, N': ', ERROR_MESSAGE());
            UPDATE dbo.CVM_TRENDS_REBUILD SET UPDATED = sysdatetime(), LAST_NOTE = @msg;
            EXEC sp_releaseapplock @Resource = N'cvm_trends_rebuild_step', @LockOwner = N'Session';
            THROW;
        END CATCH

        -- отметка сдвигается только после посчитанного месяца: упавший месяц пересчитается целиком в следующий раз
        SET @last_sec = datediff(second, @t0, sysdatetime());
        SET @msg = concat(N'посчитан ', @next, N' за ', @last_sec / 60, N' мин');
        SET @next = @to;
        SET @n += 1;
        UPDATE dbo.CVM_TRENDS_REBUILD
        SET NEXT_YM = @next, DONE = IIF(@next >= @stop, 1, 0), UPDATED = sysdatetime(), LAST_NOTE = @msg;
    END

    IF @next >= @stop
    BEGIN
        UPDATE dbo.CVM_TRENDS_REBUILD SET UPDATED = sysdatetime(), LAST_NOTE = N'пересчёт закончен';
        EXEC msdb.dbo.sp_update_job @job_name = N'CVM TRENDS REBUILD', @enabled = 0;
    END
    ELSE IF @next IS NOT NULL
        UPDATE dbo.CVM_TRENDS_REBUILD
        SET UPDATED = sysdatetime(),
            LAST_NOTE = concat(N'за проход посчитано месяцев: ', @n, N'; следующий — ', @next, ISNULL(N'; ' + @note, N''));

    EXEC sp_releaseapplock @Resource = N'cvm_trends_rebuild_step', @LockOwner = N'Session';
END
GO

-- ============================== 4. ДЖОБ: каждый день в 17:00 ==============================
-- владелец — как у работающего ночного джоба: Windows-владелец Agent проверяет в домене при каждом запуске
DECLARE @owner sysname = (SELECT TOP (1) SUSER_SNAME(owner_sid) FROM msdb.dbo.sysjobs (nolock) WHERE name = N'MCI Regular update');
IF @owner IS NULL SET @owner = suser_sname();
IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs (nolock) WHERE name = N'CVM TRENDS REBUILD')
    EXEC msdb.dbo.sp_delete_job @job_name = N'CVM TRENDS REBUILD';   -- остаток прошлого незавершённого запуска этого же скрипта
EXEC msdb.dbo.sp_add_job @job_name = N'CVM TRENDS REBUILD', @enabled = 1, @owner_login_name = @owner,
     @description = N'Пересчёт трендов 202401–202609 порциями после пересчёта миссий (CVM_TRENDS_REBUILD.sql). Сам выключается в конце.';
EXEC msdb.dbo.sp_add_jobstep @job_name = N'CVM TRENDS REBUILD', @step_id = 1, @step_name = N'trends portion',
     @subsystem = N'TSQL', @database_name = N'mci_model', @command = N'exec dbo.cvm_trends_rebuild_step;',
     @on_success_action = 1, @on_fail_action = 2;
EXEC msdb.dbo.sp_add_jobschedule @job_name = N'CVM TRENDS REBUILD', @name = N'CVM TRENDS REBUILD 17:00',
     @enabled = 1, @freq_type = 4, @freq_interval = 1, @freq_subday_type = 1, @active_start_time = 170000;   -- ежедневно, один раз в 17:00
EXEC msdb.dbo.sp_add_jobserver @job_name = N'CVM TRENDS REBUILD';
GO

-- ============================== 5. РАЗОВЫЙ «CVM REBUILD ONCE» — ВЫКЛЮЧИТЬ ==============================
-- скрипт миссий его уже выключает; здесь — если тренды ставят раньше: его шаг 1 (trends с 202401 без границы)
-- удалил бы и пересчитал все месяцы с 202401 параллельно порциям
IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs (nolock) WHERE name = N'CVM REBUILD ONCE')
    EXEC msdb.dbo.sp_update_job @job_name = N'CVM REBUILD ONCE', @enabled = 0;
GO

-- ============================== 6. ПЕРВЫЙ ПРОХОД — СРАЗУ, ЕСЛИ СЕЙЧАС ВЕЧЕР, MCI НЕ ИДЁТ И ПЕРЕСЧЁТ МИССИЙ ЗАКОНЧЕН ==============================
-- успеет ли месяц до 00:30, проход проверяет сам перед каждым месяцем
DECLARE @missions_open bit = 0;
IF OBJECT_ID(N'dbo.CVM_MISSIONS_REBUILD', N'U') IS NOT NULL   -- таблицы может не быть: чтение — отдельным оператором под IF
    IF EXISTS (SELECT 1 FROM dbo.CVM_MISSIONS_REBUILD (nolock) WHERE DONE = 0)
        SET @missions_open = 1;
IF convert(time(0), sysdatetime()) >= '00:30' AND convert(time(0), sysdatetime()) < '17:00'
    PRINT N'Сейчас не вечер — первый проход начнётся по расписанию в 17:00.';
ELSE IF EXISTS (SELECT 1
                FROM msdb.dbo.sysjobactivity AS a (nolock)
                     JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id
                WHERE j.name IN (N'MCI Regular update', N'MCI Reload every day', N'MCI Reload one time', N'MCI Reload',
                                 N'CVM REBUILD ONCE')
                  AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL
                  AND a.session_id = (SELECT max(session_id) FROM msdb.dbo.syssessions (nolock)))
    PRINT N'Ночной джоб MCI сейчас идёт — первый проход начнётся по расписанию в 17:00.';
ELSE IF @missions_open = 1
     OR EXISTS (SELECT 1
                FROM msdb.dbo.sysjobactivity AS a (nolock)
                     JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id
                WHERE j.name = N'CVM MISSIONS REBUILD'
                  AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL
                  AND a.session_id = (SELECT max(session_id) FROM msdb.dbo.syssessions (nolock)))
    PRINT N'Пересчёт миссий ещё не закончен — тренды начнутся по расписанию в 17:00 после его окончания.';
ELSE
BEGIN
    EXEC msdb.dbo.sp_start_job @job_name = N'CVM TRENDS REBUILD';
    PRINT N'Первый проход запущен. Ход: SELECT * FROM dbo.CVM_TRENDS_REBUILD;';
END
GO

SET NOEXEC OFF;
GO

-- ============================== 7. ИТОГ ==============================
SELECT j.name, j.enabled, s.name AS schedule_name, s.active_start_time
FROM msdb.dbo.sysjobs AS j (nolock)
     LEFT JOIN msdb.dbo.sysjobschedules AS js (nolock) ON js.job_id = j.job_id
     LEFT JOIN msdb.dbo.sysschedules AS s (nolock) ON s.schedule_id = js.schedule_id
WHERE j.name IN (N'CVM TRENDS REBUILD', N'CVM MISSIONS REBUILD', N'CVM REBUILD ONCE');
-- месяцы своих сегментов в I_TREND (из 8); nolock — месяц, который сейчас считает проход, держит транзакцию ~1 ч
SELECT YEAR_MONTH, SEGMENTS = count(DISTINCT SEGMENT), n = count(*)
FROM dbo.I_TREND (nolock)
WHERE ID_COMPANY = 1
  AND SEGMENT IN ('NEW TRENDS','RANDOM TRENDS','CHURN TRENDS',
                  'ACTIVE LFL TRENDS','HIGH-PRICE SEGMENT','LOW-PRICE SEGMENT',
                  'BIG CHECK','LOW CHECK')
GROUP BY YEAR_MONTH
ORDER BY YEAR_MONTH;
GO
-- отдельным батчем: после «СТОП» таблицы может не быть — ошибка 208 оборвала бы батч и скрыла итог выше
SELECT * FROM dbo.CVM_TRENDS_REBUILD (nolock);
