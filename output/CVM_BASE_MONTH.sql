-- ============================================================================================
-- CVM_BASE_MONTH — клиентская база для портала (отчёты «База клиентов» и «Динамика базы»):
-- витрины dbo.I_CVM_BASE_CURRENT (текущий срез) и dbo.I_CVM_BASE_MONTH (помесячные снимки) + процедура dbo.cvm_base_month.
-- Запустить ОДИН РАЗ целиком в SSMS (F5). В конце — сверка витрин с запросами портала.
--
-- Зачем: портал сейчас при каждом открытии агрегирует всю I_CVM_CONTACT_DATA с 2021 года (373 млн строк,
-- 2,5–3 минуты) и текущую I_CVM_CONTACT (15 млн строк, ~7 секунд). Снимки в I_CVM_CONTACT_DATA пишутся раз в месяц
-- (REFERENCE_DATE = последний день месяца), I_CVM_CONTACT — каждую ночь; агрегаты считаются ночью и хранятся,
-- портал читает готовые строки — отчёт открывается сразу.
--
-- I_CVM_BASE_CURRENT = текущий срез I_CVM_CONTACT: организация × канал × сегмент (~24 строки), те же показатели
-- и формулы, что у запроса портала «текущий срез»; перезаписывается целиком при каждом запуске процедуры.
--
-- Строка витрины = снимок (REFERENCE_DATE) × организация × канал продаж × сегмент — ровно то, что сейчас
-- считает запрос портала (канон-запрос «отчёты по базе» 07.10.2026): клиентов, доступных для push
-- (HAS_PUSH = 1 и TOKENS = 1), метрики — средние по клиентам, признаки — суммы.
--
-- Процедура dbo.cvm_base_month:
--   EXEC dbo.cvm_base_month;                                  — НОЧНОЙ: текущий срез — всегда (секунды); помесячные —
--                                                               только снимки, которых
--                                                               нет в витрине или у которых изменилось число
--                                                               клиентов, и каждую ночь — последний снимок;
--                                                               снимки, которых больше нет в
--                                                               I_CVM_CONTACT_DATA, убирает из витрины (секунды)
--   EXEC dbo.cvm_base_month @REFERENCE_DATE = '2026-09-30';   — пересчитать один снимок (например, после
--                                                               ручного пересчёта CVM_CONTACT_DATA за месяц)
-- Ночной шаг джоба (после CVM_CONTACT_DATA):   EXEC dbo.cvm_base_month;
-- ============================================================================================

USE mci_model
GO

SET NOCOUNT ON;

-- ============================== 1. ВИТРИНА ==============================
IF OBJECT_ID('dbo.I_CVM_BASE_MONTH') IS NULL
BEGIN
    CREATE TABLE dbo.I_CVM_BASE_MONTH (
          ID_COMPANY             int           NOT NULL
        , REFERENCE_DATE         date          NOT NULL
        , ID_ORGANIZATION        int           NULL
        , ID_SALE_CHANNEL        int           NULL
        , SEGMENT                int           NULL
        , CLIENTS                bigint        NOT NULL
        , PUSHABLE               bigint        NULL
        , PRICE_INDEX            float         NULL
        , BUDGET                 float         NULL
        , CHECKS                 float         NULL
        , AVG_CHECK              float         NULL
        , SHARE_WEEKEND          float         NULL
        , REAL_SALE              float         NULL
        , CARD_SALE              float         NULL
        , LTV                    float         NULL
        , TIME_PURCHASE_WORKDAY  float         NULL
        , FAV_PRODUCT_CHOICE     bigint        NULL
        , FAV_CATEGORY_CHOICE    bigint        NULL
        , FAV_PRODUCT_USE        bigint        NULL
        , FAV_CATEGORY_USE       bigint        NULL
        , PP_USE                 bigint        NULL
        , CVM_PARTICIPANTS       bigint        NULL
        , LOAD_DATE              datetime2(0)  NOT NULL
    );
    -- витрины — колоночный индекс (правило 09.10.2026); задвоение снимка исключают блокировка и DELETE + INSERT в одной транзакции
    CREATE CLUSTERED COLUMNSTORE INDEX cci_I_CVM_BASE_MONTH ON dbo.I_CVM_BASE_MONTH;
END
GO

-- текущий срез: ~24 строки, служебная таблица без индекса (правило 12 — для больших таблиц)
IF OBJECT_ID('dbo.I_CVM_BASE_CURRENT') IS NULL
    CREATE TABLE dbo.I_CVM_BASE_CURRENT (
          ID_COMPANY             int           NOT NULL
        , ID_ORGANIZATION        int           NULL
        , ID_SALE_CHANNEL        int           NULL
        , SEGMENT                int           NULL
        , CLIENTS                bigint        NOT NULL
        , PUSHABLE               bigint        NULL
        , PRICE_INDEX            float         NULL
        , BUDGET                 float         NULL
        , CHECKS                 float         NULL
        , AVG_CHECK              float         NULL
        , SHARE_WEEKEND          float         NULL
        , REAL_SALE              float         NULL
        , CARD_SALE              float         NULL
        , LTV                    float         NULL
        , TIME_PURCHASE_WORKDAY  float         NULL
        , FAV_PRODUCT_CHOICE     bigint        NULL
        , FAV_CATEGORY_CHOICE    bigint        NULL
        , FAV_PRODUCT_USE        bigint        NULL
        , FAV_CATEGORY_USE       bigint        NULL
        , PP_USE                 bigint        NULL
        , CVM_PARTICIPANTS       bigint        NULL
        , LAST_DATA              date          NULL
        , LOAD_DATE              datetime2(0)  NOT NULL
    );
GO

-- ============================== 2. ПРОЦЕДУРА ==============================
CREATE OR ALTER PROCEDURE dbo.cvm_base_month
      @REFERENCE_DATE date = NULL   -- один снимок; NULL = все новые и изменившиеся
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @idc int = 1;
    DECLARE @lock int;

    -- ── Текущий срез I_CVM_CONTACT → I_CVM_BASE_CURRENT (формулы запроса портала «текущий срез» один в один)
    SELECT c.ID_ORGANIZATION, c.ID_SALE_CHANNEL, c.SEGMENT
         , CLIENTS               = count_big(*)
         , PUSHABLE              = sum(CASE WHEN c.HAS_PUSH = 1 AND c.TOKENS = 1 THEN 1 ELSE 0 END)
         , PRICE_INDEX           = avg(cast(c.PRICE_INDEX AS float))
         , BUDGET                = avg(cast(c.BUDGET AS float))
         , CHECKS                = avg(cast(c.CHECKS AS float))
         , AVG_CHECK             = avg(cast(c.AVG_CHECK AS float))
         , SHARE_WEEKEND         = avg(cast(c.SHARE_WEEKEND AS float))
         , REAL_SALE             = avg(cast(c.REAL_SALE AS float))
         , CARD_SALE             = avg(cast(c.CARD_SALE AS float))
         , LTV                   = avg(cast(c.LTV AS float))
         , TIME_PURCHASE_WORKDAY = avg(cast(c.TIME_PURCHASE_WORKDAY AS float))
         , FAV_PRODUCT_CHOICE    = sum(cast(isnull(c.FAV_PRODUCT_CHOICE, 0) AS bigint))
         , FAV_CATEGORY_CHOICE   = sum(cast(isnull(c.FAV_CATEGORY_CHOICE, 0) AS bigint))
         , FAV_PRODUCT_USE       = sum(cast(isnull(c.FAV_PRODUCT_USE, 0) AS bigint))
         , FAV_CATEGORY_USE      = sum(cast(isnull(c.FAV_CATEGORY_USE, 0) AS bigint))
         , PP_USE                = sum(cast(isnull(c.PP_USE, 0) AS bigint))
         , CVM_PARTICIPANTS      = sum(cast(isnull(c.CVM_PARTICIPANT, 0) AS bigint))
         , LAST_DATA             = max(c.LAST_DATA)
    INTO #bm_cur
    FROM dbo.I_CVM_CONTACT AS c
    WHERE c.ID_COMPANY = @idc
    GROUP BY c.ID_ORGANIZATION, c.ID_SALE_CHANNEL, c.SEGMENT;

    BEGIN TRANSACTION;
    EXEC @lock = sp_getapplock @Resource = N'dbo.cvm_base_month', @LockMode = 'Exclusive', @LockOwner = 'Transaction';
    IF @lock < 0
        THROW 50001, N'cvm_base_month: не удалось взять блокировку записи витрины', 1;
    DELETE FROM dbo.I_CVM_BASE_CURRENT WHERE ID_COMPANY = @idc;
    INSERT INTO dbo.I_CVM_BASE_CURRENT
          (ID_COMPANY, ID_ORGANIZATION, ID_SALE_CHANNEL, SEGMENT, CLIENTS, PUSHABLE
         , PRICE_INDEX, BUDGET, CHECKS, AVG_CHECK, SHARE_WEEKEND, REAL_SALE, CARD_SALE, LTV, TIME_PURCHASE_WORKDAY
         , FAV_PRODUCT_CHOICE, FAV_CATEGORY_CHOICE, FAV_PRODUCT_USE, FAV_CATEGORY_USE, PP_USE, CVM_PARTICIPANTS, LAST_DATA, LOAD_DATE)
    SELECT @idc, ID_ORGANIZATION, ID_SALE_CHANNEL, SEGMENT, CLIENTS, PUSHABLE
         , PRICE_INDEX, BUDGET, CHECKS, AVG_CHECK, SHARE_WEEKEND, REAL_SALE, CARD_SALE, LTV, TIME_PURCHASE_WORKDAY
         , FAV_PRODUCT_CHOICE, FAV_CATEGORY_CHOICE, FAV_PRODUCT_USE, FAV_CATEGORY_USE, PP_USE, CVM_PARTICIPANTS, LAST_DATA, sysdatetime()
    FROM #bm_cur;
    COMMIT TRANSACTION;

    -- ── Помесячные снимки I_CVM_CONTACT_DATA → I_CVM_BASE_MONTH
    DECLARE @last_ref date = (SELECT max(REFERENCE_DATE) FROM dbo.I_CVM_CONTACT_DATA WHERE ID_COMPANY = @idc);

    -- имена #bm_* — свои: процедура видит временные таблицы вызвавшей сессии (SSMS), одноимённая #dates/#agg
    -- там дала бы неопределённое разрешение имени, а DROP IF EXISTS удалил бы чужую таблицу (sql-senior 09.10.2026)
    CREATE TABLE #bm_dates (REFERENCE_DATE date PRIMARY KEY);

    IF @REFERENCE_DATE IS NOT NULL
        INSERT INTO #bm_dates (REFERENCE_DATE) VALUES (@REFERENCE_DATE);
    ELSE
        -- FULL JOIN: снимок, которого больше нет в источнике (есть только в витрине), тоже попадает в список —
        -- DELETE ниже его убирает, а #bm_agg по нему пустой. REFERENCE_DATE IS NOT NULL: строка с пустой датой
        -- в эталон API не попадает (BETWEEN), а в PRIMARY KEY #bm_dates уронила бы ночной шаг (sql-senior 09.10.2026)
        INSERT INTO #bm_dates (REFERENCE_DATE)
        SELECT coalesce(s.REFERENCE_DATE, v.REFERENCE_DATE)
        FROM (SELECT REFERENCE_DATE, N = count_big(*)
              FROM dbo.I_CVM_CONTACT_DATA
              WHERE ID_COMPANY = @idc
                AND REFERENCE_DATE IS NOT NULL
              GROUP BY REFERENCE_DATE) AS s
             FULL JOIN (SELECT REFERENCE_DATE, N = sum(CLIENTS)
                        FROM dbo.I_CVM_BASE_MONTH
                        WHERE ID_COMPANY = @idc
                        GROUP BY REFERENCE_DATE) AS v ON v.REFERENCE_DATE = s.REFERENCE_DATE
        WHERE s.N IS NULL OR v.N IS NULL OR v.N <> s.N
           -- последний снимок пересчитывается каждую ночь (секунды): ловит пересчёт месяца с тем же числом клиентов
           OR s.REFERENCE_DATE = @last_ref;

    IF NOT EXISTS (SELECT 1 FROM #bm_dates) RETURN;

    DECLARE @d_min date = (SELECT min(REFERENCE_DATE) FROM #bm_dates);
    DECLARE @d_max date = (SELECT max(REFERENCE_DATE) FROM #bm_dates);

    SELECT d.REFERENCE_DATE, d.ID_ORGANIZATION, d.ID_SALE_CHANNEL, d.SEGMENT
         , CLIENTS               = count_big(*)
         , PUSHABLE              = sum(CASE WHEN d.HAS_PUSH = 1 AND d.TOKENS = 1 THEN 1 ELSE 0 END)
         , PRICE_INDEX           = avg(cast(d.PRICE_INDEX AS float))
         , BUDGET                = avg(cast(d.BUDGET AS float))
         , CHECKS                = avg(cast(d.CHECKS AS float))
         , AVG_CHECK             = avg(cast(d.AVG_CHECK AS float))
         , SHARE_WEEKEND         = avg(cast(d.SHARE_WEEKEND AS float))
         , REAL_SALE             = avg(cast(d.REAL_SALE AS float))
         , CARD_SALE             = avg(cast(d.CARD_SALE AS float))
         , LTV                   = avg(cast(d.LTV AS float))
         , TIME_PURCHASE_WORKDAY = avg(cast(d.TIME_PURCHASE_WORKDAY AS float))
         , FAV_PRODUCT_CHOICE    = sum(cast(isnull(d.FAV_PRODUCT_CHOICE, 0) AS bigint))
         , FAV_CATEGORY_CHOICE   = sum(cast(isnull(d.FAV_CATEGORY_CHOICE, 0) AS bigint))
         , FAV_PRODUCT_USE       = sum(cast(isnull(d.FAV_PRODUCT_USE, 0) AS bigint))
         , FAV_CATEGORY_USE      = sum(cast(isnull(d.FAV_CATEGORY_USE, 0) AS bigint))
         , PP_USE                = sum(cast(isnull(d.PP_USE, 0) AS bigint))
         , CVM_PARTICIPANTS      = sum(cast(isnull(d.CVM_PARTICIPANT, 0) AS bigint))
    INTO #bm_agg
    FROM dbo.I_CVM_CONTACT_DATA AS d          -- без nolock: читаем ровно как API, незакоммиченное в витрину не попадёт
         JOIN #bm_dates AS x ON x.REFERENCE_DATE = d.REFERENCE_DATE
    WHERE d.ID_COMPANY = @idc
      AND d.REFERENCE_DATE BETWEEN @d_min AND @d_max
    GROUP BY d.REFERENCE_DATE, d.ID_ORGANIZATION, d.ID_SALE_CHANNEL, d.SEGMENT;

    BEGIN TRANSACTION;

    -- один запуск за раз: параллельный EXEC (ночной шаг + ручной) ждёт здесь, а не задваивает строки снимка;
    -- блокировка транзакционная — снимается сама на COMMIT или откате (sql-senior 09.10.2026)
    EXEC @lock = sp_getapplock @Resource = N'dbo.cvm_base_month', @LockMode = 'Exclusive', @LockOwner = 'Transaction';
    IF @lock < 0
        THROW 50001, N'cvm_base_month: не удалось взять блокировку записи витрины', 1;

    DELETE v
    FROM dbo.I_CVM_BASE_MONTH AS v
         JOIN #bm_dates AS x ON x.REFERENCE_DATE = v.REFERENCE_DATE
    WHERE v.ID_COMPANY = @idc;

    INSERT INTO dbo.I_CVM_BASE_MONTH
          (ID_COMPANY, REFERENCE_DATE, ID_ORGANIZATION, ID_SALE_CHANNEL, SEGMENT, CLIENTS, PUSHABLE
         , PRICE_INDEX, BUDGET, CHECKS, AVG_CHECK, SHARE_WEEKEND, REAL_SALE, CARD_SALE, LTV, TIME_PURCHASE_WORKDAY
         , FAV_PRODUCT_CHOICE, FAV_CATEGORY_CHOICE, FAV_PRODUCT_USE, FAV_CATEGORY_USE, PP_USE, CVM_PARTICIPANTS, LOAD_DATE)
    SELECT @idc, REFERENCE_DATE, ID_ORGANIZATION, ID_SALE_CHANNEL, SEGMENT, CLIENTS, PUSHABLE
         , PRICE_INDEX, BUDGET, CHECKS, AVG_CHECK, SHARE_WEEKEND, REAL_SALE, CARD_SALE, LTV, TIME_PURCHASE_WORKDAY
         , FAV_PRODUCT_CHOICE, FAV_CATEGORY_CHOICE, FAV_PRODUCT_USE, FAV_CATEGORY_USE, PP_USE, CVM_PARTICIPANTS, sysdatetime()
    FROM #bm_agg;

    COMMIT TRANSACTION;
END
GO

-- ============================== 3. ПЕРВЫЙ ПРОГОН: текущий срез и все снимки (2,5–3 минуты) ==============================
EXEC dbo.cvm_base_month;
GO

-- ============================== 4. СВЕРКА ==============================
-- 4.1. Последний снимок 1-в-1 с запросом портала: обе выборки должны быть ПУСТЫМИ
--      (средние сравниваются с округлением до 6 знаков — при параллельном плане последние знаки float плавают).
DECLARE @last_ref date = (SELECT max(REFERENCE_DATE) FROM dbo.I_CVM_CONTACT_DATA WHERE ID_COMPANY = 1);

WITH api AS (
    SELECT d.REFERENCE_DATE, d.ID_ORGANIZATION, d.ID_SALE_CHANNEL, d.SEGMENT
         , CLIENTS = count_big(*), PUSHABLE = cast(sum(CASE WHEN d.HAS_PUSH = 1 AND d.TOKENS = 1 THEN 1 ELSE 0 END) AS bigint)
         , PRICE_INDEX = round(avg(cast(d.PRICE_INDEX AS float)), 6), BUDGET = round(avg(cast(d.BUDGET AS float)), 6)
         , CHECKS = round(avg(cast(d.CHECKS AS float)), 6), AVG_CHECK = round(avg(cast(d.AVG_CHECK AS float)), 6)
         , SHARE_WEEKEND = round(avg(cast(d.SHARE_WEEKEND AS float)), 6), REAL_SALE = round(avg(cast(d.REAL_SALE AS float)), 6)
         , CARD_SALE = round(avg(cast(d.CARD_SALE AS float)), 6), LTV = round(avg(cast(d.LTV AS float)), 6)
         , TIME_PURCHASE_WORKDAY = round(avg(cast(d.TIME_PURCHASE_WORKDAY AS float)), 6)
         , FAV_PRODUCT_CHOICE = sum(cast(isnull(d.FAV_PRODUCT_CHOICE, 0) AS bigint)), FAV_CATEGORY_CHOICE = sum(cast(isnull(d.FAV_CATEGORY_CHOICE, 0) AS bigint))
         , FAV_PRODUCT_USE = sum(cast(isnull(d.FAV_PRODUCT_USE, 0) AS bigint)), FAV_CATEGORY_USE = sum(cast(isnull(d.FAV_CATEGORY_USE, 0) AS bigint))
         , PP_USE = sum(cast(isnull(d.PP_USE, 0) AS bigint)), CVM_PARTICIPANTS = sum(cast(isnull(d.CVM_PARTICIPANT, 0) AS bigint))
    FROM dbo.I_CVM_CONTACT_DATA AS d
    WHERE d.ID_COMPANY = 1 AND d.REFERENCE_DATE = @last_ref
    GROUP BY d.REFERENCE_DATE, d.ID_ORGANIZATION, d.ID_SALE_CHANNEL, d.SEGMENT
), vit AS (
    SELECT REFERENCE_DATE, ID_ORGANIZATION, ID_SALE_CHANNEL, SEGMENT, CLIENTS, PUSHABLE
         , PRICE_INDEX = round(PRICE_INDEX, 6), BUDGET = round(BUDGET, 6), CHECKS = round(CHECKS, 6)
         , AVG_CHECK = round(AVG_CHECK, 6), SHARE_WEEKEND = round(SHARE_WEEKEND, 6)
         , REAL_SALE = round(REAL_SALE, 6), CARD_SALE = round(CARD_SALE, 6), LTV = round(LTV, 6)
         , TIME_PURCHASE_WORKDAY = round(TIME_PURCHASE_WORKDAY, 6)
         , FAV_PRODUCT_CHOICE, FAV_CATEGORY_CHOICE, FAV_PRODUCT_USE, FAV_CATEGORY_USE, PP_USE, CVM_PARTICIPANTS
    FROM dbo.I_CVM_BASE_MONTH
    WHERE ID_COMPANY = 1 AND REFERENCE_DATE = @last_ref
)
SELECT [Расхождение] = N'есть в запросе портала, нет в витрине', * FROM (SELECT * FROM api EXCEPT SELECT * FROM vit) AS a
UNION ALL
SELECT N'есть в витрине, нет в запросе портала', * FROM (SELECT * FROM vit EXCEPT SELECT * FROM api) AS b;

-- 4.2. Клиентов в витрине = строк в I_CVM_CONTACT_DATA по каждому снимку
-- FULL JOIN — чтобы видеть и снимки, которые остались только в витрине (sql-senior 09.10.2026)
SELECT REFERENCE_DATE                 = coalesce(s.REFERENCE_DATE, v.REFERENCE_DATE)
     , [Строк в I_CVM_CONTACT_DATA] = s.N
     , [Клиентов в витрине]         = v.N
     , [Сходится]                   = IIF(s.N = v.N, N'да', N'НЕТ')
FROM (SELECT REFERENCE_DATE, N = count_big(*) FROM dbo.I_CVM_CONTACT_DATA (nolock)
      WHERE ID_COMPANY = 1 AND REFERENCE_DATE IS NOT NULL GROUP BY REFERENCE_DATE) AS s
     FULL JOIN (SELECT REFERENCE_DATE, N = sum(CLIENTS) FROM dbo.I_CVM_BASE_MONTH WHERE ID_COMPANY = 1 GROUP BY REFERENCE_DATE) AS v
       ON v.REFERENCE_DATE = s.REFERENCE_DATE
ORDER BY coalesce(s.REFERENCE_DATE, v.REFERENCE_DATE) DESC;

-- 4.3. Текущий срез 1-в-1 с запросом портала: обе выборки должны быть ПУСТЫМИ
WITH api AS (
    SELECT c.ID_ORGANIZATION, c.ID_SALE_CHANNEL, c.SEGMENT
         , CLIENTS = count_big(*), PUSHABLE = cast(sum(CASE WHEN c.HAS_PUSH = 1 AND c.TOKENS = 1 THEN 1 ELSE 0 END) AS bigint)
         , PRICE_INDEX = round(avg(cast(c.PRICE_INDEX AS float)), 6), BUDGET = round(avg(cast(c.BUDGET AS float)), 6)
         , CHECKS = round(avg(cast(c.CHECKS AS float)), 6), AVG_CHECK = round(avg(cast(c.AVG_CHECK AS float)), 6)
         , SHARE_WEEKEND = round(avg(cast(c.SHARE_WEEKEND AS float)), 6), REAL_SALE = round(avg(cast(c.REAL_SALE AS float)), 6)
         , CARD_SALE = round(avg(cast(c.CARD_SALE AS float)), 6), LTV = round(avg(cast(c.LTV AS float)), 6)
         , TIME_PURCHASE_WORKDAY = round(avg(cast(c.TIME_PURCHASE_WORKDAY AS float)), 6)
         , FAV_PRODUCT_CHOICE = sum(cast(isnull(c.FAV_PRODUCT_CHOICE, 0) AS bigint)), FAV_CATEGORY_CHOICE = sum(cast(isnull(c.FAV_CATEGORY_CHOICE, 0) AS bigint))
         , FAV_PRODUCT_USE = sum(cast(isnull(c.FAV_PRODUCT_USE, 0) AS bigint)), FAV_CATEGORY_USE = sum(cast(isnull(c.FAV_CATEGORY_USE, 0) AS bigint))
         , PP_USE = sum(cast(isnull(c.PP_USE, 0) AS bigint)), CVM_PARTICIPANTS = sum(cast(isnull(c.CVM_PARTICIPANT, 0) AS bigint))
         , LAST_DATA = max(c.LAST_DATA)
    FROM dbo.I_CVM_CONTACT AS c
    WHERE c.ID_COMPANY = 1
    GROUP BY c.ID_ORGANIZATION, c.ID_SALE_CHANNEL, c.SEGMENT
), vit AS (
    SELECT ID_ORGANIZATION, ID_SALE_CHANNEL, SEGMENT, CLIENTS, PUSHABLE
         , PRICE_INDEX = round(PRICE_INDEX, 6), BUDGET = round(BUDGET, 6), CHECKS = round(CHECKS, 6)
         , AVG_CHECK = round(AVG_CHECK, 6), SHARE_WEEKEND = round(SHARE_WEEKEND, 6)
         , REAL_SALE = round(REAL_SALE, 6), CARD_SALE = round(CARD_SALE, 6), LTV = round(LTV, 6)
         , TIME_PURCHASE_WORKDAY = round(TIME_PURCHASE_WORKDAY, 6)
         , FAV_PRODUCT_CHOICE, FAV_CATEGORY_CHOICE, FAV_PRODUCT_USE, FAV_CATEGORY_USE, PP_USE, CVM_PARTICIPANTS, LAST_DATA
    FROM dbo.I_CVM_BASE_CURRENT
    WHERE ID_COMPANY = 1
)
SELECT [Расхождение текущего среза] = N'есть в запросе портала, нет в витрине', * FROM (SELECT * FROM api EXCEPT SELECT * FROM vit) AS a
UNION ALL
SELECT N'есть в витрине, нет в запросе портала', * FROM (SELECT * FROM vit EXCEPT SELECT * FROM api) AS b;
