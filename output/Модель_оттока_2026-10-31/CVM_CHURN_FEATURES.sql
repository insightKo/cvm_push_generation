-- CVM · модель оттока, Т2: витрина признаков и исхода — слой недель (Н0–Н5), срезы для обучения (С1), журнал, история
-- ОК Елены 01.11.2026 на таблицы Н0–Н5, С1 (Т1_источники.txt п. 2.1, категории — вариант (а)); панель 10 % — CVM_CHURN_PANEL.sql
-- (сначала он: срезы и таблицы панели читают I_CVM_CHURN_PANEL). Бэклог: output/Модель_оттока_2026-10-31/бэклог_задач.txt, Т2.
--
-- Что делает скрипт (по объектам; все таблицы новые, колоночные, недельные — секции по номеру недели I_CALENDAR):
--   pf_CVM_CHURN_WEEK / ps_CVM_CHURN_WEEK — функция и схема секций по неделе (850…1800); перезапись недели = TRUNCATE секции.
--   I_CVM_CHURN_FIRST   (Н0) первая покупка: клиент × организация → дата (стаж, «Новый на дату»); все клиенты.
--   I_CVM_CHURN_WEEK    (Н1) покупки: клиент × организация × неделя, все клиенты, только недели с покупкой: чеки, дни
--                       (маска дней недели — из неё точные даты покупок), траты после и до скидки, скидка, SKU, макс. чек,
--                       выходные, утро/вечер, магазины и основной магазин недели, бонусы, дни > 5 чеков, чеки по
--                       виртуальной карте (приложение), чеки с наградой 5ПП / акций CVM / акций лояльности (I_CHECK_RULE).
--   I_CVM_CHURN_CAT_W   (Н2а) категории CVM: клиент панели × орг. × неделя × категория — траты, количество, чеки.
--   I_CVM_CHURN_CAT_W_ALL (Н2б) то же для всех клиентов, скользящие 33 недели (для скоринга Т4).
--                       Табак — категория −1, энергетики — −2, товара нет в справочнике — 0 (в признаки не идут).
--   I_CVM_CHURN_ITEM_W  (Н3) товары клиента панели по неделям (без табака и энергетиков): траты, количество, чеки.
--   I_CVM_CHURN_COMM_W  (Н4) рассылки: клиент × неделя, все клиенты: пуш — сообщений, доставлено, переходов, ошибок;
--                       email — сообщений, доставлено, открыто, переходов, отписка/спам; SMS — сообщений, доставлено.
--   I_CVM_CHURN_EVENT   (Н5) события клиентов панели: ЦГ/КГ акций CVM и 5ПП, выбор любимых категорий и товаров, гашения
--                       купонов; пересобирается целиком.
--   I_CVM_CHURN_SLICE   (С1) срезы обучения: воскресенье × клиент панели × организация с покупкой за 6 нед. до среза
--                       (без фрода): ≈ 160 признаков строго по данным до среза + 20 групп категорий CVM (по 4 колонки) +
--                       оба исхода (покупки, дни, траты за 6 нед. после среза; дней до первой покупки) + сегмент на дату и
--                       через 6 нед. по правилу CVM_CONTACT. Норма и разброс — в признаках, порога частичного оттока нет.
--   I_CVM_CHURN_LOG     журнал расчёта (шаг, неделя, время, строк, статус) — история продолжает с места остановки.
--   Процедуры: CVM_CHURN_TRUNC (очистка секций), CVM_CHURN_WEEK_LAYER (Н1–Н4 за неделю), CVM_CHURN_FIRST_BUILD (Н0),
--   CVM_CHURN_EVENTS_BUILD (Н5), CVM_CHURN_SLICES (С1 за пачку срезов), CVM_CHURN_HISTORY (вся история, с ожиданием).
--   Задание SQL Agent «CVM_CHURN_HISTORY» — разовое, старт 07.11.2026 15:00 (раньше нельзя: до ≈ 04.11 идёт история
--   ценовых витрин, 06.11 — первая пятница с перезаливкой 3 мес.); после успеха удаляется само.
-- Существующие таблицы и задания не меняются. Недельный шаг в «MCI Regular update» — отдельно, после истории (Т2/Т4).
--
-- Источники (только чтение): I_CHECKHEADER, I_CHECK_RULE, I_CLIENT_ITEM_DAY, I_CVM_PRODUCT_CATEGORY, I_PROMO_DELIVERY_REPORT,
-- I_PROMO, I_PROMO_all, I_CAMPAIGN, I_LOYALTY_PROMO, I_PROMO_OFFER, fav_category, fav_product, loyalty__coupons_application_cur,
-- I_CARD, I_CONTACT, I_CONT_ASSORT_W, I_SHOP_WEEK, I_FRAUD_CONTACT, I_MISTAKE_CONTACT, I_CALENDAR.
--
-- Скрипт для задания CVM_API_nightly (обменник; sqlcmd -b), идемпотентный: таблицы — если их нет, процедуры —
-- CREATE OR ALTER, задание — если его нет и история ещё не посчитана. Стоп (ошибка 16, без изменений): нет панели
-- (CVM_CHURN_PANEL.sql не выполнен) или нужных колонок источников.

SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET NOCOUNT ON
SET XACT_ABORT ON
SET LOCK_TIMEOUT 60000
GO

USE mci_model;
GO

-- ============================== 0. СТОПЫ ==============================
IF OBJECT_ID('dbo.I_CVM_CHURN_PANEL') IS NULL OR NOT EXISTS (SELECT 1 FROM dbo.I_CVM_CHURN_PANEL_BATCH (nolock))
BEGIN
    RAISERROR(N'СТОП: панели нет — сначала CVM_CHURN_PANEL.sql. Ничего не создаю.', 16, 1);
    SET NOEXEC ON;
END
GO
IF COL_LENGTH('dbo.I_CHECKHEADER', 'CARD_NUMBER') IS NULL OR COL_LENGTH('dbo.I_CHECKHEADER', 'BONUS_ACCRUAL') IS NULL
   OR COL_LENGTH('dbo.I_CHECKHEADER', 'DAYTYPE') IS NULL OR COL_LENGTH('dbo.I_CHECKHEADER', 'CLOCK') IS NULL
   OR COL_LENGTH('dbo.I_CHECK_RULE', 'ID_CAMPAIGN') IS NULL OR COL_LENGTH('dbo.I_CLIENT_ITEM_DAY', 'ID_CATEGORY_5_EXT') IS NULL
   OR COL_LENGTH('dbo.I_CVM_PRODUCT_CATEGORY', 'ID_CVM_GROUP') IS NULL OR COL_LENGTH('dbo.I_PROMO_DELIVERY_REPORT', 'SEND_DT') IS NULL
   OR COL_LENGTH('dbo.I_CONT_ASSORT_W', 'PENETRATION_CHECK') IS NULL OR COL_LENGTH('dbo.I_SHOP_WEEK', 'COUNT_CHECK') IS NULL
   OR COL_LENGTH('dbo.loyalty__coupons_application_cur', 'transaction_date') IS NULL OR COL_LENGTH('dbo.fav_product', 'ID_PRODUCT') IS NULL
   OR COL_LENGTH('dbo.I_CARD', 'CARD_TYPE') IS NULL OR COL_LENGTH('dbo.I_CONTACT', 'BIRTH_DATE') IS NULL
BEGIN
    RAISERROR(N'СТОП: нет нужных колонок источников — ничего не создаю.', 16, 1);
    SET NOEXEC ON;
END
GO

-- ============================== 1. СЕКЦИИ И ТАБЛИЦЫ ==============================
IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = N'pf_CVM_CHURN_WEEK')
BEGIN
    DECLARE @b nvarchar(max) = N'', @i int = 850;
    WHILE @i <= 1800   -- ревью 1: было 1400 — номера недель подходят к 1400, неделя ≥ 1400 попала бы в одну последнюю секцию, и TRUNCATE одной недели стёр бы все
    BEGIN
        SET @b += IIF(@b = N'', N'', N',') + CAST(@i AS nvarchar(10));
        SET @i += 1;
    END
    EXEC (N'CREATE PARTITION FUNCTION pf_CVM_CHURN_WEEK (int) AS RANGE RIGHT FOR VALUES (' + @b + N')');
END
GO
IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = N'ps_CVM_CHURN_WEEK')
    EXEC (N'CREATE PARTITION SCHEME ps_CVM_CHURN_WEEK AS PARTITION pf_CVM_CHURN_WEEK ALL TO ([PRIMARY])');
GO

IF OBJECT_ID('dbo.I_CVM_CHURN_LOG') IS NULL
    CREATE TABLE dbo.I_CVM_CHURN_LOG (
          ID          int IDENTITY(1,1) NOT NULL PRIMARY KEY
        , STEP        varchar(10)    NOT NULL      -- H0, H1, H2, H2B, H3, H4, H5, C1, HIST
        , WEEKNUMBER  int            NULL          -- неделя слоя или первая неделя пачки срезов
        , WEEK_TO     int            NULL          -- последняя неделя пачки срезов
        , STARTED     datetime2(0)   NOT NULL
        , FINISHED    datetime2(0)   NULL
        , ROWS_       bigint         NULL
        , STATUS      varchar(10)    NOT NULL      -- run / ok / err
        , MSG         nvarchar(2000) NULL
    );
GO

IF OBJECT_ID('dbo.I_CVM_CHURN_FIRST') IS NULL
BEGIN
    CREATE TABLE dbo.I_CVM_CHURN_FIRST (
          ID_COMPANY      int      NOT NULL
        , ID_ORGANIZATION tinyint  NOT NULL
        , ID_CONTACT      int      NOT NULL
        , FIRST_DATA      date     NOT NULL
    );
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_CVM_CHURN_FIRST ON dbo.I_CVM_CHURN_FIRST;
END
GO

IF OBJECT_ID('dbo.I_CVM_CHURN_WEEK') IS NULL
BEGIN
    CREATE TABLE dbo.I_CVM_CHURN_WEEK (
          WEEKNUMBER       int            NOT NULL
        , ID_COMPANY       int            NOT NULL
        , ID_ORGANIZATION  tinyint        NOT NULL
        , ID_CONTACT       int            NOT NULL
        , CHECKS           int            NOT NULL
        , DAY_MASK         tinyint        NOT NULL   -- бит 0 — понедельник … бит 6 — воскресенье: дни с покупкой
        , DAYS             tinyint        NOT NULL
        , DAYS_GT5         tinyint        NOT NULL   -- дней с > 5 чеками
        , SPEND            decimal(14,2)  NULL       -- SUM(COST_DISCOUNT), как CVM_CONTACT
        , SPEND_GROSS      decimal(14,2)  NULL       -- SUM(COST) — до скидки
        , DISCOUNT         decimal(14,2)  NULL       -- SUM(DISCOUNT)
        , SKU              int            NULL
        , MAX_CHECK        decimal(14,2)  NULL
        , WE_CHECKS        int            NOT NULL   -- DAYTYPE <> 1 (выходные и праздники), как CVM_CONTACT
        , WE_SPEND         decimal(14,2)  NULL
        , MORN_CHECKS      int            NOT NULL   -- CLOCK < 12
        , EVEN_CHECKS      int            NOT NULL   -- CLOCK >= 18
        , SHOPS            smallint       NOT NULL
        , MAIN_SHOP        int            NULL       -- больше всего чеков за неделю; равенство — траты, затем меньший ID_SHOP
        , MAIN_SHOP_CHECKS int            NULL
        , BONUS_ACCR       decimal(14,2)  NULL
        , BONUS_PAY        decimal(14,2)  NULL       -- как в чеке (списание — отрицательное)
        , VCARD_CHECKS     int            NOT NULL   -- чеки по виртуальной карте (I_CARD.CARD_TYPE DIXY_Virtual…) — приложение
        , PP5_CHECKS       int            NULL       -- чеки с правилом кампании 5ПП; NULL — неделя раньше начала расчёта правил
        , CVM_CHECKS       int            NULL       -- чеки с правилом кампании акции CVM
        , LOY_CHECKS       int            NULL       -- чеки с правилом акции лояльности (I_LOYALTY_PROMO)
    ) ON ps_CVM_CHURN_WEEK (WEEKNUMBER);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_CVM_CHURN_WEEK ON dbo.I_CVM_CHURN_WEEK ON ps_CVM_CHURN_WEEK (WEEKNUMBER);
END
GO

IF OBJECT_ID('dbo.I_CVM_CHURN_CAT_W') IS NULL
BEGIN
    CREATE TABLE dbo.I_CVM_CHURN_CAT_W (
          WEEKNUMBER       int            NOT NULL
        , ID_COMPANY       int            NOT NULL
        , ID_ORGANIZATION  tinyint        NOT NULL
        , ID_CONTACT       int            NOT NULL
        , ID_CVM_CATEGORY  int            NOT NULL   -- −1 табак, −2 энергетики, 0 — товара нет в справочнике
        , SPEND            decimal(14,2)  NULL
        , QTY              decimal(14,3)  NULL
        , CHECKS           int            NOT NULL
        , MAP_DT           date           NULL       -- MAX(DATE_UPDATE) справочника I_CVM_PRODUCT_CATEGORY на момент расчёта
    ) ON ps_CVM_CHURN_WEEK (WEEKNUMBER);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_CVM_CHURN_CAT_W ON dbo.I_CVM_CHURN_CAT_W ON ps_CVM_CHURN_WEEK (WEEKNUMBER);
END
GO

IF OBJECT_ID('dbo.I_CVM_CHURN_CAT_W_ALL') IS NULL
BEGIN
    CREATE TABLE dbo.I_CVM_CHURN_CAT_W_ALL (
          WEEKNUMBER       int            NOT NULL
        , ID_COMPANY       int            NOT NULL
        , ID_ORGANIZATION  tinyint        NOT NULL
        , ID_CONTACT       int            NOT NULL
        , ID_CVM_CATEGORY  int            NOT NULL
        , SPEND            decimal(14,2)  NULL
        , QTY              decimal(14,3)  NULL
        , CHECKS           int            NOT NULL
        , MAP_DT           date           NULL
    ) ON ps_CVM_CHURN_WEEK (WEEKNUMBER);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_CVM_CHURN_CAT_W_ALL ON dbo.I_CVM_CHURN_CAT_W_ALL ON ps_CVM_CHURN_WEEK (WEEKNUMBER);
END
GO

IF OBJECT_ID('dbo.I_CVM_CHURN_ITEM_W') IS NULL
BEGIN
    CREATE TABLE dbo.I_CVM_CHURN_ITEM_W (
          WEEKNUMBER       int            NOT NULL
        , ID_COMPANY       int            NOT NULL
        , ID_ORGANIZATION  tinyint        NOT NULL
        , ID_CONTACT       int            NOT NULL
        , ID_PRODUCT       bigint         NOT NULL
        , SPEND            decimal(14,2)  NULL
        , QTY              decimal(14,3)  NULL
        , CHECKS           int            NOT NULL
    ) ON ps_CVM_CHURN_WEEK (WEEKNUMBER);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_CVM_CHURN_ITEM_W ON dbo.I_CVM_CHURN_ITEM_W ON ps_CVM_CHURN_WEEK (WEEKNUMBER);
END
GO

IF OBJECT_ID('dbo.I_CVM_CHURN_COMM_W') IS NULL
BEGIN
    CREATE TABLE dbo.I_CVM_CHURN_COMM_W (
          WEEKNUMBER   int  NOT NULL
        , ID_COMPANY   int  NOT NULL
        , ID_CONTACT   int  NOT NULL
        , PUSH_MSG     int  NOT NULL   -- сообщение = клиент × время отправки (строки бывают задвоены; статус у сообщения последний)
        , PUSH_DELIV   int  NOT NULL   -- «Доставлено» + «Переход по ссылке» (переход заменяет доставку)
        , PUSH_CLICK   int  NOT NULL
        , PUSH_ERR     int  NOT NULL   -- «Ошибка» (токен потерян)
        , EMAIL_MSG    int  NOT NULL
        , EMAIL_DELIV  int  NOT NULL   -- «Доставлено», «Открыто», «Переход по ссылке», «Отписка», «Спам»
        , EMAIL_OPEN   int  NOT NULL   -- «Открыто» + «Переход по ссылке»
        , EMAIL_CLICK  int  NOT NULL
        , EMAIL_UNSUB  int  NOT NULL   -- «Отписка» + «Спам»
        , SMS_MSG      int  NOT NULL
        , SMS_DELIV    int  NOT NULL
    ) ON ps_CVM_CHURN_WEEK (WEEKNUMBER);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_CVM_CHURN_COMM_W ON dbo.I_CVM_CHURN_COMM_W ON ps_CVM_CHURN_WEEK (WEEKNUMBER);
END
GO

IF OBJECT_ID('dbo.I_CVM_CHURN_EVENT') IS NULL
BEGIN
    CREATE TABLE dbo.I_CVM_CHURN_EVENT (
          ID_COMPANY      int      NOT NULL
        , ID_CONTACT      int      NOT NULL
        , EV_TYPE         tinyint  NOT NULL   -- 1 акция CVM (TYPE_PROMO CVM, номера 5xxxxx), 2 — 5ПП, 4 выбор любимых категорий
                                              -- (QTY — сколько), 5 выбор любимого товара (ID_OBJ — товар), 6 гашение купона
        , ID_ORGANIZATION tinyint  NULL       -- для акций
        , ID_OBJ          bigint   NULL       -- акция / товар / выпуск купонов
        , CG              tinyint  NULL       -- для акций: 1 — контрольная группа
        , DATE_FROM       date     NOT NULL
        , DATE_TO         date     NULL
        , QTY             int      NULL
    );
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_CVM_CHURN_EVENT ON dbo.I_CVM_CHURN_EVENT;
END
GO

-- С1: статические колонки + группы категорий CVM (по 4 колонки на группу: G<группа>_L4, _L6 — траты за 4 и 6 нед. до
-- среза; _BM, _BSD — среднее и разброс недельных трат за 26 нед. до последних 6). Набор групп — справочник на момент
-- создания; новая группа после этого в срезы не попадёт (отметка в журнале).
IF OBJECT_ID('dbo.I_CVM_CHURN_SLICE') IS NULL
BEGIN
    DECLARE @g nvarchar(max);
    SELECT @g = STRING_AGG(CAST(CONCAT(N', G', ID_CVM_GROUP, N'_L4 decimal(14,2) NULL, G', ID_CVM_GROUP, N'_L6 decimal(14,2) NULL, G',
                                       ID_CVM_GROUP, N'_BM float NULL, G', ID_CVM_GROUP, N'_BSD float NULL') AS nvarchar(max)), N'') WITHIN GROUP (ORDER BY ID_CVM_GROUP)
    FROM (SELECT DISTINCT ID_CVM_GROUP FROM dbo.I_CVM_PRODUCT_CATEGORY (nolock) WHERE ID_CVM_GROUP > 0) AS g;
    SET @g = isnull(@g, N'');

    EXEC (N'CREATE TABLE dbo.I_CVM_CHURN_SLICE (
          WEEKNUMBER int NOT NULL, SLICE_DATE date NOT NULL, ID_COMPANY int NOT NULL, ID_ORGANIZATION tinyint NOT NULL
        , ID_CONTACT int NOT NULL
        , SEG_D tinyint NULL, SEG_D6 tinyint NULL, LABELED bit NOT NULL
        , Y_CHECKS_6 int NULL, Y_DAYS_6 smallint NULL, Y_SPEND_6 decimal(14,2) NULL, Y_FIRST_DAYS smallint NULL
        , Y_OTH_CHECKS_6 int NULL
        , REC_DAYS smallint NULL, TENURE_DAYS int NULL, TENURE_CO_DAYS int NULL, REG_DAYS int NULL, AGE tinyint NULL, SEX tinyint NULL
        , GAP_N smallint NULL, GAP_MEAN float NULL, GAP_SD float NULL, GAP_MAX smallint NULL
        , CHECKS_4 int NULL, DAYS_4 smallint NULL, SPEND_4 decimal(14,2) NULL, GROSS_4 decimal(14,2) NULL, DISC_4 decimal(14,2) NULL, SKU_4 int NULL
        , CHECKS_6 int NULL, DAYS_6 smallint NULL, SPEND_6 decimal(14,2) NULL, GROSS_6 decimal(14,2) NULL, DISC_6 decimal(14,2) NULL, SKU_6 int NULL
        , CHECKS_8 int NULL, DAYS_8 smallint NULL, SPEND_8 decimal(14,2) NULL, GROSS_8 decimal(14,2) NULL, DISC_8 decimal(14,2) NULL, SKU_8 int NULL
        , CHECKS_13 int NULL, DAYS_13 smallint NULL, SPEND_13 decimal(14,2) NULL, GROSS_13 decimal(14,2) NULL, DISC_13 decimal(14,2) NULL, SKU_13 int NULL
        , CHECKS_26 int NULL, DAYS_26 smallint NULL, SPEND_26 decimal(14,2) NULL, GROSS_26 decimal(14,2) NULL, DISC_26 decimal(14,2) NULL, SKU_26 int NULL
        , CHECKS_52 int NULL, DAYS_52 smallint NULL, SPEND_52 decimal(14,2) NULL, GROSS_52 decimal(14,2) NULL, DISC_52 decimal(14,2) NULL, SKU_52 int NULL
        , WEEKS_26 tinyint NULL, WEEKS_52 tinyint NULL, MAX_CHECK_26 decimal(14,2) NULL, SPEND_SD_26 float NULL
        , B_WEEKS tinyint NULL, B_SPEND_MEAN float NULL, B_SPEND_SD float NULL, B_DAYS_MEAN float NULL, B_DAYS_SD float NULL
        , B_CHECK_MEAN float NULL, B_CHECK_SD float NULL, B_SKUCHK_MEAN float NULL, B_SKUCHK_SD float NULL
        , B_PRICE_MEAN float NULL, B_PRICE_SD float NULL, B_DSHARE_MEAN float NULL, B_DSHARE_SD float NULL
        , WE_CHECKS_26 int NULL, WE_SPEND_26 decimal(14,2) NULL, MORN_CHECKS_26 int NULL, EVEN_CHECKS_26 int NULL
        , GT5_DAYS_52 smallint NULL, VCARD_CHECKS_52 int NULL, BONUS_ACCR_13 decimal(14,2) NULL, BONUS_PAY_13 decimal(14,2) NULL
        , AVG_SHOPS_26 float NULL, MAIN_SHOP int NULL, MAIN_SHARE_26 float NULL, MAIN_SHOPS_26 smallint NULL, MAIN_CHANGED bit NULL
        , SHOP_T4 float NULL, SHOP_T13 float NULL, SHOP_CLOSED bit NULL
        , OTH_CHECKS_4 int NULL, OTH_CHECKS_26 int NULL, OTH_SPEND_26 decimal(14,2) NULL
        , RULES_OK bit NULL, PP5_CHECKS_13 int NULL, PP5_CHECKS_26 int NULL, CVM_CHECKS_13 int NULL, CVM_CHECKS_26 int NULL
        , LOY_CHECKS_13 int NULL, LOY_CHECKS_26 int NULL
        , PEN_4 float NULL, PEN_P13 float NULL, POP_4 float NULL, POP_P13 float NULL, PIDX_4 float NULL, PIDX_P13 float NULL, USKU_4 float NULL
        , COMM_OK bit NULL, PUSH_MSG_13 int NULL, PUSH_DELIV_4 int NULL, PUSH_DELIV_13 int NULL, PUSH_DELIV_26 int NULL
        , PUSH_CLICK_4 int NULL, PUSH_CLICK_6 int NULL, PUSH_CLICK_13 int NULL, PUSH_CLICK_26 int NULL, PUSH_ERR_13 int NULL
        , B_CLICK_MEAN float NULL, B_CLICK_SD float NULL
        , EMAIL_DELIV_13 int NULL, EMAIL_OPEN_13 int NULL, EMAIL_CLICK_13 int NULL, EMAIL_UNSUB_26 int NULL, SMS_DELIV_13 int NULL
        , CVM_TG_26 int NULL, CVM_CG_26 int NULL, CVM_RESP_26 int NULL, CVM_TG_LAST_DAYS int NULL, PP5_TG_26 int NULL
        , FAV_CAT_N int NULL, FAV_CAT_DAYS int NULL, FAV_PROD_N int NULL, FAV_PROD_DAYS int NULL
        , FAV_PROD_BUY_4 int NULL, FAV_PROD_BUY_13 int NULL, COUPON_13 int NULL, COUPON_26 int NULL
        , CAT_NORM_N smallint NULL, CAT_ACTIVE_6 smallint NULL, CAT_DROP_N smallint NULL, CAT_DROP_SHARE float NULL
        , CAT_MAXDROP int NULL, CAT_MAXDROP_L6 decimal(14,2) NULL, CAT_MAXDROP_BM float NULL, CAT_MAXDROP_BSD float NULL, CAT_MAXDROP_BW tinyint NULL'
        + @g + N'
    ) ON ps_CVM_CHURN_WEEK (WEEKNUMBER)');
    EXEC (N'CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_CVM_CHURN_SLICE ON dbo.I_CVM_CHURN_SLICE ON ps_CVM_CHURN_WEEK (WEEKNUMBER)');
END
GO

-- ============================== 2. ОЧИСТКА СЕКЦИЙ ==============================
CREATE OR ALTER PROCEDURE dbo.CVM_CHURN_TRUNC @table sysname, @w1 int, @w2 int
AS
BEGIN
    SET NOCOUNT ON;
    IF @table NOT IN (N'I_CVM_CHURN_WEEK', N'I_CVM_CHURN_CAT_W', N'I_CVM_CHURN_CAT_W_ALL', N'I_CVM_CHURN_ITEM_W',
                      N'I_CVM_CHURN_COMM_W', N'I_CVM_CHURN_SLICE')
        THROW 50050, N'CVM_CHURN_TRUNC: таблица не из витрины оттока', 1;
    DECLARE @p1 int = $PARTITION.pf_CVM_CHURN_WEEK(@w1), @p2 int = $PARTITION.pf_CVM_CHURN_WEEK(@w2);
    DECLARE @sql nvarchar(400) = N'TRUNCATE TABLE dbo.' + QUOTENAME(@table) + N' WITH (PARTITIONS ('
                               + CAST(@p1 AS nvarchar(10)) + IIF(@p2 > @p1, N' TO ' + CAST(@p2 AS nvarchar(10)), N'') + N'))';
    EXEC (@sql);
END
GO

-- ============================== 3. СЛОЙ НЕДЕЛИ: Н1, Н2а/Н2б, Н3, Н4 ==============================
-- @parts: 'W' — Н1, 'C' — Н2а + Н3 (+ Н2б при @all_cat = 1), 'M' — Н4. @rules = 0 — без I_CHECK_RULE (недели раньше
-- 25.03.2024: колонки правил NULL).
CREATE OR ALTER PROCEDURE dbo.CVM_CHURN_WEEK_LAYER
      @w        int
    , @parts    varchar(5) = 'WCM'
    , @rules    bit = 1
    , @all_cat  bit = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @idc int = 1, @ws date, @we date, @t0 datetime2(0), @n bigint, @lid int;
    SELECT @ws = min([DATE]), @we = max([DATE]) FROM dbo.I_CALENDAR (nolock) WHERE WEEKNUMBER = @w;
    IF @ws IS NULL OR datediff(day, @ws, @we) <> 6
        THROW 50051, N'CVM_CHURN_WEEK_LAYER: неделя не найдена в I_CALENDAR или не из 7 дней', 1;

    DROP TABLE IF EXISTS #pn;
    SELECT DISTINCT ID_CONTACT INTO #pn FROM dbo.I_CVM_CHURN_PANEL (nolock) WHERE ID_COMPANY = @idc;
    CREATE CLUSTERED COLUMNSTORE INDEX cci_pn ON #pn;

    -- ---------- Н1 ----------
    IF @parts LIKE '%W%'
    BEGIN
        SET @t0 = sysdatetime();
        INSERT dbo.I_CVM_CHURN_LOG (STEP, WEEKNUMBER, STARTED, STATUS) VALUES ('H1', @w, @t0, 'run');
        SET @lid = SCOPE_IDENTITY();

        DROP TABLE IF EXISTS #vc;
        SELECT DISTINCT CARD_NUMBER = TRY_CAST(CARD_NUMBER AS bigint) INTO #vc FROM dbo.I_CARD (nolock)   -- ревью 1: TRY_CAST и тут — нечисловой номер не должен ронять неявное преобразование
        WHERE ID_COMPANY = @idc AND CARD_TYPE LIKE N'DIXY[_]Virtual%' AND TRY_CAST(CARD_NUMBER AS bigint) IS NOT NULL;
        CREATE CLUSTERED INDEX ci_vc ON #vc (CARD_NUMBER);

        DROP TABLE IF EXISTS #chk;
        SELECT h.ID_CHECK, h.ID_CONTACT, ID_ORGANIZATION = CAST(h.ID_ORGANIZATION AS tinyint), h.DATA, h.ID_SHOP
             , h.CLOCK, h.DAYTYPE, h.COST_DISCOUNT, h.COST, h.DISCOUNT, h.SKU, h.BONUS_ACCRUAL, h.BONUS_PAY
             , VC = CASE WHEN v.CARD_NUMBER IS NOT NULL THEN 1 ELSE 0 END
        INTO #chk
        FROM dbo.I_CHECKHEADER AS h (nolock)
             LEFT JOIN #vc AS v ON v.CARD_NUMBER = TRY_CAST(h.CARD_NUMBER AS bigint)
        WHERE h.ID_COMPANY = @idc AND h.ID_ORGANIZATION IN (1, 3) AND h.ID_CONTACT <> 0
          AND h.DATA >= @ws AND h.DATA <= @we
        OPTION (RECOMPILE);
        CREATE CLUSTERED COLUMNSTORE INDEX cci_chk ON #chk;

        -- правила чеков: I_CHECK_RULE без даты — диапазоны ID_CHECK по нодам (ID_CHECK / 2^42), как discount_mart
        DROP TABLE IF EXISTS #rr;
        CREATE TABLE #rr (ID_CHECK bigint NOT NULL, PP5 tinyint NOT NULL, CVM tinyint NOT NULL, LOY tinyint NOT NULL);
        IF @rules = 1
        BEGIN
            -- кампании: акции лояльности, наши CVM (I_PROMO.ID_CAMPAIGN; кампании «<номер>_…» / «<номер>. …»; старые CVM из
            -- I_PROMO_all), 5ПП (6ПП — без награды, не считаем; Елена 10.10.2026) — как CVM_CLUSTERS_VITRINA
            DROP TABLE IF EXISTS #camp;
            SELECT ID_CAMPAIGN, IS_LOY = max(IS_LOY), IS_CVM = max(IS_CVM), IS_PP = max(IS_PP)
            INTO #camp
            FROM (SELECT ID_CAMPAIGN, IS_LOY = 1, IS_CVM = 0, IS_PP = 0 FROM dbo.I_LOYALTY_PROMO (nolock)
                  UNION ALL
                  SELECT p.ID_CAMPAIGN, 0, 1, 0 FROM dbo.I_PROMO AS p (nolock)
                  WHERE p.TYPE_PROMO = N'CVM' AND p.ID_COMPANY = @idc AND p.ID_CAMPAIGN IS NOT NULL
                  UNION ALL
                  SELECT p.ID_CAMPAIGN, 0, 0, 1 FROM dbo.I_PROMO AS p (nolock)
                  WHERE p.TYPE_PROMO = N'5ПП' AND p.ID_COMPANY = @idc AND p.ID_CAMPAIGN IS NOT NULL
                  UNION ALL
                  SELECT c.ID_CAMPAIGN, 0, 1, 0
                  FROM dbo.I_CAMPAIGN AS c (nolock)
                       JOIN dbo.I_PROMO AS p (nolock)
                         ON p.TYPE_PROMO = N'CVM' AND p.ID_COMPANY = @idc AND p.ID_PROMO BETWEEN 100000 AND 999999
                        AND c.CAMPAIGN_NAME LIKE CAST(p.ID_PROMO AS nvarchar(12)) + N'[_.]%'
                  WHERE c.ID_COMPANY = @idc
                  UNION ALL
                  SELECT a.ID_PROMO, 0, 1, 0 FROM dbo.I_PROMO_all AS a (nolock)
                  WHERE a.TYPE_PROMO = 'CVM' AND a.ID_COMPANY = @idc AND a.PROMO_NAME LIKE '10[0-9][0-9][0-9][0-9].%') AS x
            WHERE ID_CAMPAIGN IS NOT NULL
            GROUP BY ID_CAMPAIGN;

            DROP TABLE IF EXISTS #rng;
            SELECT RN = row_number() OVER (ORDER BY min(ID_CHECK)), C_MIN = min(ID_CHECK), C_MAX = max(ID_CHECK)
            INTO #rng
            FROM #chk
            GROUP BY ID_CHECK / 4398046511104;

            DECLARE @r int = 1, @rn int = (SELECT count(*) FROM #rng), @a bigint, @z bigint;
            WHILE @r <= @rn
            BEGIN
                SELECT @a = C_MIN, @z = C_MAX FROM #rng WHERE RN = @r;
                INSERT INTO #rr (ID_CHECK, PP5, CVM, LOY)
                SELECT r.ID_CHECK, max(c.IS_PP), max(c.IS_CVM), max(c.IS_LOY)
                FROM dbo.I_CHECK_RULE AS r (nolock)
                     JOIN #camp AS c ON c.ID_CAMPAIGN = r.ID_CAMPAIGN
                WHERE r.ID_COMPANY = @idc AND r.ID_CHECK BETWEEN @a AND @z
                  AND EXISTS (SELECT 1 FROM #chk AS k WHERE k.ID_CHECK = r.ID_CHECK)
                GROUP BY r.ID_CHECK
                OPTION (RECOMPILE, MAXDOP 2);
                SET @r += 1;
            END
            -- чек мог попасть в две ноды-диапазона только при пересечении диапазонов — схлопываем
            DROP TABLE IF EXISTS #rr1;
            SELECT ID_CHECK, PP5 = max(PP5), CVM = max(CVM), LOY = max(LOY) INTO #rr1 FROM #rr GROUP BY ID_CHECK;
            TRUNCATE TABLE #rr;
            INSERT INTO #rr SELECT ID_CHECK, PP5, CVM, LOY FROM #rr1;
        END

        DROP TABLE IF EXISTS #cd;   -- клиент × орг. × день
        SELECT ID_CONTACT, ID_ORGANIZATION, BIT = power(2, datediff(day, @ws, DATA)), N = count(*)
        INTO #cd
        FROM #chk
        GROUP BY ID_CONTACT, ID_ORGANIZATION, DATA;

        DROP TABLE IF EXISTS #cs;   -- клиент × орг. × магазин → основной магазин недели
        SELECT ID_CONTACT, ID_ORGANIZATION, SHOPS = count(*), MAIN_SHOP = max(CASE WHEN RN = 1 THEN ID_SHOP END)
             , MAIN_SHOP_CHECKS = max(CASE WHEN RN = 1 THEN N END)
        INTO #cs
        FROM (SELECT ID_CONTACT, ID_ORGANIZATION, ID_SHOP, N = count(*)
                   , RN = row_number() OVER (PARTITION BY ID_CONTACT, ID_ORGANIZATION ORDER BY count(*) DESC, sum(COST_DISCOUNT) DESC, ID_SHOP)
              FROM #chk
              GROUP BY ID_CONTACT, ID_ORGANIZATION, ID_SHOP) AS s
        GROUP BY ID_CONTACT, ID_ORGANIZATION;

        EXEC dbo.CVM_CHURN_TRUNC N'I_CVM_CHURN_WEEK', @w, @w;
        INSERT INTO dbo.I_CVM_CHURN_WEEK WITH (TABLOCK)
              (WEEKNUMBER, ID_COMPANY, ID_ORGANIZATION, ID_CONTACT, CHECKS, DAY_MASK, DAYS, DAYS_GT5, SPEND, SPEND_GROSS, DISCOUNT,
               SKU, MAX_CHECK, WE_CHECKS, WE_SPEND, MORN_CHECKS, EVEN_CHECKS, SHOPS, MAIN_SHOP, MAIN_SHOP_CHECKS, BONUS_ACCR,
               BONUS_PAY, VCARD_CHECKS, PP5_CHECKS, CVM_CHECKS, LOY_CHECKS)
        SELECT @w, @idc, a.ID_ORGANIZATION, a.ID_CONTACT, a.CHECKS, d.DAY_MASK, d.DAYS, d.DAYS_GT5, a.SPEND, a.SPEND_GROSS,
               a.DISCOUNT, a.SKU, a.MAX_CHECK, a.WE_CHECKS, a.WE_SPEND, a.MORN_CHECKS, a.EVEN_CHECKS, s.SHOPS, s.MAIN_SHOP,
               s.MAIN_SHOP_CHECKS, a.BONUS_ACCR, a.BONUS_PAY, a.VCARD_CHECKS,
               IIF(@rules = 1, a.PP5_CHECKS, NULL), IIF(@rules = 1, a.CVM_CHECKS, NULL), IIF(@rules = 1, a.LOY_CHECKS, NULL)
        FROM (SELECT k.ID_CONTACT, k.ID_ORGANIZATION
                   , CHECKS      = count(*)
                   , SPEND       = sum(k.COST_DISCOUNT), SPEND_GROSS = sum(k.COST), DISCOUNT = sum(k.DISCOUNT), SKU = sum(k.SKU)
                   , MAX_CHECK   = max(k.COST_DISCOUNT)
                   , WE_CHECKS   = sum(CASE WHEN k.DAYTYPE <> 1 THEN 1 ELSE 0 END)
                   , WE_SPEND    = sum(CASE WHEN k.DAYTYPE <> 1 THEN k.COST_DISCOUNT ELSE 0 END)
                   , MORN_CHECKS = sum(CASE WHEN k.CLOCK < 12 THEN 1 ELSE 0 END)
                   , EVEN_CHECKS = sum(CASE WHEN k.CLOCK >= 18 THEN 1 ELSE 0 END)
                   , BONUS_ACCR  = sum(k.BONUS_ACCRUAL), BONUS_PAY = sum(k.BONUS_PAY)
                   , VCARD_CHECKS = sum(k.VC)
                   , PP5_CHECKS  = sum(CAST(isnull(r.PP5, 0) AS int))
                   , CVM_CHECKS  = sum(CAST(isnull(r.CVM, 0) AS int))
                   , LOY_CHECKS  = sum(CAST(isnull(r.LOY, 0) AS int))
              FROM #chk AS k
                   LEFT JOIN #rr AS r ON r.ID_CHECK = k.ID_CHECK
              GROUP BY k.ID_CONTACT, k.ID_ORGANIZATION) AS a
             JOIN (SELECT ID_CONTACT, ID_ORGANIZATION, DAY_MASK = CAST(sum(BIT) AS tinyint), DAYS = CAST(count(*) AS tinyint)
                        , DAYS_GT5 = CAST(sum(CASE WHEN N > 5 THEN 1 ELSE 0 END) AS tinyint)
                   FROM #cd GROUP BY ID_CONTACT, ID_ORGANIZATION) AS d
               ON d.ID_CONTACT = a.ID_CONTACT AND d.ID_ORGANIZATION = a.ID_ORGANIZATION
             JOIN #cs AS s ON s.ID_CONTACT = a.ID_CONTACT AND s.ID_ORGANIZATION = a.ID_ORGANIZATION;
        SET @n = @@ROWCOUNT;
        UPDATE dbo.I_CVM_CHURN_LOG SET FINISHED = sysdatetime(), ROWS_ = @n, STATUS = 'ok',
               MSG = CONCAT(N'чеков ', (SELECT count(*) FROM #chk), N', по виртуальной карте ', (SELECT sum(VC) FROM #chk),
                            N', с правилами ', (SELECT count(*) FROM #rr))
        WHERE ID = @lid;
        DROP TABLE IF EXISTS #chk, #rr, #cd, #cs;
    END

    -- ---------- Н2а / Н2б / Н3 ----------
    IF @parts LIKE '%C%'
    BEGIN
        SET @t0 = sysdatetime();
        INSERT dbo.I_CVM_CHURN_LOG (STEP, WEEKNUMBER, STARTED, STATUS) VALUES (IIF(@all_cat = 1, 'H2B', 'H2'), @w, @t0, 'run');
        SET @lid = SCOPE_IDENTITY();

        DECLARE @map_dt date = (SELECT max(DATE_UPDATE) FROM dbo.I_CVM_PRODUCT_CATEGORY (nolock));
        DROP TABLE IF EXISTS #map;
        SELECT ID_PRODUCT, ID_CVM_CATEGORY = max(ID_CVM_CATEGORY)
        INTO #map
        FROM dbo.I_CVM_PRODUCT_CATEGORY (nolock)
        WHERE ID_CVM_CATEGORY IS NOT NULL
        GROUP BY ID_PRODUCT;
        CREATE CLUSTERED INDEX ci_map ON #map (ID_PRODUCT);

        -- табак: коды ДИКСИ 610901, 610902, 610904, 610905, 610906*, 70030101 (как CVM_CLUSTER_SCORE / CVM_DISC_MODEL);
        -- энергетики: 61030701, 61110902 (решение 21.10.2026)
        DROP TABLE IF EXISTS #it;
        SELECT i.ID_CONTACT, ID_ORGANIZATION = CAST(i.ID_ORGANIZATION AS tinyint), i.ID_CHECK, i.ID_PRODUCT, i.SUMMA, i.QTY
             , CAT = CASE WHEN LEFT(CAST(i.ID_CATEGORY_5_EXT AS varchar(20)), 6) IN ('610901', '610902', '610904', '610905', '610906')
                            OR i.ID_CATEGORY_5_EXT = 70030101 THEN -1
                          WHEN i.ID_CATEGORY_5_EXT IN (61030701, 61110902) THEN -2
                          ELSE isnull(m.ID_CVM_CATEGORY, 0) END
             , PAN = CASE WHEN p.ID_CONTACT IS NOT NULL THEN 1 ELSE 0 END
        INTO #it
        FROM dbo.I_CLIENT_ITEM_DAY AS i (nolock)
             LEFT JOIN #pn AS p ON p.ID_CONTACT = i.ID_CONTACT
             LEFT JOIN #map AS m ON m.ID_PRODUCT = i.ID_PRODUCT
        WHERE i.DATA >= @ws AND i.DATA <= @we AND i.ID_COMPANY = @idc AND i.ID_ORGANIZATION IN (1, 3) AND i.ID_CONTACT <> 0
          AND (@all_cat = 1 OR p.ID_CONTACT IS NOT NULL)
        OPTION (RECOMPILE);
        CREATE CLUSTERED COLUMNSTORE INDEX cci_it ON #it;

        DROP TABLE IF EXISTS #cat;
        SELECT ID_CONTACT, ID_ORGANIZATION, CAT, PAN = max(PAN), SPEND = sum(SUMMA), QTY = sum(QTY), CHECKS = count(DISTINCT ID_CHECK)
        INTO #cat
        FROM #it
        GROUP BY ID_CONTACT, ID_ORGANIZATION, CAT;

        EXEC dbo.CVM_CHURN_TRUNC N'I_CVM_CHURN_CAT_W', @w, @w;
        INSERT INTO dbo.I_CVM_CHURN_CAT_W WITH (TABLOCK)
              (WEEKNUMBER, ID_COMPANY, ID_ORGANIZATION, ID_CONTACT, ID_CVM_CATEGORY, SPEND, QTY, CHECKS, MAP_DT)
        SELECT @w, @idc, ID_ORGANIZATION, ID_CONTACT, CAT, SPEND, QTY, CHECKS, @map_dt FROM #cat WHERE PAN = 1;
        SET @n = @@ROWCOUNT;

        IF @all_cat = 1
        BEGIN
            EXEC dbo.CVM_CHURN_TRUNC N'I_CVM_CHURN_CAT_W_ALL', @w, @w;
            INSERT INTO dbo.I_CVM_CHURN_CAT_W_ALL WITH (TABLOCK)
                  (WEEKNUMBER, ID_COMPANY, ID_ORGANIZATION, ID_CONTACT, ID_CVM_CATEGORY, SPEND, QTY, CHECKS, MAP_DT)
            SELECT @w, @idc, ID_ORGANIZATION, ID_CONTACT, CAT, SPEND, QTY, CHECKS, @map_dt FROM #cat;
        END

        EXEC dbo.CVM_CHURN_TRUNC N'I_CVM_CHURN_ITEM_W', @w, @w;
        INSERT INTO dbo.I_CVM_CHURN_ITEM_W WITH (TABLOCK)
              (WEEKNUMBER, ID_COMPANY, ID_ORGANIZATION, ID_CONTACT, ID_PRODUCT, SPEND, QTY, CHECKS)
        SELECT @w, @idc, ID_ORGANIZATION, ID_CONTACT, ID_PRODUCT, sum(SUMMA), sum(QTY), count(DISTINCT ID_CHECK)   -- ревью 1: как в Н2а; count(*) считал бы строки клиент × товар, если товар в чеке встречается несколькими строками
        FROM #it
        WHERE PAN = 1 AND CAT >= 0
        GROUP BY ID_ORGANIZATION, ID_CONTACT, ID_PRODUCT;

        UPDATE dbo.I_CVM_CHURN_LOG SET FINISHED = sysdatetime(), ROWS_ = @n, STATUS = 'ok',
               MSG = CONCAT(N'строк покупок ', (SELECT count(*) FROM #it), N', категорий всех клиентов ', IIF(@all_cat = 1, (SELECT count(*) FROM #cat), NULL),
                            N', товаров панели ', @@ROWCOUNT)
        WHERE ID = @lid;
        DROP TABLE IF EXISTS #it, #cat;
    END

    -- ---------- Н4 ----------
    IF @parts LIKE '%M%'
    BEGIN
        SET @t0 = sysdatetime();
        INSERT dbo.I_CVM_CHURN_LOG (STEP, WEEKNUMBER, STARTED, STATUS) VALUES ('H4', @w, @t0, 'run');
        SET @lid = SCOPE_IDENTITY();

        EXEC dbo.CVM_CHURN_TRUNC N'I_CVM_CHURN_COMM_W', @w, @w;
        INSERT INTO dbo.I_CVM_CHURN_COMM_W WITH (TABLOCK)
              (WEEKNUMBER, ID_COMPANY, ID_CONTACT, PUSH_MSG, PUSH_DELIV, PUSH_CLICK, PUSH_ERR, EMAIL_MSG, EMAIL_DELIV, EMAIL_OPEN,
               EMAIL_CLICK, EMAIL_UNSUB, SMS_MSG, SMS_DELIV)
        SELECT @w, @idc, m.ID_CONTACT
             , sum(CASE WHEN m.CH = 1 THEN 1 ELSE 0 END)
             , sum(CASE WHEN m.CH = 1 AND (m.S_DLV = 1 OR m.S_CLK = 1) THEN 1 ELSE 0 END)
             , sum(CASE WHEN m.CH = 1 AND m.S_CLK = 1 THEN 1 ELSE 0 END)
             , sum(CASE WHEN m.CH = 1 AND m.S_ERR = 1 THEN 1 ELSE 0 END)
             , sum(CASE WHEN m.CH = 3 THEN 1 ELSE 0 END)
             , sum(CASE WHEN m.CH = 3 AND (m.S_DLV = 1 OR m.S_OPN = 1 OR m.S_CLK = 1 OR m.S_UNS = 1) THEN 1 ELSE 0 END)
             , sum(CASE WHEN m.CH = 3 AND (m.S_OPN = 1 OR m.S_CLK = 1) THEN 1 ELSE 0 END)
             , sum(CASE WHEN m.CH = 3 AND m.S_CLK = 1 THEN 1 ELSE 0 END)
             , sum(CASE WHEN m.CH = 3 AND m.S_UNS = 1 THEN 1 ELSE 0 END)
             , sum(CASE WHEN m.CH = 2 THEN 1 ELSE 0 END)
             , sum(CASE WHEN m.CH = 2 AND m.S_DLV = 1 THEN 1 ELSE 0 END)
        FROM (SELECT d.ID_CONTACT, CH = d.ID_CHANNEL, d.SEND_DT
                   , S_DLV = max(CASE WHEN d.STATUS = N'Доставлено' THEN 1 ELSE 0 END)
                   , S_CLK = max(CASE WHEN d.STATUS = N'Переход по ссылке' THEN 1 ELSE 0 END)
                   , S_ERR = max(CASE WHEN d.STATUS = N'Ошибка' THEN 1 ELSE 0 END)
                   , S_OPN = max(CASE WHEN d.STATUS = N'Открыто' THEN 1 ELSE 0 END)
                   , S_UNS = max(CASE WHEN d.STATUS IN (N'Отписка', N'Спам') THEN 1 ELSE 0 END)
              FROM dbo.I_PROMO_DELIVERY_REPORT AS d (nolock)
              WHERE d.ID_COMPANY = @idc AND d.ID_CHANNEL IN (1, 2, 3) AND d.ID_CONTACT <> 0
                AND d.SEND_DT >= CAST(@ws AS datetime) AND d.SEND_DT < dateadd(day, 1, CAST(@we AS datetime))
              GROUP BY d.ID_CONTACT, d.ID_CHANNEL, d.SEND_DT) AS m
        GROUP BY m.ID_CONTACT
        OPTION (RECOMPILE);
        SET @n = @@ROWCOUNT;
        UPDATE dbo.I_CVM_CHURN_LOG SET FINISHED = sysdatetime(), ROWS_ = @n, STATUS = 'ok' WHERE ID = @lid;
    END
END
GO

-- ============================== 4. Н0 ПЕРВАЯ ПОКУПКА ==============================
-- @mode 'full' — один проход по всей I_CHECKHEADER; 'inc' — новые клиент × орг. из Н1 с недели @w_from.
CREATE OR ALTER PROCEDURE dbo.CVM_CHURN_FIRST_BUILD @mode varchar(4) = 'inc', @w_from int = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @idc int = 1, @t0 datetime2(0) = sysdatetime(), @n bigint, @lid int;
    INSERT dbo.I_CVM_CHURN_LOG (STEP, WEEKNUMBER, STARTED, STATUS, MSG) VALUES ('H0', @w_from, @t0, 'run', @mode);
    SET @lid = SCOPE_IDENTITY();

    IF @mode = 'full'
    BEGIN
        DROP TABLE IF EXISTS #f;
        SELECT ID_ORGANIZATION = CAST(ID_ORGANIZATION AS tinyint), ID_CONTACT, FIRST_DATA = min(DATA)
        INTO #f
        FROM dbo.I_CHECKHEADER (nolock)
        WHERE ID_COMPANY = @idc AND ID_ORGANIZATION IN (1, 3) AND ID_CONTACT <> 0
        GROUP BY ID_ORGANIZATION, ID_CONTACT
        OPTION (MAXDOP 4);
        BEGIN TRANSACTION;
        TRUNCATE TABLE dbo.I_CVM_CHURN_FIRST;
        INSERT INTO dbo.I_CVM_CHURN_FIRST WITH (TABLOCK) (ID_COMPANY, ID_ORGANIZATION, ID_CONTACT, FIRST_DATA)
        SELECT @idc, ID_ORGANIZATION, ID_CONTACT, FIRST_DATA FROM #f;
        SET @n = @@ROWCOUNT;
        COMMIT TRANSACTION;
    END
    ELSE
    BEGIN
        -- первый день покупки в самой ранней новой неделе: младший бит маски дней
        INSERT INTO dbo.I_CVM_CHURN_FIRST (ID_COMPANY, ID_ORGANIZATION, ID_CONTACT, FIRST_DATA)
        SELECT @idc, x.ID_ORGANIZATION, x.ID_CONTACT
             , dateadd(day, CASE WHEN x.DAY_MASK & 1 > 0 THEN 0 WHEN x.DAY_MASK & 2 > 0 THEN 1 WHEN x.DAY_MASK & 4 > 0 THEN 2
                                 WHEN x.DAY_MASK & 8 > 0 THEN 3 WHEN x.DAY_MASK & 16 > 0 THEN 4 WHEN x.DAY_MASK & 32 > 0 THEN 5
                                 ELSE 6 END, c.WS)
        FROM (SELECT w.ID_ORGANIZATION, w.ID_CONTACT, w.WEEKNUMBER, w.DAY_MASK
                   , RN = row_number() OVER (PARTITION BY w.ID_ORGANIZATION, w.ID_CONTACT ORDER BY w.WEEKNUMBER)
              FROM dbo.I_CVM_CHURN_WEEK AS w (nolock)
              WHERE w.WEEKNUMBER >= @w_from
                AND NOT EXISTS (SELECT 1 FROM dbo.I_CVM_CHURN_FIRST AS f   -- целевая таблица INSERT: без (nolock) (ошибка 1065)
                                WHERE f.ID_CONTACT = w.ID_CONTACT AND f.ID_ORGANIZATION = w.ID_ORGANIZATION)) AS x
             JOIN (SELECT WEEKNUMBER, WS = min([DATE]) FROM dbo.I_CALENDAR (nolock) WHERE WEEKNUMBER >= @w_from GROUP BY WEEKNUMBER) AS c
               ON c.WEEKNUMBER = x.WEEKNUMBER
        WHERE x.RN = 1;
        SET @n = @@ROWCOUNT;
    END
    UPDATE dbo.I_CVM_CHURN_LOG SET FINISHED = sysdatetime(), ROWS_ = @n, STATUS = 'ok' WHERE ID = @lid;
END
GO

-- ============================== 5. Н5 СОБЫТИЯ ПАНЕЛИ ==============================
CREATE OR ALTER PROCEDURE dbo.CVM_CHURN_EVENTS_BUILD @from date = '20230801'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @idc int = 1, @t0 datetime2(0) = sysdatetime(), @n bigint, @lid int;
    INSERT dbo.I_CVM_CHURN_LOG (STEP, STARTED, STATUS) VALUES ('H5', @t0, 'run');
    SET @lid = SCOPE_IDENTITY();

    DROP TABLE IF EXISTS #pn;
    SELECT DISTINCT ID_CONTACT INTO #pn FROM dbo.I_CVM_CHURN_PANEL (nolock) WHERE ID_COMPANY = @idc;
    CREATE CLUSTERED INDEX ci_pn ON #pn (ID_CONTACT);

    DROP TABLE IF EXISTS #pr;   -- акции CVM (тип CVM, номера 5xxxxx портала) и 5ПП с датами
    SELECT ID_PROMO, EV_TYPE = CAST(max(CASE WHEN TYPE_PROMO = N'5ПП' THEN 2 ELSE 1 END) AS tinyint)
         , START_DATE = min(START_DATE), FINISH_DATE = max(FINISH_DATE)
    INTO #pr
    FROM dbo.I_PROMO (nolock)
    WHERE ID_COMPANY = @idc AND (TYPE_PROMO IN (N'CVM', N'5ПП') OR ID_PROMO >= 500000) AND START_DATE >= @from
    GROUP BY ID_PROMO;
    CREATE CLUSTERED INDEX ci_pr ON #pr (ID_PROMO);

    DROP TABLE IF EXISTS #ev;
    CREATE TABLE #ev (ID_CONTACT int NOT NULL, EV_TYPE tinyint NOT NULL, ID_ORGANIZATION tinyint NULL, ID_OBJ bigint NULL,
                      CG tinyint NULL, DATE_FROM date NOT NULL, DATE_TO date NULL, QTY int NULL);

    INSERT INTO #ev WITH (TABLOCK)
    SELECT o.ID_CONTACT, p.EV_TYPE, CAST(o.ID_ORGANIZATION AS tinyint), o.ID_PROMO, CAST(max(o.CONTROL_GROUP) AS tinyint),
           p.START_DATE, p.FINISH_DATE, NULL
    FROM dbo.I_PROMO_OFFER AS o (nolock)
         JOIN #pr AS p ON p.ID_PROMO = o.ID_PROMO
         JOIN #pn AS n ON n.ID_CONTACT = o.ID_CONTACT
    WHERE o.ID_COMPANY = @idc
    GROUP BY o.ID_CONTACT, p.EV_TYPE, o.ID_ORGANIZATION, o.ID_PROMO, p.START_DATE, p.FINISH_DATE;

    INSERT INTO #ev WITH (TABLOCK)
    SELECT f.ID_CONTACT, 4, NULL, NULL, NULL, f.DATA, NULL, count(*)
    FROM dbo.fav_category AS f (nolock)
         JOIN #pn AS n ON n.ID_CONTACT = f.ID_CONTACT
    WHERE f.DATA >= @from
    GROUP BY f.ID_CONTACT, f.DATA;

    INSERT INTO #ev WITH (TABLOCK)
    SELECT f.ID_CONTACT, 5, NULL, f.ID_PRODUCT, NULL, f.DATA, NULL, 1
    FROM dbo.fav_product AS f (nolock)
         JOIN #pn AS n ON n.ID_CONTACT = f.ID_CONTACT
    WHERE f.DATA >= @from
    GROUP BY f.ID_CONTACT, f.ID_PRODUCT, f.DATA;

    -- гашения купонов (CDP; transaction_date — секунды Unix, до 2038 года помещаются в int)
    INSERT INTO #ev WITH (TABLOCK)
    SELECT a.contact_id, 6, NULL, a.coupon_emission_id, NULL, x.D, NULL, count(*)
    FROM dbo.loyalty__coupons_application_cur AS a (nolock)
         JOIN #pn AS n ON n.ID_CONTACT = a.contact_id
         CROSS APPLY (SELECT D = CAST(dateadd(second, TRY_CAST(a.transaction_date AS int), '19700101') AS date)) AS x
    WHERE a.ID_COMPANY = @idc AND a.transaction_date BETWEEN 946684800 AND 2147483647
    GROUP BY a.contact_id, a.coupon_emission_id, x.D;

    BEGIN TRANSACTION;
    TRUNCATE TABLE dbo.I_CVM_CHURN_EVENT;
    INSERT INTO dbo.I_CVM_CHURN_EVENT WITH (TABLOCK) (ID_COMPANY, ID_CONTACT, EV_TYPE, ID_ORGANIZATION, ID_OBJ, CG, DATE_FROM, DATE_TO, QTY)
    SELECT @idc, ID_CONTACT, EV_TYPE, ID_ORGANIZATION, ID_OBJ, CG, DATE_FROM, DATE_TO, QTY FROM #ev;
    SET @n = @@ROWCOUNT;
    COMMIT TRANSACTION;
    UPDATE dbo.I_CVM_CHURN_LOG SET FINISHED = sysdatetime(), ROWS_ = @n, STATUS = 'ok',
           MSG = (SELECT STRING_AGG(CONCAT(N'тип ', EV_TYPE, N': ', N), N', ') FROM (SELECT EV_TYPE, N = count(*) FROM #ev GROUP BY EV_TYPE) AS t)
    WHERE ID = @lid;
END
GO

-- ============================== 6. С1 СРЕЗЫ ПАНЕЛИ ==============================
-- Пачка соседних срезов @s1…@s2 (номера недель; срез — воскресенье недели). Все признаки — по данным до среза включительно;
-- исходы — по неделям среза +1…+6 (только у размеченных: @s + 6 ≤ последней закрытой недели @lcw).
-- Окна: W = неделя среза − неделя слоя; 4 нед. = W 0…3; база для z-скоров = W 6…31 (26 нед. до последних 6).
CREATE OR ALTER PROCEDURE dbo.CVM_CHURN_SLICES @s1 int, @s2 int, @lcw int
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @idc int = 1, @t0 datetime2(0) = sysdatetime(), @n bigint, @lid int;
    INSERT dbo.I_CVM_CHURN_LOG (STEP, WEEKNUMBER, WEEK_TO, STARTED, STATUS) VALUES ('C1', @s1, @s2, @t0, 'run');
    SET @lid = SCOPE_IDENTITY();

    -- срезы пачки и недели
    DROP TABLE IF EXISTS #sl, #wk;
    SELECT SL = WEEKNUMBER, D = max([DATE]), LAB = CAST(IIF(WEEKNUMBER + 6 <= @lcw, 1, 0) AS bit)
    INTO #sl FROM dbo.I_CALENDAR (nolock) WHERE WEEKNUMBER BETWEEN @s1 AND @s2 GROUP BY WEEKNUMBER;
    SELECT WEEKNUMBER, WS = min([DATE]) INTO #wk FROM dbo.I_CALENDAR (nolock)
    WHERE WEEKNUMBER BETWEEN @s1 - 60 AND @s2 + 6 GROUP BY WEEKNUMBER;
    CREATE CLUSTERED INDEX ci_wk ON #wk (WEEKNUMBER);

    -- начала данных правил и рассылок (по журналу слоя): окна раньше — NULL «нет данных»
    DECLARE @rules_w int = (SELECT min(WEEKNUMBER) FROM dbo.I_CVM_CHURN_WEEK (nolock) WHERE PP5_CHECKS IS NOT NULL AND WEEKNUMBER >= @s1 - 60);
    DECLARE @comm_w int  = (SELECT min(WEEKNUMBER) FROM dbo.I_CVM_CHURN_LOG (nolock) WHERE STEP = 'H4' AND STATUS = 'ok');

    DROP TABLE IF EXISTS #pn, #fr;
    SELECT DISTINCT ID_CONTACT INTO #pn FROM dbo.I_CVM_CHURN_PANEL (nolock) WHERE ID_COMPANY = @idc;
    CREATE CLUSTERED COLUMNSTORE INDEX cci_pn ON #pn;
    SELECT ID_CONTACT INTO #fr FROM (SELECT ID_CONTACT FROM dbo.I_FRAUD_CONTACT (nolock) WHERE ID_COMPANY = @idc
                                     UNION SELECT ID_CONTACT FROM dbo.I_MISTAKE_CONTACT (nolock) WHERE ID_COMPANY = @idc) AS x;
    CREATE CLUSTERED INDEX ci_fr ON #fr (ID_CONTACT);

    -- Н1 панели за окно пачки: 58 нед. назад (окно LFL год назад и правило 56 нед.) … 6 нед. вперёд (исходы)
    DROP TABLE IF EXISTS #h;
    SELECT w.*
    INTO #h
    FROM dbo.I_CVM_CHURN_WEEK AS w (nolock)
    WHERE w.WEEKNUMBER BETWEEN @s1 - 58 AND @s2 + 6
      AND EXISTS (SELECT 1 FROM #pn AS p WHERE p.ID_CONTACT = w.ID_CONTACT)
    OPTION (RECOMPILE);
    CREATE CLUSTERED COLUMNSTORE INDEX cci_h ON #h;

    -- население: клиент панели × орг. с покупкой за 6 нед. до среза, без фрода
    DROP TABLE IF EXISTS #pop;
    SELECT s.SL, s.D, s.LAB, h.ID_CONTACT, h.ID_ORGANIZATION
    INTO #pop
    FROM #sl AS s
         JOIN #h AS h ON h.WEEKNUMBER BETWEEN s.SL - 5 AND s.SL
    WHERE NOT EXISTS (SELECT 1 FROM #fr AS f WHERE f.ID_CONTACT = h.ID_CONTACT)
    GROUP BY s.SL, s.D, s.LAB, h.ID_CONTACT, h.ID_ORGANIZATION;
    CREATE CLUSTERED COLUMNSTORE INDEX cci_pop ON #pop;

    -- дни покупок (из масок) и интервал до прошлого дня покупки в той же организации
    DROP TABLE IF EXISTS #days;
    SELECT h.ID_CONTACT, h.ID_ORGANIZATION, DT = dateadd(day, b.B, k.WS)
         , PREV_DT = lag(dateadd(day, b.B, k.WS)) OVER (PARTITION BY h.ID_CONTACT, h.ID_ORGANIZATION ORDER BY h.WEEKNUMBER, b.B)
    INTO #days
    FROM #h AS h
         JOIN #wk AS k ON k.WEEKNUMBER = h.WEEKNUMBER
         JOIN (VALUES (0, 1), (1, 2), (2, 4), (3, 8), (4, 16), (5, 32), (6, 64)) AS b (B, M) ON h.DAY_MASK & b.M > 0;
    CREATE CLUSTERED COLUMNSTORE INDEX cci_days ON #days;

    -- ---------- основные суммы по окнам ----------
    DROP TABLE IF EXISTS #a;
    SELECT p.SL, p.ID_CONTACT, p.ID_ORGANIZATION
         , CHECKS_4  = sum(CASE WHEN x.W BETWEEN 0 AND 3  THEN h.CHECKS END), DAYS_4  = sum(CASE WHEN x.W BETWEEN 0 AND 3  THEN CAST(h.DAYS AS int) END)
         , SPEND_4   = sum(CASE WHEN x.W BETWEEN 0 AND 3  THEN h.SPEND END),  GROSS_4 = sum(CASE WHEN x.W BETWEEN 0 AND 3  THEN h.SPEND_GROSS END)
         , DISC_4    = sum(CASE WHEN x.W BETWEEN 0 AND 3  THEN h.DISCOUNT END), SKU_4 = sum(CASE WHEN x.W BETWEEN 0 AND 3  THEN h.SKU END)
         , CHECKS_6  = sum(CASE WHEN x.W BETWEEN 0 AND 5  THEN h.CHECKS END), DAYS_6  = sum(CASE WHEN x.W BETWEEN 0 AND 5  THEN CAST(h.DAYS AS int) END)
         , SPEND_6   = sum(CASE WHEN x.W BETWEEN 0 AND 5  THEN h.SPEND END),  GROSS_6 = sum(CASE WHEN x.W BETWEEN 0 AND 5  THEN h.SPEND_GROSS END)
         , DISC_6    = sum(CASE WHEN x.W BETWEEN 0 AND 5  THEN h.DISCOUNT END), SKU_6 = sum(CASE WHEN x.W BETWEEN 0 AND 5  THEN h.SKU END)
         , CHECKS_8  = sum(CASE WHEN x.W BETWEEN 0 AND 7  THEN h.CHECKS END), DAYS_8  = sum(CASE WHEN x.W BETWEEN 0 AND 7  THEN CAST(h.DAYS AS int) END)
         , SPEND_8   = sum(CASE WHEN x.W BETWEEN 0 AND 7  THEN h.SPEND END),  GROSS_8 = sum(CASE WHEN x.W BETWEEN 0 AND 7  THEN h.SPEND_GROSS END)
         , DISC_8    = sum(CASE WHEN x.W BETWEEN 0 AND 7  THEN h.DISCOUNT END), SKU_8 = sum(CASE WHEN x.W BETWEEN 0 AND 7  THEN h.SKU END)
         , CHECKS_13 = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN h.CHECKS END), DAYS_13 = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN CAST(h.DAYS AS int) END)
         , SPEND_13  = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN h.SPEND END),  GROSS_13 = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN h.SPEND_GROSS END)
         , DISC_13   = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN h.DISCOUNT END), SKU_13 = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN h.SKU END)
         , CHECKS_26 = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN h.CHECKS END), DAYS_26 = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN CAST(h.DAYS AS int) END)
         , SPEND_26  = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN h.SPEND END),  GROSS_26 = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN h.SPEND_GROSS END)
         , DISC_26   = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN h.DISCOUNT END), SKU_26 = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN h.SKU END)
         , CHECKS_52 = sum(CASE WHEN x.W BETWEEN 0 AND 51 THEN h.CHECKS END), DAYS_52 = sum(CASE WHEN x.W BETWEEN 0 AND 51 THEN CAST(h.DAYS AS int) END)
         , SPEND_52  = sum(CASE WHEN x.W BETWEEN 0 AND 51 THEN h.SPEND END),  GROSS_52 = sum(CASE WHEN x.W BETWEEN 0 AND 51 THEN h.SPEND_GROSS END)
         , DISC_52   = sum(CASE WHEN x.W BETWEEN 0 AND 51 THEN h.DISCOUNT END), SKU_52 = sum(CASE WHEN x.W BETWEEN 0 AND 51 THEN h.SKU END)
         , WEEKS_26  = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN 1 END), WEEKS_52 = sum(CASE WHEN x.W BETWEEN 0 AND 51 THEN 1 END)
         , MAX_CHECK_26 = max(CASE WHEN x.W BETWEEN 0 AND 25 THEN h.MAX_CHECK END)
         , SPEND_SQ_26  = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN CAST(h.SPEND AS float) * CAST(h.SPEND AS float) END)
         -- база W 6…31: суммы и квадраты (недели без покупок — нули, учитываются делением на 26)
         , B_WEEKS   = sum(CASE WHEN x.W BETWEEN 6 AND 31 THEN 1 END)
         , B_S       = sum(CASE WHEN x.W BETWEEN 6 AND 31 THEN CAST(h.SPEND AS float) END)
         , B_SQ      = sum(CASE WHEN x.W BETWEEN 6 AND 31 THEN CAST(h.SPEND AS float) * CAST(h.SPEND AS float) END)
         , B_D       = sum(CASE WHEN x.W BETWEEN 6 AND 31 THEN CAST(h.DAYS AS float) END)
         , B_DQ      = sum(CASE WHEN x.W BETWEEN 6 AND 31 THEN CAST(h.DAYS AS float) * CAST(h.DAYS AS float) END)
         -- отношения — по неделям с покупкой
         , B_CHECK_MEAN  = avg(CASE WHEN x.W BETWEEN 6 AND 31 THEN CAST(h.SPEND AS float) / nullif(h.CHECKS, 0) END)
         , B_CHECK_SD    = stdevp(CASE WHEN x.W BETWEEN 6 AND 31 THEN CAST(h.SPEND AS float) / nullif(h.CHECKS, 0) END)
         , B_SKUCHK_MEAN = avg(CASE WHEN x.W BETWEEN 6 AND 31 THEN CAST(h.SKU AS float) / nullif(h.CHECKS, 0) END)
         , B_SKUCHK_SD   = stdevp(CASE WHEN x.W BETWEEN 6 AND 31 THEN CAST(h.SKU AS float) / nullif(h.CHECKS, 0) END)
         , B_PRICE_MEAN  = avg(CASE WHEN x.W BETWEEN 6 AND 31 THEN CAST(h.SPEND AS float) / nullif(h.SKU, 0) END)
         , B_PRICE_SD    = stdevp(CASE WHEN x.W BETWEEN 6 AND 31 THEN CAST(h.SPEND AS float) / nullif(h.SKU, 0) END)
         , B_DSHARE_MEAN = avg(CASE WHEN x.W BETWEEN 6 AND 31 THEN CAST(h.DISCOUNT AS float) / nullif(h.SPEND_GROSS, 0) END)
         , B_DSHARE_SD   = stdevp(CASE WHEN x.W BETWEEN 6 AND 31 THEN CAST(h.DISCOUNT AS float) / nullif(h.SPEND_GROSS, 0) END)
         , WE_CHECKS_26  = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN h.WE_CHECKS END), WE_SPEND_26 = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN h.WE_SPEND END)
         , MORN_CHECKS_26 = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN h.MORN_CHECKS END), EVEN_CHECKS_26 = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN h.EVEN_CHECKS END)
         , GT5_DAYS_52   = sum(CASE WHEN x.W BETWEEN 0 AND 51 THEN CAST(h.DAYS_GT5 AS int) END)
         , VCARD_CHECKS_52 = sum(CASE WHEN x.W BETWEEN 0 AND 51 THEN h.VCARD_CHECKS END)
         , BONUS_ACCR_13 = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN h.BONUS_ACCR END), BONUS_PAY_13 = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN h.BONUS_PAY END)
         , AVG_SHOPS_26  = avg(CASE WHEN x.W BETWEEN 0 AND 25 THEN CAST(h.SHOPS AS float) END)
         , PP5_CHECKS_13 = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN h.PP5_CHECKS END), PP5_CHECKS_26 = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN h.PP5_CHECKS END)
         , CVM_CHECKS_13 = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN h.CVM_CHECKS END), CVM_CHECKS_26 = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN h.CVM_CHECKS END)
         , LOY_CHECKS_13 = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN h.LOY_CHECKS END), LOY_CHECKS_26 = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN h.LOY_CHECKS END)
         -- исходы (недели после среза) и правило сегмента через 6 нед.: чеки в 52 нед. и 6 нед. до D + 42
         , Y_CHECKS_6 = sum(CASE WHEN x.W BETWEEN -6 AND -1 THEN h.CHECKS END)
         , Y_DAYS_6   = sum(CASE WHEN x.W BETWEEN -6 AND -1 THEN CAST(h.DAYS AS int) END)
         , Y_SPEND_6  = sum(CASE WHEN x.W BETWEEN -6 AND -1 THEN h.SPEND END)
    INTO #a
    FROM #pop AS p
         JOIN #h AS h ON h.ID_CONTACT = p.ID_CONTACT AND h.ID_ORGANIZATION = p.ID_ORGANIZATION
                     AND h.WEEKNUMBER BETWEEN p.SL - 51 AND p.SL + 6
         CROSS APPLY (SELECT W = p.SL - h.WEEKNUMBER) AS x
    GROUP BY p.SL, p.ID_CONTACT, p.ID_ORGANIZATION;
    CREATE CLUSTERED COLUMNSTORE INDEX cci_a ON #a;

    -- ---------- дни: давность, интервалы за 52 нед., LFL-окна, первая покупка после среза ----------
    DROP TABLE IF EXISTS #dy;
    SELECT p.SL, p.ID_CONTACT, p.ID_ORGANIZATION
         , REC_DAYS = datediff(day, max(CASE WHEN d.DT <= p.D THEN d.DT END), p.D)
         , GAP_N    = sum(CASE WHEN d.DT <= p.D AND d.PREV_DT >= dateadd(day, -363, p.D) THEN 1 END)
         , GAP_MEAN = avg(CASE WHEN d.DT <= p.D AND d.PREV_DT >= dateadd(day, -363, p.D) THEN CAST(datediff(day, d.PREV_DT, d.DT) AS float) END)
         , GAP_SD   = stdevp(CASE WHEN d.DT <= p.D AND d.PREV_DT >= dateadd(day, -363, p.D) THEN CAST(datediff(day, d.PREV_DT, d.DT) AS float) END)
         , GAP_MAX  = max(CASE WHEN d.DT <= p.D AND d.PREV_DT >= dateadd(day, -363, p.D) THEN datediff(day, d.PREV_DT, d.DT) END)
         -- LFL на дату (как CVM_CONTACT): покупка в те же 6 нед. год назад; и на дату D + 42
         , LFL0 = max(CASE WHEN d.DT BETWEEN dateadd(year, -1, dateadd(day, -41, p.D)) AND dateadd(year, -1, p.D) THEN 1 ELSE 0 END)
         , LFL6 = max(CASE WHEN d.DT BETWEEN dateadd(year, -1, dateadd(day, 1, p.D)) AND dateadd(year, -1, dateadd(day, 42, p.D)) THEN 1 ELSE 0 END)
         , Y_FIRST_DAYS = datediff(day, p.D, min(CASE WHEN d.DT > p.D AND d.DT <= dateadd(day, 42, p.D) THEN d.DT END))
    INTO #dy
    FROM #pop AS p
         JOIN #days AS d ON d.ID_CONTACT = p.ID_CONTACT AND d.ID_ORGANIZATION = p.ID_ORGANIZATION
                        AND d.DT BETWEEN dateadd(day, -408, p.D) AND dateadd(day, 42, p.D)   -- ревью 1: 408 — в високосный год начало окна LFL (год назад от D − 41) на день раньше 406
    GROUP BY p.SL, p.D, p.ID_CONTACT, p.ID_ORGANIZATION;
    CREATE CLUSTERED COLUMNSTORE INDEX cci_dy ON #dy;

    -- ---------- правило «Случайные» (CVM_CONTACT): средний шаг между неделями покупок за 56 нед. ----------
    -- как в CVM_CONTACT: строки клиент × орг. × неделя; одна неделя в организации — шаг 56; иначе шаг до прошлой строки
    -- клиента (LAG по клиенту без организации — так в исходном правиле); среднее, округлённое до целого, > 6 → Случайный.
    -- E = 0 — на дату среза (недели W 0…55), E = 6 — на дату D + 42 (W −6…49, только размеченные).
    -- LAG считается один раз по всем строкам #h клиента; в окне шаг берётся, только если прошлая строка тоже в окне
    -- (первая строка окна в исходном правиле — без шага); одна неделя организации в окне — 56. Результат тот же, что у
    -- LAG внутри каждого окна, без сортировки строк каждого среза.
    DROP TABLE IF EXISTS #hl;
    SELECT ID_CONTACT, ID_ORGANIZATION, WEEKNUMBER
         , PREV_W = lag(WEEKNUMBER) OVER (PARTITION BY ID_CONTACT ORDER BY WEEKNUMBER, ID_ORGANIZATION)
    INTO #hl
    FROM #h;
    CREATE CLUSTERED COLUMNSTORE INDEX cci_hl ON #hl;

    DROP TABLE IF EXISTS #rw;
    SELECT e.E, p.SL, h.ID_CONTACT, h.ID_ORGANIZATION
         , AVG_DW = CASE WHEN count(*) = 1 THEN 56
                         ELSE round(sum(CASE WHEN h.PREV_W >= p.SL + e.E - 55 THEN CAST(h.WEEKNUMBER - h.PREV_W AS float) END)
                                    / nullif(count(CASE WHEN h.PREV_W >= p.SL + e.E - 55 THEN 1 END), 0), 0) END
    INTO #rw
    FROM (SELECT DISTINCT SL, LAB, ID_CONTACT FROM #pop) AS p
         JOIN (VALUES (0), (6)) AS e (E) ON e.E = 0 OR p.LAB = 1
         JOIN #hl AS h ON h.ID_CONTACT = p.ID_CONTACT
                      AND h.WEEKNUMBER BETWEEN p.SL + e.E - 55 AND p.SL + e.E
    GROUP BY e.E, p.SL, h.ID_CONTACT, h.ID_ORGANIZATION;
    CREATE CLUSTERED COLUMNSTORE INDEX cci_rw ON #rw;
    DROP TABLE IF EXISTS #hl;

    -- ---------- другая организация клиента ----------
    DROP TABLE IF EXISTS #oth;
    SELECT p.SL, p.ID_CONTACT, p.ID_ORGANIZATION
         , OTH_CHECKS_4  = sum(CASE WHEN p.SL - h.WEEKNUMBER BETWEEN 0 AND 3  THEN h.CHECKS END)
         , OTH_CHECKS_26 = sum(CASE WHEN p.SL - h.WEEKNUMBER BETWEEN 0 AND 25 THEN h.CHECKS END)
         , OTH_SPEND_26  = sum(CASE WHEN p.SL - h.WEEKNUMBER BETWEEN 0 AND 25 THEN h.SPEND END)
         , Y_OTH_CHECKS_6 = sum(CASE WHEN p.SL - h.WEEKNUMBER BETWEEN -6 AND -1 THEN h.CHECKS END)
    INTO #oth
    FROM #pop AS p
         JOIN #h AS h ON h.ID_CONTACT = p.ID_CONTACT AND h.ID_ORGANIZATION <> p.ID_ORGANIZATION
                     AND h.WEEKNUMBER BETWEEN p.SL - 25 AND p.SL + 6
    GROUP BY p.SL, p.ID_CONTACT, p.ID_ORGANIZATION;

    -- ---------- основной магазин: 26 нед., последние 6 против базы, трафик и закрытие ----------
    DROP TABLE IF EXISTS #msx;
    SELECT p.SL, p.ID_CONTACT, p.ID_ORGANIZATION, h.MAIN_SHOP
         , N26 = sum(CASE WHEN p.SL - h.WEEKNUMBER BETWEEN 0 AND 25 THEN h.MAIN_SHOP_CHECKS END)
         , N6  = sum(CASE WHEN p.SL - h.WEEKNUMBER BETWEEN 0 AND 5  THEN h.MAIN_SHOP_CHECKS END)
         , NB  = sum(CASE WHEN p.SL - h.WEEKNUMBER BETWEEN 6 AND 31 THEN h.MAIN_SHOP_CHECKS END)
    INTO #msx
    FROM #pop AS p
         JOIN #h AS h ON h.ID_CONTACT = p.ID_CONTACT AND h.ID_ORGANIZATION = p.ID_ORGANIZATION
                     AND h.WEEKNUMBER BETWEEN p.SL - 31 AND p.SL AND h.MAIN_SHOP IS NOT NULL
    GROUP BY p.SL, p.ID_CONTACT, p.ID_ORGANIZATION, h.MAIN_SHOP;

    DROP TABLE IF EXISTS #ms;
    SELECT SL, ID_CONTACT, ID_ORGANIZATION
         , MAIN_SHOP     = max(CASE WHEN R26 = 1 AND N26 > 0 THEN MAIN_SHOP END)
         , MAIN_N26      = max(CASE WHEN R26 = 1 AND N26 > 0 THEN N26 END)
         , MAIN_SHOPS_26 = sum(CASE WHEN N26 > 0 THEN 1 ELSE 0 END)
         , SHOP6         = max(CASE WHEN R6 = 1 AND N6 > 0 THEN MAIN_SHOP END)
         , SHOPB         = max(CASE WHEN RB = 1 AND NB > 0 THEN MAIN_SHOP END)
    INTO #ms
    FROM (SELECT *, R26 = row_number() OVER (PARTITION BY SL, ID_CONTACT, ID_ORGANIZATION ORDER BY N26 DESC, MAIN_SHOP)
                  , R6  = row_number() OVER (PARTITION BY SL, ID_CONTACT, ID_ORGANIZATION ORDER BY N6 DESC, MAIN_SHOP)
                  , RB  = row_number() OVER (PARTITION BY SL, ID_CONTACT, ID_ORGANIZATION ORDER BY NB DESC, MAIN_SHOP)
          FROM #msx) AS x
    GROUP BY SL, ID_CONTACT, ID_ORGANIZATION;

    DROP TABLE IF EXISTS #st;   -- трафик магазина: чеков в неделю за последние 4 и прошлые 13 нед.; нет чеков в неделю среза — закрыт
    SELECT s.SL, w.ID_SHOP
         , T4  = sum(CASE WHEN s.SL - w.WEEKNUMBER BETWEEN 0 AND 3  THEN CAST(w.COUNT_CHECK AS float) END) / 4
         , T13 = sum(CASE WHEN s.SL - w.WEEKNUMBER BETWEEN 4 AND 16 THEN CAST(w.COUNT_CHECK AS float) END) / 13
         , T0  = sum(CASE WHEN w.WEEKNUMBER = s.SL THEN w.COUNT_CHECK END)
    INTO #st
    FROM #sl AS s
         JOIN dbo.I_SHOP_WEEK AS w (nolock) ON w.WEEKNUMBER BETWEEN s.SL - 16 AND s.SL
    WHERE w.ID_COMPANY = @idc
      AND w.ID_SHOP IN (SELECT MAIN_SHOP FROM #ms WHERE MAIN_SHOP IS NOT NULL)
    GROUP BY s.SL, w.ID_SHOP;

    -- ---------- пенетрация, популярность, ценовой индекс (I_CONT_ASSORT_W), средние с весом UNIQUE_SKU ----------
    DROP TABLE IF EXISTS #ca;
    SELECT p.SL, p.ID_CONTACT, p.ID_ORGANIZATION
         , PEN_4   = sum(CASE WHEN x.W BETWEEN 0 AND 3  THEN c.PENETRATION_CHECK * c.UNIQUE_SKU END) / nullif(sum(CASE WHEN x.W BETWEEN 0 AND 3 AND c.PENETRATION_CHECK IS NOT NULL THEN c.UNIQUE_SKU END), 0)
         , PEN_P13 = sum(CASE WHEN x.W BETWEEN 4 AND 16 THEN c.PENETRATION_CHECK * c.UNIQUE_SKU END) / nullif(sum(CASE WHEN x.W BETWEEN 4 AND 16 AND c.PENETRATION_CHECK IS NOT NULL THEN c.UNIQUE_SKU END), 0)
         , POP_4   = sum(CASE WHEN x.W BETWEEN 0 AND 3  THEN c.POPULARITY * c.UNIQUE_SKU END) / nullif(sum(CASE WHEN x.W BETWEEN 0 AND 3 AND c.POPULARITY IS NOT NULL THEN c.UNIQUE_SKU END), 0)
         , POP_P13 = sum(CASE WHEN x.W BETWEEN 4 AND 16 THEN c.POPULARITY * c.UNIQUE_SKU END) / nullif(sum(CASE WHEN x.W BETWEEN 4 AND 16 AND c.POPULARITY IS NOT NULL THEN c.UNIQUE_SKU END), 0)
         , PIDX_4  = sum(CASE WHEN x.W BETWEEN 0 AND 3  THEN c.AVG_PRICE_INDEX * c.UNIQUE_SKU END) / nullif(sum(CASE WHEN x.W BETWEEN 0 AND 3 AND c.AVG_PRICE_INDEX IS NOT NULL THEN c.UNIQUE_SKU END), 0)
         , PIDX_P13 = sum(CASE WHEN x.W BETWEEN 4 AND 16 THEN c.AVG_PRICE_INDEX * c.UNIQUE_SKU END) / nullif(sum(CASE WHEN x.W BETWEEN 4 AND 16 AND c.AVG_PRICE_INDEX IS NOT NULL THEN c.UNIQUE_SKU END), 0)
         , USKU_4  = avg(CASE WHEN x.W BETWEEN 0 AND 3 THEN CAST(c.UNIQUE_SKU AS float) END)
    INTO #ca
    FROM #pop AS p
         JOIN dbo.I_CONT_ASSORT_W AS c (nolock)
           ON c.ID_CONTACT = p.ID_CONTACT AND c.ID_ORGANIZATION = p.ID_ORGANIZATION AND c.ID_COMPANY = @idc
          AND c.WEEKNUMBER BETWEEN p.SL - 16 AND p.SL
         CROSS APPLY (SELECT W = p.SL - c.WEEKNUMBER) AS x
    WHERE c.WEEKNUMBER BETWEEN @s1 - 16 AND @s2
    GROUP BY p.SL, p.ID_CONTACT, p.ID_ORGANIZATION
    OPTION (RECOMPILE);

    -- ---------- рассылки (клиент, без организации) ----------
    DROP TABLE IF EXISTS #cm;
    SELECT p.SL, p.ID_CONTACT
         , PUSH_MSG_13   = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN m.PUSH_MSG END)
         , PUSH_DELIV_4  = sum(CASE WHEN x.W BETWEEN 0 AND 3  THEN m.PUSH_DELIV END)
         , PUSH_DELIV_13 = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN m.PUSH_DELIV END)
         , PUSH_DELIV_26 = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN m.PUSH_DELIV END)
         , PUSH_CLICK_4  = sum(CASE WHEN x.W BETWEEN 0 AND 3  THEN m.PUSH_CLICK END)
         , PUSH_CLICK_6  = sum(CASE WHEN x.W BETWEEN 0 AND 5  THEN m.PUSH_CLICK END)
         , PUSH_CLICK_13 = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN m.PUSH_CLICK END)
         , PUSH_CLICK_26 = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN m.PUSH_CLICK END)
         , PUSH_ERR_13   = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN m.PUSH_ERR END)
         , B_C   = sum(CASE WHEN x.W BETWEEN 6 AND 31 THEN CAST(m.PUSH_CLICK AS float) END)
         , B_CQ  = sum(CASE WHEN x.W BETWEEN 6 AND 31 THEN CAST(m.PUSH_CLICK AS float) * m.PUSH_CLICK END)
         , EMAIL_DELIV_13 = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN m.EMAIL_DELIV END)
         , EMAIL_OPEN_13  = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN m.EMAIL_OPEN END)
         , EMAIL_CLICK_13 = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN m.EMAIL_CLICK END)
         , EMAIL_UNSUB_26 = sum(CASE WHEN x.W BETWEEN 0 AND 25 THEN m.EMAIL_UNSUB END)
         , SMS_DELIV_13   = sum(CASE WHEN x.W BETWEEN 0 AND 12 THEN m.SMS_DELIV END)
    INTO #cm
    FROM (SELECT DISTINCT SL, ID_CONTACT FROM #pop) AS p
         JOIN dbo.I_CVM_CHURN_COMM_W AS m (nolock)
           ON m.ID_CONTACT = p.ID_CONTACT AND m.WEEKNUMBER BETWEEN p.SL - 31 AND p.SL
         CROSS APPLY (SELECT W = p.SL - m.WEEKNUMBER) AS x
    WHERE m.WEEKNUMBER BETWEEN @s1 - 31 AND @s2
    GROUP BY p.SL, p.ID_CONTACT
    OPTION (RECOMPILE);

    -- ---------- события: акции CVM и 5ПП, любимые, купоны ----------
    DROP TABLE IF EXISTS #ev;
    SELECT e.* INTO #ev FROM dbo.I_CVM_CHURN_EVENT AS e (nolock)
    WHERE EXISTS (SELECT 1 FROM #pop AS p WHERE p.ID_CONTACT = e.ID_CONTACT);
    CREATE CLUSTERED COLUMNSTORE INDEX cci_ev ON #ev;

    DROP TABLE IF EXISTS #ep;   -- акции: в ЦГ / КГ за 26 нед.; отклик — покупка в организации акции в окне акции до среза
    SELECT p.SL, p.ID_CONTACT, p.ID_ORGANIZATION
         , CVM_TG_26 = sum(CASE WHEN e.EV_TYPE = 1 AND e.CG = 0 AND e.DATE_FROM > dateadd(day, -182, p.D) THEN 1 ELSE 0 END)
         , CVM_CG_26 = sum(CASE WHEN e.EV_TYPE = 1 AND e.CG = 1 AND e.DATE_FROM > dateadd(day, -182, p.D) THEN 1 ELSE 0 END)
         , CVM_RESP_26 = CAST(0 AS int)   -- ревью 1: подзапрос EXISTS внутри SUM невозможен (Msg 130) — считается ниже (#er) и вписывается UPDATE
         -- дней с последнего попадания в ЦГ — в пределах года (NULL — за 52 нед. в ЦГ не был): события только окна 52 нед.
         , CVM_TG_LAST_DAYS = datediff(day, max(CASE WHEN e.EV_TYPE = 1 AND e.CG = 0 THEN e.DATE_FROM END), p.D)
         , PP5_TG_26 = sum(CASE WHEN e.EV_TYPE = 2 AND e.CG = 0 AND e.DATE_FROM > dateadd(day, -182, p.D) THEN 1 ELSE 0 END)
    INTO #ep
    FROM #pop AS p
         JOIN #ev AS e ON e.ID_CONTACT = p.ID_CONTACT AND e.EV_TYPE IN (1, 2)
                      AND (e.ID_ORGANIZATION = p.ID_ORGANIZATION OR e.ID_ORGANIZATION IS NULL)
                      AND e.DATE_FROM <= p.D AND e.DATE_FROM > dateadd(day, -364, p.D)
    GROUP BY p.SL, p.D, p.ID_CONTACT, p.ID_ORGANIZATION;

    -- отклик на акции ЦГ: акция за 26 нед. до среза, после старта которой клиент покупал в организации до конца акции или до среза
    -- (одна акция = одна строка #ev клиента для этой организации — группировка по ID_OBJ только защищает от повтора)
    DROP TABLE IF EXISTS #er;
    SELECT x.SL, x.ID_CONTACT, x.ID_ORGANIZATION, N = count(*)
    INTO #er
    FROM (SELECT p.SL, p.ID_CONTACT, p.ID_ORGANIZATION, e.ID_OBJ
          FROM #pop AS p
               JOIN #ev AS e ON e.ID_CONTACT = p.ID_CONTACT AND e.EV_TYPE = 1 AND e.CG = 0
                            AND (e.ID_ORGANIZATION = p.ID_ORGANIZATION OR e.ID_ORGANIZATION IS NULL)
                            AND e.DATE_FROM > dateadd(day, -182, p.D) AND e.DATE_FROM <= p.D
               JOIN #days AS d ON d.ID_CONTACT = p.ID_CONTACT AND d.ID_ORGANIZATION = p.ID_ORGANIZATION
                              AND d.DT >= e.DATE_FROM AND d.DT > dateadd(day, -182, p.D)
                              AND d.DT <= IIF(isnull(e.DATE_TO, p.D) < p.D, isnull(e.DATE_TO, p.D), p.D)
          GROUP BY p.SL, p.ID_CONTACT, p.ID_ORGANIZATION, e.ID_OBJ) AS x
    GROUP BY x.SL, x.ID_CONTACT, x.ID_ORGANIZATION;
    UPDATE ep SET CVM_RESP_26 = r.N
    FROM #ep AS ep
         JOIN #er AS r ON r.SL = ep.SL AND r.ID_CONTACT = ep.ID_CONTACT AND r.ID_ORGANIZATION = ep.ID_ORGANIZATION;

    DROP TABLE IF EXISTS #ef;   -- любимые (последний выбор не позже среза) и купоны — по клиенту
    SELECT p.SL, p.ID_CONTACT
         , FAV_CAT_DT  = max(CASE WHEN e.EV_TYPE = 4 THEN e.DATE_FROM END)
         , FAV_PROD_DT = max(CASE WHEN e.EV_TYPE = 5 THEN e.DATE_FROM END)
         , COUPON_13 = sum(CASE WHEN e.EV_TYPE = 6 AND e.DATE_FROM > dateadd(day, -91,  p.D) THEN e.QTY ELSE 0 END)
         , COUPON_26 = sum(CASE WHEN e.EV_TYPE = 6 AND e.DATE_FROM > dateadd(day, -182, p.D) THEN e.QTY ELSE 0 END)
    INTO #ef
    FROM (SELECT DISTINCT SL, D, ID_CONTACT FROM #pop) AS p
         JOIN #ev AS e ON e.ID_CONTACT = p.ID_CONTACT AND e.EV_TYPE IN (4, 5, 6) AND e.DATE_FROM <= p.D
    GROUP BY p.SL, p.ID_CONTACT;

    DROP TABLE IF EXISTS #ef2;  -- сколько выбрано в последнем выборе
    -- ревью 1: вместо коррелированных подзапросов по #ev (CCI без индекса) — одно соединение; результат тот же
    -- (не выбирал до среза — NULL и у категорий, и у товаров)
    SELECT f.SL, f.ID_CONTACT
         , FAV_CAT_N  = sum(CASE WHEN e.EV_TYPE = 4 AND e.DATE_FROM = f.FAV_CAT_DT THEN e.QTY END)
         , FAV_PROD_N = sum(CASE WHEN e.EV_TYPE = 5 AND e.DATE_FROM = f.FAV_PROD_DT THEN 1 END)
    INTO #ef2
    FROM #ef AS f
         LEFT JOIN #ev AS e ON e.ID_CONTACT = f.ID_CONTACT AND e.EV_TYPE IN (4, 5)
                           AND (e.DATE_FROM = f.FAV_CAT_DT OR e.DATE_FROM = f.FAV_PROD_DT)
    GROUP BY f.SL, f.ID_CONTACT;

    DROP TABLE IF EXISTS #fb;   -- покупки любимых товаров последнего выбора за 4 и 13 нед. до среза (чеки)
    SELECT p.SL, p.ID_CONTACT, p.ID_ORGANIZATION
         , FAV_PROD_BUY_4  = sum(CASE WHEN p.SL - i.WEEKNUMBER BETWEEN 0 AND 3 THEN i.CHECKS END)
         , FAV_PROD_BUY_13 = sum(i.CHECKS)
    INTO #fb
    FROM #pop AS p
         JOIN #ef AS f ON f.SL = p.SL AND f.ID_CONTACT = p.ID_CONTACT AND f.FAV_PROD_DT IS NOT NULL
         JOIN #ev AS e ON e.ID_CONTACT = p.ID_CONTACT AND e.EV_TYPE = 5 AND e.DATE_FROM = f.FAV_PROD_DT
         JOIN dbo.I_CVM_CHURN_ITEM_W AS i (nolock)
           ON i.ID_CONTACT = p.ID_CONTACT AND i.ID_ORGANIZATION = p.ID_ORGANIZATION AND i.ID_PRODUCT = e.ID_OBJ
          AND i.WEEKNUMBER BETWEEN p.SL - 12 AND p.SL
    WHERE i.WEEKNUMBER BETWEEN @s1 - 12 AND @s2
    GROUP BY p.SL, p.ID_CONTACT, p.ID_ORGANIZATION
    OPTION (RECOMPILE);

    -- ---------- категории CVM (Н2а): нормальные категории (≥ 3 нед. из 26 базы), выпавшие, наибольшее падение, группы ----------
    DROP TABLE IF EXISTS #cg;
    SELECT CAT = ID_CVM_CATEGORY, GRP = max(ID_CVM_GROUP) INTO #cg
    FROM dbo.I_CVM_PRODUCT_CATEGORY (nolock) WHERE ID_CVM_CATEGORY > 0 GROUP BY ID_CVM_CATEGORY;
    CREATE CLUSTERED INDEX ci_cg ON #cg (CAT);

    DROP TABLE IF EXISTS #kc;
    SELECT p.SL, p.ID_CONTACT, p.ID_ORGANIZATION, c.ID_CVM_CATEGORY AS CAT
         , L4 = sum(CASE WHEN x.W BETWEEN 0 AND 3 THEN c.SPEND END)
         , L6 = sum(CASE WHEN x.W BETWEEN 0 AND 5 THEN c.SPEND END)
         , BS = sum(CASE WHEN x.W BETWEEN 6 AND 31 THEN CAST(c.SPEND AS float) END)
         , BQ = sum(CASE WHEN x.W BETWEEN 6 AND 31 THEN CAST(c.SPEND AS float) * CAST(c.SPEND AS float) END)
         , BW = sum(CASE WHEN x.W BETWEEN 6 AND 31 THEN 1 END)
    INTO #kc
    FROM #pop AS p
         JOIN dbo.I_CVM_CHURN_CAT_W AS c (nolock)
           ON c.ID_CONTACT = p.ID_CONTACT AND c.ID_ORGANIZATION = p.ID_ORGANIZATION
          AND c.WEEKNUMBER BETWEEN p.SL - 31 AND p.SL AND c.ID_CVM_CATEGORY > 0
         CROSS APPLY (SELECT W = p.SL - c.WEEKNUMBER) AS x
    WHERE c.WEEKNUMBER BETWEEN @s1 - 31 AND @s2
    GROUP BY p.SL, p.ID_CONTACT, p.ID_ORGANIZATION, c.ID_CVM_CATEGORY
    OPTION (RECOMPILE);
    CREATE CLUSTERED COLUMNSTORE INDEX cci_kc ON #kc;

    DROP TABLE IF EXISTS #kk;
    SELECT SL, ID_CONTACT, ID_ORGANIZATION
         , CAT_NORM_N   = sum(CASE WHEN BW >= 3 THEN 1 ELSE 0 END)
         , CAT_ACTIVE_6 = sum(CASE WHEN L6 > 0 THEN 1 ELSE 0 END)
         , CAT_DROP_N   = sum(CASE WHEN BW >= 3 AND isnull(L6, 0) <= 0 THEN 1 ELSE 0 END)
         , CAT_DROP_SHARE = sum(CASE WHEN BW >= 3 AND isnull(L6, 0) <= 0 THEN BS END) / nullif(sum(BS), 0)
         , CAT_MAXDROP     = max(CASE WHEN RD = 1 THEN CAT END)
         , CAT_MAXDROP_L6  = max(CASE WHEN RD = 1 THEN isnull(L6, 0) END)
         , CAT_MAXDROP_BM  = max(CASE WHEN RD = 1 THEN BS / 26 END)
         , CAT_MAXDROP_BSD = max(CASE WHEN RD = 1 THEN sqrt(IIF(BQ / 26 - square(BS / 26) > 0, BQ / 26 - square(BS / 26), 0)) END)
         , CAT_MAXDROP_BW  = max(CASE WHEN RD = 1 THEN BW END)
    INTO #kk
    FROM (SELECT *, RD = CASE WHEN BW >= 3 THEN row_number() OVER (PARTITION BY SL, ID_CONTACT, ID_ORGANIZATION
                                                                   ORDER BY CASE WHEN BW >= 3 THEN 0 ELSE 1 END,
                                                                            isnull(BS, 0) / 26 * 6 - isnull(CAST(L6 AS float), 0) DESC, CAT) END
          FROM #kc) AS x
    GROUP BY SL, ID_CONTACT, ID_ORGANIZATION;

    DROP TABLE IF EXISTS #kg;   -- группы: траты за 4 и 6 нед., среднее и разброс недельных трат базы (26 нед., с нулями)
    SELECT k.SL, k.ID_CONTACT, k.ID_ORGANIZATION, g.GRP
         , L4 = sum(k.L4), L6 = sum(k.L6), BM = sum(k.BS) / 26
         , BSD = CAST(NULL AS float)   -- ревью 1: голый NULL давал колонку int — разброс обрезался до целого
    INTO #kg
    FROM #kc AS k JOIN #cg AS g ON g.CAT = k.CAT
    WHERE g.GRP > 0
    GROUP BY k.SL, k.ID_CONTACT, k.ID_ORGANIZATION, g.GRP;
    -- разброс группы — по недельным суммам группы (не по категориям): отдельный проход по неделям
    DROP TABLE IF EXISTS #kgw;
    SELECT p.SL, p.ID_CONTACT, p.ID_ORGANIZATION, g.GRP, c.WEEKNUMBER, S = sum(CAST(c.SPEND AS float))
    INTO #kgw
    FROM #pop AS p
         JOIN dbo.I_CVM_CHURN_CAT_W AS c (nolock)
           ON c.ID_CONTACT = p.ID_CONTACT AND c.ID_ORGANIZATION = p.ID_ORGANIZATION
          AND c.WEEKNUMBER BETWEEN p.SL - 31 AND p.SL - 6 AND c.ID_CVM_CATEGORY > 0
         JOIN #cg AS g ON g.CAT = c.ID_CVM_CATEGORY AND g.GRP > 0
    WHERE c.WEEKNUMBER BETWEEN @s1 - 31 AND @s2 - 6
    GROUP BY p.SL, p.ID_CONTACT, p.ID_ORGANIZATION, g.GRP, c.WEEKNUMBER
    OPTION (RECOMPILE);
    UPDATE k SET BSD = sqrt(IIF(q.SQ / 26 - square(q.S / 26) > 0, q.SQ / 26 - square(q.S / 26), 0))
    FROM #kg AS k
         JOIN (SELECT SL, ID_CONTACT, ID_ORGANIZATION, GRP, S = sum(S), SQ = sum(S * S) FROM #kgw
               GROUP BY SL, ID_CONTACT, ID_ORGANIZATION, GRP) AS q
           ON q.SL = k.SL AND q.ID_CONTACT = k.ID_CONTACT AND q.ID_ORGANIZATION = k.ID_ORGANIZATION AND q.GRP = k.GRP;
    UPDATE #kg SET BSD = 0 WHERE BSD IS NULL;   -- в базе групповых покупок не было
    DROP TABLE IF EXISTS #kgw, #kc;

    -- ---------- клиент: пол, возраст, регистрация, первая покупка ----------
    DROP TABLE IF EXISTS #ct;
    SELECT c.ID_CONTACT, SEX = max(NULLIF(c.SEX, 0)), BIRTH = max(c.BIRTH_DATE), REG = min(c.REGISTRATION_DATE)
    INTO #ct
    FROM dbo.I_CONTACT AS c (nolock)
    WHERE c.ID_COMPANY = @idc AND EXISTS (SELECT 1 FROM #pn AS p WHERE p.ID_CONTACT = c.ID_CONTACT)
    GROUP BY c.ID_CONTACT;
    CREATE CLUSTERED INDEX ci_ct ON #ct (ID_CONTACT);

    DROP TABLE IF EXISTS #f0;
    SELECT f.ID_CONTACT, f.ID_ORGANIZATION, f.FIRST_DATA, CO_FIRST = min(f.FIRST_DATA) OVER (PARTITION BY f.ID_CONTACT)
    INTO #f0
    FROM dbo.I_CVM_CHURN_FIRST AS f (nolock)
    WHERE EXISTS (SELECT 1 FROM #pn AS p WHERE p.ID_CONTACT = f.ID_CONTACT);
    CREATE CLUSTERED COLUMNSTORE INDEX cci_f0 ON #f0;

    -- ---------- запись среза: статические колонки + группы (динамически по колонкам таблицы) ----------
    -- ревью 1: списки колонок групп — STRING_AGG ... WITHIN GROUP (ORDER BY g): порядок во всех четырёх списках гарантированно один
    -- (SELECT @v += ... ORDER BY порядок не гарантирует — при расхождении колонки группы молча поехали бы)
    DROP TABLE IF EXISTS #gcol;
    SELECT g = CAST(SUBSTRING(name, 2, CHARINDEX(N'_', name) - 2) AS int)
    INTO #gcol
    FROM sys.columns WHERE object_id = OBJECT_ID('dbo.I_CVM_CHURN_SLICE') AND name LIKE N'G[0-9]%[_]L4';

    DECLARE @gcols nvarchar(max), @gsel nvarchar(max), @gnames nvarchar(max), @galias nvarchar(max);
    SELECT @gcols  = STRING_AGG(CAST(CONCAT(N', G', g, N'_L4, G', g, N'_L6, G', g, N'_BM, G', g, N'_BSD') AS nvarchar(max)), N'') WITHIN GROUP (ORDER BY g)
         , @gsel   = STRING_AGG(CAST(CONCAT(N', max(CASE WHEN GRP = ', g, N' THEN L4 END), max(CASE WHEN GRP = ', g, N' THEN L6 END)',
                                            N', max(CASE WHEN GRP = ', g, N' THEN BM END), max(CASE WHEN GRP = ', g, N' THEN BSD END)') AS nvarchar(max)), N'') WITHIN GROUP (ORDER BY g)
         , @gnames = STRING_AGG(CAST(CONCAT(N', gp.G', g, N'_L4, gp.G', g, N'_L6, gp.G', g, N'_BM, gp.G', g, N'_BSD') AS nvarchar(max)), N'') WITHIN GROUP (ORDER BY g)
         , @galias = STRING_AGG(CAST(CONCAT(N', G', g, N'_L4, G', g, N'_L6, G', g, N'_BM, G', g, N'_BSD') AS nvarchar(max)), N'') WITHIN GROUP (ORDER BY g)
    FROM #gcol;
    SELECT @gcols = isnull(@gcols, N''), @gsel = isnull(@gsel, N''), @gnames = isnull(@gnames, N''), @galias = isnull(@galias, N'');

    DECLARE @gl nvarchar(400) = (SELECT STRING_AGG(CAST(GRP AS nvarchar(10)), N', ') FROM (SELECT DISTINCT GRP FROM #kg) AS x
                                 WHERE COL_LENGTH('dbo.I_CVM_CHURN_SLICE', CONCAT('G', GRP, '_L4')) IS NULL);

    DECLARE @gpiv nvarchar(max) = IIF(@gsel = N'', N'SELECT SL, ID_CONTACT, ID_ORGANIZATION FROM #kg WHERE 1 = 0',
                                      N'SELECT SL, ID_CONTACT, ID_ORGANIZATION' + @gsel + N' FROM #kg GROUP BY SL, ID_CONTACT, ID_ORGANIZATION');

    DECLARE @w1 int = (SELECT min(SL) FROM #sl), @w2 int = (SELECT max(SL) FROM #sl);
    EXEC dbo.CVM_CHURN_TRUNC N'I_CVM_CHURN_SLICE', @w1, @w2;

    DECLARE @sql nvarchar(max) = N'
    INSERT INTO dbo.I_CVM_CHURN_SLICE WITH (TABLOCK) (
          WEEKNUMBER, SLICE_DATE, ID_COMPANY, ID_ORGANIZATION, ID_CONTACT, SEG_D, SEG_D6, LABELED
        , Y_CHECKS_6, Y_DAYS_6, Y_SPEND_6, Y_FIRST_DAYS, Y_OTH_CHECKS_6
        , REC_DAYS, TENURE_DAYS, TENURE_CO_DAYS, REG_DAYS, AGE, SEX, GAP_N, GAP_MEAN, GAP_SD, GAP_MAX
        , CHECKS_4, DAYS_4, SPEND_4, GROSS_4, DISC_4, SKU_4, CHECKS_6, DAYS_6, SPEND_6, GROSS_6, DISC_6, SKU_6
        , CHECKS_8, DAYS_8, SPEND_8, GROSS_8, DISC_8, SKU_8, CHECKS_13, DAYS_13, SPEND_13, GROSS_13, DISC_13, SKU_13
        , CHECKS_26, DAYS_26, SPEND_26, GROSS_26, DISC_26, SKU_26, CHECKS_52, DAYS_52, SPEND_52, GROSS_52, DISC_52, SKU_52
        , WEEKS_26, WEEKS_52, MAX_CHECK_26, SPEND_SD_26
        , B_WEEKS, B_SPEND_MEAN, B_SPEND_SD, B_DAYS_MEAN, B_DAYS_SD, B_CHECK_MEAN, B_CHECK_SD, B_SKUCHK_MEAN, B_SKUCHK_SD
        , B_PRICE_MEAN, B_PRICE_SD, B_DSHARE_MEAN, B_DSHARE_SD
        , WE_CHECKS_26, WE_SPEND_26, MORN_CHECKS_26, EVEN_CHECKS_26, GT5_DAYS_52, VCARD_CHECKS_52, BONUS_ACCR_13, BONUS_PAY_13
        , AVG_SHOPS_26, MAIN_SHOP, MAIN_SHARE_26, MAIN_SHOPS_26, MAIN_CHANGED, SHOP_T4, SHOP_T13, SHOP_CLOSED
        , OTH_CHECKS_4, OTH_CHECKS_26, OTH_SPEND_26
        , RULES_OK, PP5_CHECKS_13, PP5_CHECKS_26, CVM_CHECKS_13, CVM_CHECKS_26, LOY_CHECKS_13, LOY_CHECKS_26
        , PEN_4, PEN_P13, POP_4, POP_P13, PIDX_4, PIDX_P13, USKU_4
        , COMM_OK, PUSH_MSG_13, PUSH_DELIV_4, PUSH_DELIV_13, PUSH_DELIV_26, PUSH_CLICK_4, PUSH_CLICK_6, PUSH_CLICK_13, PUSH_CLICK_26
        , PUSH_ERR_13, B_CLICK_MEAN, B_CLICK_SD, EMAIL_DELIV_13, EMAIL_OPEN_13, EMAIL_CLICK_13, EMAIL_UNSUB_26, SMS_DELIV_13
        , CVM_TG_26, CVM_CG_26, CVM_RESP_26, CVM_TG_LAST_DAYS, PP5_TG_26
        , FAV_CAT_N, FAV_CAT_DAYS, FAV_PROD_N, FAV_PROD_DAYS, FAV_PROD_BUY_4, FAV_PROD_BUY_13, COUPON_13, COUPON_26
        , CAT_NORM_N, CAT_ACTIVE_6, CAT_DROP_N, CAT_DROP_SHARE, CAT_MAXDROP, CAT_MAXDROP_L6, CAT_MAXDROP_BM, CAT_MAXDROP_BSD, CAT_MAXDROP_BW'
        + @gcols + N')
    SELECT p.SL, p.D, @idc, p.ID_ORGANIZATION, p.ID_CONTACT
         -- сегмент на дату: Новый — первая покупка в организации за 6 нед.; Случайный (офлайн) — средний шаг недель > 6;
         -- e-com — Активный; офлайн — Активный LFL при покупке в те же 6 нед. год назад
         , SEG_D = CASE WHEN f.FIRST_DATA >= dateadd(day, -41, p.D) THEN 1
                        WHEN r0.AVG_DW > 6 AND p.ID_ORGANIZATION = 1 THEN 4
                        WHEN p.ID_ORGANIZATION = 3 THEN 2
                        WHEN dy.LFL0 = 1 THEN 3 ELSE 2 END
         -- сегмент на дату D + 42 (только размеченные): Случайный / Отток / Активный (LFL)
         , SEG_D6 = CASE WHEN p.LAB = 0 THEN NULL
                         WHEN r6.AVG_DW > 6 THEN CASE WHEN p.ID_ORGANIZATION = 3 THEN IIF(isnull(a.Y_CHECKS_6, 0) > 0, 2, 5) ELSE 4 END
                         WHEN isnull(a.Y_CHECKS_6, 0) = 0 THEN 5
                         WHEN p.ID_ORGANIZATION = 3 THEN 2
                         WHEN dy.LFL6 = 1 THEN 3 ELSE 2 END
         , p.LAB
         , IIF(p.LAB = 1, isnull(a.Y_CHECKS_6, 0), NULL), IIF(p.LAB = 1, isnull(a.Y_DAYS_6, 0), NULL), IIF(p.LAB = 1, isnull(a.Y_SPEND_6, 0), NULL)
         , IIF(p.LAB = 1, dy.Y_FIRST_DAYS, NULL), IIF(p.LAB = 1, isnull(o.Y_OTH_CHECKS_6, 0), NULL)
         , dy.REC_DAYS, datediff(day, f.FIRST_DATA, p.D), datediff(day, f.CO_FIRST, p.D)
         , IIF(ct.REG > p.D, NULL, datediff(day, ct.REG, p.D))   -- регистрация позже среза — знание о будущем
         , CASE WHEN ct.BIRTH IS NULL OR year(ct.BIRTH) = 1970 OR ct.BIRTH > p.D OR datediff(year, ct.BIRTH, p.D) > 255 THEN NULL   -- ревью 1: защита tinyint от мусорных дат рождения
                ELSE datediff(year, ct.BIRTH, p.D) - IIF(dateadd(year, datediff(year, ct.BIRTH, p.D), ct.BIRTH) > p.D, 1, 0) END
         , ct.SEX
         , dy.GAP_N, dy.GAP_MEAN, dy.GAP_SD, dy.GAP_MAX
         , isnull(a.CHECKS_4, 0), isnull(a.DAYS_4, 0), isnull(a.SPEND_4, 0), isnull(a.GROSS_4, 0), isnull(a.DISC_4, 0), isnull(a.SKU_4, 0)   -- ревью 1: нет покупок за 4 нед. — это 0, не NULL
         , a.CHECKS_6, a.DAYS_6, a.SPEND_6, a.GROSS_6, a.DISC_6, a.SKU_6
         , a.CHECKS_8, a.DAYS_8, a.SPEND_8, a.GROSS_8, a.DISC_8, a.SKU_8, a.CHECKS_13, a.DAYS_13, a.SPEND_13, a.GROSS_13, a.DISC_13, a.SKU_13
         , a.CHECKS_26, a.DAYS_26, a.SPEND_26, a.GROSS_26, a.DISC_26, a.SKU_26, a.CHECKS_52, a.DAYS_52, a.SPEND_52, a.GROSS_52, a.DISC_52, a.SKU_52
         , a.WEEKS_26, a.WEEKS_52, a.MAX_CHECK_26
         , sqrt(IIF(a.SPEND_SQ_26 / 26 - square(CAST(a.SPEND_26 AS float) / 26) > 0, a.SPEND_SQ_26 / 26 - square(CAST(a.SPEND_26 AS float) / 26), 0))
         , isnull(a.B_WEEKS, 0), isnull(a.B_S, 0) / 26
         , sqrt(IIF(isnull(a.B_SQ, 0) / 26 - square(isnull(a.B_S, 0) / 26) > 0, isnull(a.B_SQ, 0) / 26 - square(isnull(a.B_S, 0) / 26), 0))
         , isnull(a.B_D, 0) / 26
         , sqrt(IIF(isnull(a.B_DQ, 0) / 26 - square(isnull(a.B_D, 0) / 26) > 0, isnull(a.B_DQ, 0) / 26 - square(isnull(a.B_D, 0) / 26), 0))
         , a.B_CHECK_MEAN, a.B_CHECK_SD, a.B_SKUCHK_MEAN, a.B_SKUCHK_SD, a.B_PRICE_MEAN, a.B_PRICE_SD, a.B_DSHARE_MEAN, a.B_DSHARE_SD
         , a.WE_CHECKS_26, a.WE_SPEND_26, a.MORN_CHECKS_26, a.EVEN_CHECKS_26, a.GT5_DAYS_52, a.VCARD_CHECKS_52, a.BONUS_ACCR_13, a.BONUS_PAY_13
         , a.AVG_SHOPS_26, ms.MAIN_SHOP, CAST(ms.MAIN_N26 AS float) / nullif(a.CHECKS_26, 0), ms.MAIN_SHOPS_26
         , CASE WHEN ms.SHOP6 IS NULL OR ms.SHOPB IS NULL THEN NULL WHEN ms.SHOP6 <> ms.SHOPB THEN 1 ELSE 0 END
         , st.T4, st.T13, CASE WHEN ms.MAIN_SHOP IS NULL THEN NULL WHEN isnull(st.T0, 0) = 0 THEN 1 ELSE 0 END
         , isnull(o.OTH_CHECKS_4, 0), isnull(o.OTH_CHECKS_26, 0), isnull(o.OTH_SPEND_26, 0)
         , CAST(IIF(@rules_w IS NOT NULL AND p.SL - 25 >= @rules_w, 1, 0) AS bit)
         , IIF(@rules_w IS NOT NULL AND p.SL - 12 >= @rules_w, isnull(a.PP5_CHECKS_13, 0), NULL), IIF(@rules_w IS NOT NULL AND p.SL - 25 >= @rules_w, isnull(a.PP5_CHECKS_26, 0), NULL)
         , IIF(@rules_w IS NOT NULL AND p.SL - 12 >= @rules_w, isnull(a.CVM_CHECKS_13, 0), NULL), IIF(@rules_w IS NOT NULL AND p.SL - 25 >= @rules_w, isnull(a.CVM_CHECKS_26, 0), NULL)
         , IIF(@rules_w IS NOT NULL AND p.SL - 12 >= @rules_w, isnull(a.LOY_CHECKS_13, 0), NULL), IIF(@rules_w IS NOT NULL AND p.SL - 25 >= @rules_w, isnull(a.LOY_CHECKS_26, 0), NULL)
         , ca.PEN_4, ca.PEN_P13, ca.POP_4, ca.POP_P13, ca.PIDX_4, ca.PIDX_P13, ca.USKU_4
         , CAST(IIF(@comm_w IS NOT NULL AND p.SL - 31 >= @comm_w, 1, 0) AS bit)
         , isnull(cm.PUSH_MSG_13, 0), isnull(cm.PUSH_DELIV_4, 0), isnull(cm.PUSH_DELIV_13, 0), isnull(cm.PUSH_DELIV_26, 0)
         , isnull(cm.PUSH_CLICK_4, 0), isnull(cm.PUSH_CLICK_6, 0), isnull(cm.PUSH_CLICK_13, 0), isnull(cm.PUSH_CLICK_26, 0), isnull(cm.PUSH_ERR_13, 0)
         , isnull(cm.B_C, 0) / 26
         , sqrt(IIF(isnull(cm.B_CQ, 0) / 26 - square(isnull(cm.B_C, 0) / 26) > 0, isnull(cm.B_CQ, 0) / 26 - square(isnull(cm.B_C, 0) / 26), 0))
         , isnull(cm.EMAIL_DELIV_13, 0), isnull(cm.EMAIL_OPEN_13, 0), isnull(cm.EMAIL_CLICK_13, 0), isnull(cm.EMAIL_UNSUB_26, 0), isnull(cm.SMS_DELIV_13, 0)
         , isnull(ep.CVM_TG_26, 0), isnull(ep.CVM_CG_26, 0), isnull(ep.CVM_RESP_26, 0), ep.CVM_TG_LAST_DAYS, isnull(ep.PP5_TG_26, 0)
         , ef2.FAV_CAT_N, datediff(day, ef.FAV_CAT_DT, p.D), ef2.FAV_PROD_N, datediff(day, ef.FAV_PROD_DT, p.D)
         , IIF(ef.FAV_PROD_DT IS NULL, NULL, isnull(fb.FAV_PROD_BUY_4, 0)), IIF(ef.FAV_PROD_DT IS NULL, NULL, isnull(fb.FAV_PROD_BUY_13, 0))
         , isnull(ef.COUPON_13, 0), isnull(ef.COUPON_26, 0)
         , isnull(kk.CAT_NORM_N, 0), isnull(kk.CAT_ACTIVE_6, 0), isnull(kk.CAT_DROP_N, 0), kk.CAT_DROP_SHARE
         , kk.CAT_MAXDROP, kk.CAT_MAXDROP_L6, kk.CAT_MAXDROP_BM, kk.CAT_MAXDROP_BSD, kk.CAT_MAXDROP_BW'
        + @gnames + N'
    FROM #pop AS p
         JOIN #a AS a ON a.SL = p.SL AND a.ID_CONTACT = p.ID_CONTACT AND a.ID_ORGANIZATION = p.ID_ORGANIZATION
         LEFT JOIN #f0 AS f ON f.ID_CONTACT = p.ID_CONTACT AND f.ID_ORGANIZATION = p.ID_ORGANIZATION
         LEFT JOIN #dy AS dy ON dy.SL = p.SL AND dy.ID_CONTACT = p.ID_CONTACT AND dy.ID_ORGANIZATION = p.ID_ORGANIZATION
         LEFT JOIN #rw AS r0 ON r0.E = 0 AND r0.SL = p.SL AND r0.ID_CONTACT = p.ID_CONTACT AND r0.ID_ORGANIZATION = p.ID_ORGANIZATION
         LEFT JOIN #rw AS r6 ON r6.E = 6 AND r6.SL = p.SL AND r6.ID_CONTACT = p.ID_CONTACT AND r6.ID_ORGANIZATION = p.ID_ORGANIZATION
         LEFT JOIN #oth AS o ON o.SL = p.SL AND o.ID_CONTACT = p.ID_CONTACT AND o.ID_ORGANIZATION = p.ID_ORGANIZATION
         LEFT JOIN #ms AS ms ON ms.SL = p.SL AND ms.ID_CONTACT = p.ID_CONTACT AND ms.ID_ORGANIZATION = p.ID_ORGANIZATION
         LEFT JOIN #st AS st ON st.SL = p.SL AND st.ID_SHOP = ms.MAIN_SHOP
         LEFT JOIN #ca AS ca ON ca.SL = p.SL AND ca.ID_CONTACT = p.ID_CONTACT AND ca.ID_ORGANIZATION = p.ID_ORGANIZATION
         LEFT JOIN #cm AS cm ON cm.SL = p.SL AND cm.ID_CONTACT = p.ID_CONTACT
         LEFT JOIN #ep AS ep ON ep.SL = p.SL AND ep.ID_CONTACT = p.ID_CONTACT AND ep.ID_ORGANIZATION = p.ID_ORGANIZATION
         LEFT JOIN #ef AS ef ON ef.SL = p.SL AND ef.ID_CONTACT = p.ID_CONTACT
         LEFT JOIN #ef2 AS ef2 ON ef2.SL = p.SL AND ef2.ID_CONTACT = p.ID_CONTACT
         LEFT JOIN #fb AS fb ON fb.SL = p.SL AND fb.ID_CONTACT = p.ID_CONTACT AND fb.ID_ORGANIZATION = p.ID_ORGANIZATION
         LEFT JOIN #kk AS kk ON kk.SL = p.SL AND kk.ID_CONTACT = p.ID_CONTACT AND kk.ID_ORGANIZATION = p.ID_ORGANIZATION
         LEFT JOIN #ct AS ct ON ct.ID_CONTACT = p.ID_CONTACT
         LEFT JOIN (' + @gpiv + N') AS gp (SL, ID_CONTACT, ID_ORGANIZATION' + @galias + N')
           ON gp.SL = p.SL AND gp.ID_CONTACT = p.ID_CONTACT AND gp.ID_ORGANIZATION = p.ID_ORGANIZATION;';

    EXEC sp_executesql @sql, N'@idc int, @rules_w int, @comm_w int', @idc = @idc, @rules_w = @rules_w, @comm_w = @comm_w;
    SET @n = @@ROWCOUNT;

    UPDATE dbo.I_CVM_CHURN_LOG SET FINISHED = sysdatetime(), ROWS_ = @n, STATUS = 'ok',
           MSG = CONCAT(N'срезов ', (SELECT count(*) FROM #sl), N', строк Н1 панели ', (SELECT count(*) FROM #h),
                        IIF(@gl IS NULL, N'', N'; группы без колонок в С1 (не записаны): ' + @gl))
    WHERE ID = @lid;
END
GO

-- ============================== 7. ВСЯ ИСТОРИЯ ==============================
-- Порядок: ожидание чужих тяжёлых историй (ценовые витрины, пересборка CLIENT_ITEM) → Н0 → недели Н1–Н4 от старых к новым →
-- Н5 → срезы пачками по 4 (размеченные: неделя среза + 6 ≤ последней закрытой). Недели и срезы, задевающие последние
-- 13 недель (горизонт перезаливки первой пятницы), считаются только при простое «MCI Regular update» и пересчитываются,
-- если задание стартовало во время расчёта. Посчитанное (журнал 'ok') не повторяется — перезапуск продолжает с места.
CREATE OR ALTER PROCEDURE dbo.CVM_CHURN_HISTORY
      @first_slice date = '20240922'   -- первый срез (воскресенье); окна 52 нед. и LFL год назад — от него
    , @force bit = 0
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @idc int = 1, @msg nvarchar(400), @t0 datetime2(0) = sysdatetime(), @lid int;
    INSERT dbo.I_CVM_CHURN_LOG (STEP, STARTED, STATUS) VALUES ('HIST', @t0, 'run');
    SET @lid = SCOPE_IDENTITY();

    -- 1. не совмещать с другими тяжёлыми историями (ждём до 24 ч)
    DECLARE @until datetime2(0) = dateadd(hour, 24, sysdatetime());
    WHILE EXISTS (SELECT 1 FROM msdb.dbo.sysjobactivity AS a (nolock) JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id
                  WHERE j.name = N'CVM_PRICE_MARTS_HISTORY' AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL
                    AND a.session_id = (SELECT max(session_id) FROM msdb.dbo.syssessions (nolock)))
       OR EXISTS (SELECT 1 FROM dbo.CVM_CLIENT_ITEM_LOG (nolock) WHERE STATUS = 'RUN' AND STARTED > dateadd(day, -3, sysdatetime()))
    BEGIN
        IF sysdatetime() >= @until
        BEGIN
            UPDATE dbo.I_CVM_CHURN_LOG SET FINISHED = sysdatetime(), STATUS = 'err', MSG = N'24 ч ждали конца истории ценовых витрин / CLIENT_ITEM' WHERE ID = @lid;
            THROW 50060, N'CVM_CHURN_HISTORY: 24 ч идёт история ценовых витрин или пересборка CLIENT_ITEM — история оттока не начата', 1;
        END
        WAITFOR DELAY '00:10:00';
    END

    -- 2. границы: последний день данных, последняя закрытая неделя, горизонт перезаливки
    DECLARE @max1 date = (SELECT max(DATA) FROM dbo.I_CHECKHEADER (nolock)
                          WHERE ID_COMPANY = @idc AND ID_ORGANIZATION = 1 AND DATA >= dateadd(day, -40, cast(getdate() AS date)));
    DECLARE @lcw int = (SELECT max(WEEKNUMBER) FROM (SELECT WEEKNUMBER FROM dbo.I_CALENDAR (nolock)
                                                     GROUP BY WEEKNUMBER HAVING max([DATE]) <= @max1) AS x);
    DECLARE @reload_w int = (SELECT WEEKNUMBER FROM dbo.I_CALENDAR (nolock) WHERE [DATE] = dateadd(day, -92, @max1));
    DECLARE @s_first int = (SELECT WEEKNUMBER FROM dbo.I_CALENDAR (nolock) WHERE [DATE] = @first_slice);
    DECLARE @w_h1 int    = (SELECT WEEKNUMBER FROM dbo.I_CALENDAR (nolock) WHERE [DATE] = dateadd(day, -406, @first_slice));
    DECLARE @w_rules int = (SELECT WEEKNUMBER FROM dbo.I_CALENDAR (nolock) WHERE [DATE] = dateadd(day, -182, @first_slice));
    DECLARE @w_h2 int    = (SELECT WEEKNUMBER FROM dbo.I_CALENDAR (nolock) WHERE [DATE] = dateadd(day, -224, @first_slice));
    DECLARE @w_all int   = @lcw - 32;
    IF @max1 IS NULL OR @lcw IS NULL OR @s_first IS NULL OR @w_h1 IS NULL
        THROW 50061, N'CVM_CHURN_HISTORY: не определились границы (последний день данных / недели I_CALENDAR)', 1;

    -- 3. Н0 — один раз
    IF @force = 1 OR NOT EXISTS (SELECT 1 FROM dbo.I_CVM_CHURN_LOG (nolock) WHERE STEP = 'H0' AND STATUS = 'ok')
        EXEC dbo.CVM_CHURN_FIRST_BUILD @mode = 'full';

    -- 4. слой недель
    DECLARE @w int = @w_h1, @parts varchar(5), @js1 datetime, @js2 datetime, @try int, @wait datetime2(0), @rl bit, @ac bit;
    WHILE @w <= @lcw
    BEGIN
        SET @parts = CONCAT(
              IIF(@force = 1 OR NOT EXISTS (SELECT 1 FROM dbo.I_CVM_CHURN_LOG (nolock) WHERE STEP = 'H1' AND WEEKNUMBER = @w AND STATUS = 'ok'), 'W', '')
            , IIF(@w >= @w_h2 AND (@force = 1 OR NOT EXISTS (SELECT 1 FROM dbo.I_CVM_CHURN_LOG (nolock) WHERE STEP IN ('H2', 'H2B') AND WEEKNUMBER = @w AND STATUS = 'ok')
                                   OR (@w >= @w_all AND NOT EXISTS (SELECT 1 FROM dbo.I_CVM_CHURN_LOG (nolock) WHERE STEP = 'H2B' AND WEEKNUMBER = @w AND STATUS = 'ok'))), 'C', '')
            , IIF(@w >= @w_h2 AND (@force = 1 OR NOT EXISTS (SELECT 1 FROM dbo.I_CVM_CHURN_LOG (nolock) WHERE STEP = 'H4' AND WEEKNUMBER = @w AND STATUS = 'ok')), 'M', ''));
        IF @parts <> ''
        BEGIN
            SET @try = 0;
            WHILE @try < 2
            BEGIN
                SET @try += 1;
                IF @w >= @reload_w
                BEGIN
                    SELECT @js1 = max(a.start_execution_date) FROM msdb.dbo.sysjobactivity AS a (nolock)
                         JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id WHERE j.name = N'MCI Regular update';
                    SET @wait = dateadd(hour, 14, sysdatetime());   -- ревью 1: было 6 ч, а «MCI Regular update» идёт ≈ 13 ч (05:30–18:40) — задание упало бы на ожидании
                    WHILE EXISTS (SELECT 1 FROM msdb.dbo.sysjobactivity AS a (nolock) JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id
                                  WHERE j.name = N'MCI Regular update' AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL
                                    AND a.session_id = (SELECT max(session_id) FROM msdb.dbo.syssessions (nolock)))
                    BEGIN
                        IF sysdatetime() >= @wait
                            THROW 50062, N'CVM_CHURN_HISTORY: «MCI Regular update» идёт дольше 14 ч — запустить историю ещё раз (продолжит с места)', 1;
                        WAITFOR DELAY '00:05:00';
                    END
                END
                SET @rl = IIF(@w >= @w_rules, 1, 0);
                SET @ac = IIF(@w >= @w_all, 1, 0);
                EXEC dbo.CVM_CHURN_WEEK_LAYER @w = @w, @parts = @parts, @rules = @rl, @all_cat = @ac;
                IF @w >= @reload_w
                BEGIN
                    SELECT @js2 = max(a.start_execution_date) FROM msdb.dbo.sysjobactivity AS a (nolock)
                         JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id WHERE j.name = N'MCI Regular update';
                    IF isnull(@js2, '19000101') = isnull(@js1, '19000101') BREAK;
                END
                ELSE BREAK;
            END
        END
        SET @w += 1;
    END

    -- 5. Н5
    IF @force = 1 OR NOT EXISTS (SELECT 1 FROM dbo.I_CVM_CHURN_LOG (nolock) WHERE STEP = 'H5' AND STATUS = 'ok')
        EXEC dbo.CVM_CHURN_EVENTS_BUILD;

    -- 6. срезы пачками по 4 соседних (размеченные)
    DECLARE @s int = @s_first, @s2 int;
    WHILE @s + 6 <= @lcw
    BEGIN
        SET @s2 = IIF(@s + 3 + 6 <= @lcw, @s + 3, @lcw - 6);
        IF @force = 1 OR NOT EXISTS (SELECT 1 FROM dbo.I_CVM_CHURN_LOG (nolock) WHERE STEP = 'C1' AND WEEKNUMBER = @s AND WEEK_TO = @s2 AND STATUS = 'ok')
        BEGIN
            IF @s2 + 6 >= @reload_w
            BEGIN
                SET @wait = dateadd(hour, 14, sysdatetime());   -- ревью 1: было 6 ч, а «MCI Regular update» идёт ≈ 13 ч (05:30–18:40) — задание упало бы на ожидании
                WHILE EXISTS (SELECT 1 FROM msdb.dbo.sysjobactivity AS a (nolock) JOIN msdb.dbo.sysjobs AS j (nolock) ON j.job_id = a.job_id
                              WHERE j.name = N'MCI Regular update' AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL
                                AND a.session_id = (SELECT max(session_id) FROM msdb.dbo.syssessions (nolock)))
                BEGIN
                    IF sysdatetime() >= @wait
                        THROW 50062, N'CVM_CHURN_HISTORY: «MCI Regular update» идёт дольше 14 ч — запустить историю ещё раз (продолжит с места)', 1;
                    WAITFOR DELAY '00:05:00';
                END
            END
            EXEC dbo.CVM_CHURN_SLICES @s1 = @s, @s2 = @s2, @lcw = @lcw;
        END
        SET @s = @s2 + 1;
    END

    UPDATE dbo.I_CVM_CHURN_LOG SET FINISHED = sysdatetime(), STATUS = 'ok',
           MSG = CONCAT(N'последний день данных ', convert(nvarchar(10), @max1, 104), N', последняя закрытая неделя ', @lcw,
                        N', срезов ', (SELECT count(DISTINCT WEEKNUMBER) FROM dbo.I_CVM_CHURN_SLICE (nolock)))
    WHERE ID = @lid;
END
GO

-- ============================== 8. ЗАДАНИЕ ИСТОРИИ: СТАРТ 07.11.2026 15:00 ==============================
-- Разовое задание SQL Agent; после успеха удаляется само; при ошибке остаётся — причина в I_CVM_CHURN_LOG и истории
-- задания, повторный запуск продолжает с места. Повтор этого файла: идущее или уже созданное задание не трогается;
-- история уже посчитана (журнал HIST 'ok') — задание не создаётся.
USE msdb;
GO
SET NOCOUNT ON;
DECLARE @job sysname = N'CVM_CHURN_HISTORY', @rc int = 0;

IF EXISTS (SELECT 1 FROM mci_model.dbo.I_CVM_CHURN_LOG (nolock) WHERE STEP = 'HIST' AND STATUS = 'ok')
BEGIN
    RAISERROR(N'История модели оттока уже посчитана — задание не создаю.', 10, 1);
    RETURN;
END
IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs (nolock) WHERE name = @job)
BEGIN
    RAISERROR(N'Задание CVM_CHURN_HISTORY уже есть — не трогаю.', 10, 1);
    RETURN;
END

EXEC @rc = msdb.dbo.sp_add_job @job_name = @job, @enabled = 1, @delete_level = 1,
     @description = N'Модель оттока, Т2: разовый расчёт всей истории витрины признаков (CVM_CHURN_FEATURES.sql); после успеха удаляется.';
IF @rc <> 0 BEGIN RAISERROR(N'СТОП: задание не создано (sp_add_job).', 16, 1); RETURN; END
EXEC @rc = msdb.dbo.sp_add_jobstep @job_name = @job, @step_name = N'history', @subsystem = N'TSQL',
     @database_name = N'mci_model', @command = N'exec dbo.CVM_CHURN_HISTORY;',
     @on_success_action = 1, @on_fail_action = 2;
IF @rc <> 0 BEGIN EXEC msdb.dbo.sp_delete_job @job_name = @job; RAISERROR(N'СТОП: шаг не создан — задание удалено.', 16, 1); RETURN; END
-- ревью 1: если файл применён позже 07.11.2026 15:00, разовое расписание в прошлом не сработало бы никогда — старт через 5 минут
DECLARE @st datetime = IIF(getdate() < '20261107 15:00', '20261107 15:00', dateadd(minute, 5, getdate())), @stm varchar(20);
-- параметры EXEC — только переменные и константы, не выражения
DECLARE @sd int = CONVERT(int, CONVERT(char(8), @st, 112)), @stt int = CONVERT(int, REPLACE(CONVERT(char(8), @st, 108), ':', ''));
EXEC @rc = msdb.dbo.sp_add_jobschedule @job_name = @job, @name = N'once_20261107_1500', @enabled = 1,
     @freq_type = 1, @active_start_date = @sd, @active_start_time = @stt;
IF @rc <> 0 BEGIN EXEC msdb.dbo.sp_delete_job @job_name = @job; RAISERROR(N'СТОП: расписание не создано — задание удалено.', 16, 1); RETURN; END
EXEC @rc = msdb.dbo.sp_add_jobserver @job_name = @job;
IF @rc <> 0 BEGIN EXEC msdb.dbo.sp_delete_job @job_name = @job; RAISERROR(N'СТОП: задание не привязано к серверу — удалено.', 16, 1); RETURN; END
SET @stm = CONVERT(varchar(16), @st, 120);
RAISERROR(N'Задание CVM_CHURN_HISTORY создано: старт %s.', 10, 1, @stm);
GO

SET NOEXEC OFF;
GO
