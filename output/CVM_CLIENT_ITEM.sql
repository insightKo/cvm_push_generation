-- ============================================================================================
-- CVM_CLIENT_ITEM — витрина покупок клиента по ДАТАМ: клиент × чек × товар по дням + ключи по дням (товар / категория CVM /
-- миссия) для конструктора («Покупки»: календарь «с — по» и «за всё время»).
-- Решение Елены 19.10.2026: витрина по датам — один раз загрузил историю, каждый день добавляешь и фильтруешь за любой период.
-- ОДИН СКРИПТ: запустить целиком в SSMS (F5) один раз. Создаёт таблицы и процедуры, ничего не загружает и не запускает;
-- загрузка — первичная (после F5, вечерами: EXEC client_item_load 'init' диапазонами дат, см. конец секции 6) и ночная
-- (шаг «MCI Regular update»). Шаг добавляется только когда I_CLIENT_ITEM_KEY_DAY уже не пустая и в журнале есть успешный init
-- с меткой: при первом F5 блок шага
-- остановится с сообщением; после первой вечерней загрузки запустить блок шага (секция 6, от USE msdb) ещё раз.
--
-- ПЕРВАЯ ДАТА ИСТОРИИ — сама: минимальная DATA в I_CHECKHEADER (секция 1.1, один раз при создании секций).
--           Задаёт только начальные секции; раньше можно догрузить позже (init сам добавит пустые секции назад).
--           Если функция секций уже есть от прошлой версии (59 нед.), @history_from не действует — секции назад добавит init.
--           Если I_CLIENT_ITEM_DAY уже заполнялась прошлой версией: ключей (KEY_DAY) за эти дни нет — перезалить их init
--           или собрать client_item_rebuild_keys за диапазон, который уже есть в I_CLIENT_ITEM_DAY.
--
-- ПЕРЕЧЕНЬ ДЕЙСТВИЙ ПО ОБЪЕКТАМ
--   dbo.I_CLIENT_ITEM_DAY            создаётся (если нет): строка = день × организация × клиент × чек × товар;
--                                    секции по дню (pf/ps_CLIENT_ITEM_DAY), кластерный колоночный индекс; хранится ВСЯ история,
--                                    очистки и MERGE старых секций нет; перезалитый день — TRUNCATE секции дня и вставка заново.
--   pf_CLIENT_ITEM_DAY / ps_CLIENT_ITEM_DAY  создаются (если нет): функция/схема секций по дню, от @history_from до сегодня + 30 дн.
--                                    (≈ 2 150 секций из лимита 15 000); одни и те же на обе таблицы (выровнены). Ночью — SPLIT пустых
--                                    будущих секций (на 21 день вперёд); при init с датой раньше первой границы — SPLIT назад
--                                    (только пустой первой секции, иначе ошибка).
--   dbo.I_CLIENT_ITEM_KEY_DAY        создаётся (если нет): день × организация × клиент × тип ключа (1 товар, 2 категория CVM,
--                                    3 миссия) × ключ → CHECKS (число разных чеков за день), SUMMA; секции по дню, кластерный
--                                    колоночный индекс, вставка отсортирована по (KEY_TYPE, ID_KEY). Сумма CHECKS по дням любого
--                                    диапазона дат = точное число чеков с ключом (чек целиком в одном дне).
--   dbo.CVM_CLIENT_ITEM_LOG          создаётся (если нет): журнал запусков (маленькая, не колоночная); если есть от прошлой версии —
--                                    добавляется колонка KEY_ROWS (старая PERIOD_ROWS остаётся, не используется).
--   dbo.CVM_CLIENT_ITEM_CAT_VER      создаётся (если нет): версия справочника категорий CVM, с которой собраны ключи дня (день → CAT_VER,
--                                    BUILT_AT); маленькая служебная (по строке на день), PRIMARY KEY по дню, НЕ колоночная. Строку дня
--                                    пишет client_item_keys_day в транзакции дня; читает вечерний CVM_CLIENT_ITEM_HISTORY.sql, чтобы
--                                    пересобрать категории по дням со старой версией справочника.
--   dbo.client_item_partitions       создаётся/заменяется: SPLIT будущих секций (и SPLIT назад при init); ничего не очищает и не сливает.
--   dbo.client_item_keys_day         создаётся/заменяется: внутренняя — ключи ОДНОГО дня из товарной таблицы: TRUNCATE секции дня
--                                    в KEY_DAY + вставка (I_CHECK не читает); затем отметка версии справочника категорий дня в
--                                    CVM_CLIENT_ITEM_CAT_VER (UPDATE/INSERT строки дня); вызывается из load и rebuild_keys внутри их
--                                    транзакции.
--   dbo.client_item_load             создаётся/заменяется: загрузка дней (режимы 'night' и 'init'); на день — одна транзакция:
--                                    TRUNCATE секции дня в DAY и в KEY_DAY, вставка товарной строки дня, затем ключей дня.
--   dbo.client_item_rebuild_keys     создаётся/заменяется: пересборка KEY_DAY за диапазон дат из товарной таблицы, по дню.
--   dbo.client_item_night            создаётся/заменяется: ночной вход = load('night') (KEY_DAY заполняется внутри).
--   msdb: задание «MCI Regular update» — вставляется шаг CLIENT_ITEM на место сразу после CVM_CONTACT (секция 6, отдельный
--         блок; следующие шаги сдвигаются на 1, переходы msdb пересчитывает сам); при успехе и при ошибке — следующий шаг.
--   I_CLIENT_ITEM_PERIOD, client_item_period (и рабочие I_CLIENT_ITEM_PERIOD_n/_old) — из прошлой версии, не используются;
--         скрипт их не создаёт и не трогает; удалить — только по слову Елены.
--   НЕ ТРОГАЕТСЯ И НЕ УДАЛЯЕТСЯ: I_CHECK, I_CHECKHEADER, I_PRODUCT, I_MISSION, service.loading_log (только чтение, NOLOCK),
--         все прочие таблицы и процедуры mci_model, остальные шаги задания.
--
-- ОБЪЁМ (для планирования диска и вечеров загрузки)
--   I_CLIENT_ITEM_DAY: ~3,9 млн строк в день (замер 19.10.2026) → за ~5,8 года (с 2021-01-01) порядка 8 млрд строк
--         (для ранних лет день, вероятно, меньше — оценка).
--   I_CLIENT_ITEM_KEY_DAY: оценка 5–6 млн строк в день (товары ≈ строк товарной + миссии; категории пока нет) → порядка 10 млрд.
--   Размер на диске — оценить по первым неделям загрузки (размер таблиц ÷ число дней × число дней истории).
--
-- ЗНАЧЕНИЯ
--   Основа — товары. Сумма = SUM(I_CHECK.COST_DISCOUNT), количество = SUM(I_CHECK.QUANTITY) по (день, чек, товар).
--   ID_ORGANIZATION: 1 офлайн, 3 e-com. ID_CONTACT <> 0. Табак НЕ исключается нигде (в отличие от CVM_CLUSTERS_VITRINA).
--   ID_CATEGORY_5_EXT — ext-категория товара из I_PRODUCT (числом, как EXT5_NUM в CVM_CLUSTERS_VITRINA).
--   Категории CVM (KEY_TYPE = 2) — по справочнику dbo.I_CVM_PRODUCT_CATEGORY (HOW > 0) при сборке ключей дня; колонка ID_CATEGORY_CVM не используется (NULL).
--   Версия справочника (CAT_VER) = CHECKSUM_AGG по строкам HOW > 0 от первых 4 байт SHA2_256(ID_PRODUCT bigint | ID_CVM_CATEGORY int |
--   HOW tinyint) — тем же строкам, что берут ключи дня (строки HOW = 0 на ключи не влияют, версию не меняют). Не CHECKSUM(колонки):
--   он для чисел раскладывается в XOR по колонкам, и перенос чётного числа товаров из одной категории в другую версию не меняет. Решение Елены 20.10.2026: после правки справочника категории по всей истории
--   пересобирает вечерний CVM_CLIENT_ITEM_HISTORY.sql по уже загруженной витрине (чеки не читаются).
--   Ключи дня (KEY_DAY): KEY_TYPE 1 — товар (ID_KEY = ID_PRODUCT); 2 — категория CVM (только ID_CATEGORY_CVM IS NOT NULL);
--   3 — миссия (ID_KEY = ID_MISSION): ext-категория товара → I_MISSION, товар учитывается во всех своих миссиях, «Служебное
--   (не миссия)» не берётся, чек считается один раз на миссию в день. Правило состава чека (MAIN >= COUNT_MAIN) не применяется
--   (решение Елены 19.10.2026). CHECKS — число разных ID_CHECK с ключом за день, SUMMA — сумма строк ключа за день.
--   Период в конструкторе — календарь «с — по» или «за всё время»: SUM(CHECKS) по DATA (запрос-шаблон — секция 8).
--   Ночной отбор дней: service.loading_log, table_name = N'I_CHECK_DELTA', строки с dt_load позже обработанной
--   (журнал: LOADING_DT) → дни min_date … max_date этих строк (до вчера; раньше первой секции истории — обрезаются по ней).
-- ============================================================================================

USE mci_model
GO

-- ============================== 0. ПРОВЕРКИ ==============================
SET NOCOUNT ON;

IF    COL_LENGTH('dbo.I_CHECK', 'DATA') IS NULL OR COL_LENGTH('dbo.I_CHECK', 'ID_COMPANY') IS NULL
   OR COL_LENGTH('dbo.I_CHECK', 'ID_ORGANIZATION') IS NULL OR COL_LENGTH('dbo.I_CHECK', 'ID_CONTACT') IS NULL
   OR COL_LENGTH('dbo.I_CHECK', 'ID_CHECK') IS NULL OR COL_LENGTH('dbo.I_CHECK', 'ID_PRODUCT') IS NULL
   OR COL_LENGTH('dbo.I_CHECK', 'COST_DISCOUNT') IS NULL OR COL_LENGTH('dbo.I_CHECK', 'QUANTITY') IS NULL
   OR COL_LENGTH('dbo.I_PRODUCT', 'ID_PRODUCT') IS NULL OR COL_LENGTH('dbo.I_PRODUCT', 'ID_CATEGORY_5_ext') IS NULL
   OR COL_LENGTH('dbo.I_MISSION', 'ID_MISSION') IS NULL OR COL_LENGTH('dbo.I_MISSION', 'MISSION') IS NULL
   OR COL_LENGTH('dbo.I_MISSION', 'ID_CATEGORY_5_ext') IS NULL OR COL_LENGTH('dbo.I_MISSION', 'ID_COMPANY') IS NULL
   OR COL_LENGTH('service.loading_log', 'table_name') IS NULL OR COL_LENGTH('service.loading_log', 'min_date') IS NULL
   OR COL_LENGTH('service.loading_log', 'max_date') IS NULL OR COL_LENGTH('service.loading_log', 'dt_load') IS NULL
BEGIN
    RAISERROR(N'СТОП: в источниках нет нужных колонок (I_CHECK / I_PRODUCT / I_MISSION / service.loading_log) — ничего не создаю.', 16, 1);
    SET NOEXEC ON;
END
GO

-- ============================== 1. ТАБЛИЦЫ ==============================
-- 1.1 Секции по дню: от @history_from до (сегодня + 30 дн.). Дальше границы двигает client_item_partitions.
IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = N'pf_CLIENT_ITEM_DAY')
BEGIN
    -- первая дата истории = минимальная дата чеков (один раз, при создании секций)
    DECLARE @history_from date;
    SELECT @history_from = CAST(MIN(h.DATA) AS date)
    FROM dbo.I_CHECKHEADER AS h WITH (NOLOCK)
    WHERE h.ID_COMPANY = 1 AND h.ID_ORGANIZATION IN (1, 3)
    OPTION (MAXDOP 4);

    IF @history_from IS NULL OR @history_from < '20100101' OR @history_from >= CAST(GETDATE() AS date)
    BEGIN
        RAISERROR(N'СТОП: минимальная дата чеков пустая или вне границ (2010-01-01 … вчера) — секции не созданы.', 16, 1);
        SET NOEXEC ON;
    END
    ELSE
    BEGIN
        DECLARE @n   int = DATEDIFF(day, @history_from, DATEADD(day, 30, CAST(GETDATE() AS date)));
        DECLARE @vals nvarchar(max);
        DECLARE @sql  nvarchar(max);

        SELECT @vals = STRING_AGG(CAST(QUOTENAME(CONVERT(char(10), DATEADD(day, t.k, @history_from), 23), '''') AS nvarchar(max)), N',')
                       WITHIN GROUP (ORDER BY t.k)
        FROM (SELECT TOP (@n + 1) k = ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1
              FROM sys.all_columns AS a CROSS JOIN sys.all_columns AS b) AS t;

        SET @sql = N'CREATE PARTITION FUNCTION pf_CLIENT_ITEM_DAY (date) AS RANGE RIGHT FOR VALUES (' + @vals + N');';
        EXEC sys.sp_executesql @sql;
    END
END
GO
-- схема — отдельной проверкой: если прошлый запуск оборвался после функции, схема всё равно создастся
IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = N'ps_CLIENT_ITEM_DAY')
BEGIN
    DECLARE @fg2  sysname = (SELECT TOP (1) name FROM sys.filegroups WHERE is_default = 1);
    DECLARE @sql2 nvarchar(max) = N'CREATE PARTITION SCHEME ps_CLIENT_ITEM_DAY AS PARTITION pf_CLIENT_ITEM_DAY ALL TO (' + QUOTENAME(@fg2) + N');';
    EXEC sys.sp_executesql @sql2;
END
GO

-- 1.2 Дневная таблица: строка = день × чек × товар (сумма и количество агрегированы по строкам чека)
IF OBJECT_ID('dbo.I_CLIENT_ITEM_DAY') IS NULL
BEGIN
    CREATE TABLE dbo.I_CLIENT_ITEM_DAY (
          DATA               date          NOT NULL
        , ID_COMPANY         int           NOT NULL
        , ID_ORGANIZATION    int           NOT NULL          -- 1 офлайн, 3 e-com
        , ID_CONTACT         int           NOT NULL          -- <> 0
        , ID_CHECK           bigint        NOT NULL
        , ID_PRODUCT         bigint        NOT NULL
        , SUMMA              decimal(18,2) NULL              -- SUM(COST_DISCOUNT)
        , QTY                decimal(18,3) NULL              -- SUM(QUANTITY)
        , ID_CATEGORY_5_EXT  bigint        NULL              -- из I_PRODUCT; не число → NULL
        , ID_CATEGORY_CVM    int           NULL              -- наша категория; справочник не утверждён — пока NULL
    ) ON ps_CLIENT_ITEM_DAY (DATA);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_CLIENT_ITEM_DAY ON dbo.I_CLIENT_ITEM_DAY ON ps_CLIENT_ITEM_DAY (DATA);
END
GO

-- 1.3 Ключи по дням (основа конструктора): те же секции по дню
IF OBJECT_ID('dbo.I_CLIENT_ITEM_KEY_DAY') IS NULL
BEGIN
    CREATE TABLE dbo.I_CLIENT_ITEM_KEY_DAY (
          DATA               date          NOT NULL
        , ID_COMPANY         int           NOT NULL
        , ID_ORGANIZATION    int           NOT NULL          -- 1 офлайн, 3 e-com
        , ID_CONTACT         int           NOT NULL
        , KEY_TYPE           tinyint       NOT NULL          -- 1 товар, 2 категория CVM, 3 миссия
        , ID_KEY             bigint        NOT NULL
        , CHECKS             int           NOT NULL          -- число разных чеков с ключом за день
        , SUMMA              decimal(18,2) NOT NULL          -- сумма строк ключа за день
    ) ON ps_CLIENT_ITEM_DAY (DATA);
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_CLIENT_ITEM_KEY_DAY ON dbo.I_CLIENT_ITEM_KEY_DAY ON ps_CLIENT_ITEM_DAY (DATA);
END
GO

-- 1.4 Журнал (маленькая служебная, точечные UPDATE по ключу — без колоночного индекса)
IF OBJECT_ID('dbo.CVM_CLIENT_ITEM_LOG') IS NULL
    CREATE TABLE dbo.CVM_CLIENT_ITEM_LOG (
          ID           int IDENTITY(1,1) NOT NULL PRIMARY KEY
        , STARTED      datetime2(0)  NOT NULL
        , FINISHED     datetime2(0)  NULL
        , MODE         varchar(10)   NOT NULL                -- night / init / keys / mark (ручная метка)
        , DATE_FROM    date          NULL                    -- границы обработанных дней
        , DATE_TO      date          NULL
        , DAYS_DONE    int           NULL                    -- дней пересчитано
        , ROWS_        bigint        NULL                    -- строк вставлено в дневную таблицу
        , KEY_ROWS     bigint        NULL                    -- строк вставлено в I_CLIENT_ITEM_KEY_DAY
        , LOADING_DT   datetime2(7)  NULL                    -- обработанный dt_load из service.loading_log (метка прогресса;
                                                             -- datetime2(7) — без округления метки, иначе последняя загрузка
                                                             -- может браться повторно)
        , STATUS       varchar(10)   NOT NULL                -- RUN / OK / ERROR
        , ERROR_MSG    nvarchar(2000) NULL
    );
GO
-- журнал от прошлой версии: добавить колонку (отдельным пакетом — процедуры ниже ссылаются на неё)
IF COL_LENGTH('dbo.CVM_CLIENT_ITEM_LOG', 'KEY_ROWS') IS NULL
    ALTER TABLE dbo.CVM_CLIENT_ITEM_LOG ADD KEY_ROWS bigint NULL;
GO

-- 1.5 Версия справочника категорий по дням (маленькая служебная, точечные UPDATE по дню — без колоночного индекса)
IF OBJECT_ID('dbo.CVM_CLIENT_ITEM_CAT_VER') IS NULL
    CREATE TABLE dbo.CVM_CLIENT_ITEM_CAT_VER (
          DATA      date         NOT NULL PRIMARY KEY
        , CAT_VER   int          NOT NULL                      -- версия справочника, с которой собраны ключи дня
        , BUILT_AT  datetime2(0) NULL                          -- когда собраны
    );
GO

-- ============================== 2. СЕКЦИИ: SPLIT БУДУЩИХ И НАЗАД ==============================
-- Ничего не очищает и не сливает: история хранится вся.
-- @from_date (только init): если раньше первой границы — добавляются пустые секции назад до этой даты (SPLIT пустой первой секции).
CREATE OR ALTER PROCEDURE dbo.client_item_partitions
      @ahead_days int  = 21        -- пустых секций вперёд
    , @from_date  date = NULL      -- первая дата, которую собираются грузить
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @today date = CAST(GETDATE() AS date);
    DECLARE @target date = DATEADD(day, @ahead_days, @today);
    DECLARE @fg sysname = (SELECT TOP (1) name FROM sys.filegroups WHERE is_default = 1);
    DECLARE @mx date, @mn date, @b date, @sql nvarchar(max), @cnt int;

    -- SPLIT вперёд: добавить границы до @target
    SELECT @mx = MAX(CAST(v.value AS date)), @mn = MIN(CAST(v.value AS date)), @cnt = COUNT(*)
    FROM sys.partition_range_values AS v
    JOIN sys.partition_functions AS f ON f.function_id = v.function_id
    WHERE f.name = N'pf_CLIENT_ITEM_DAY';

    WHILE @mx < @target
    BEGIN
        SET @mx = DATEADD(day, 1, @mx);
        SET @sql = N'ALTER PARTITION SCHEME ps_CLIENT_ITEM_DAY NEXT USED ' + QUOTENAME(@fg) + N';';
        EXEC sys.sp_executesql @sql;
        ALTER PARTITION FUNCTION pf_CLIENT_ITEM_DAY () SPLIT RANGE (@mx);
        SET @cnt += 1;
    END

    -- SPLIT назад: только пустая первая секция (всё, что раньше первой границы) — метаданные, без перемещения данных
    IF @from_date IS NOT NULL AND @from_date < @mn
    BEGIN
        IF @from_date < '20100101'
            THROW 50008, N'client_item_partitions: дата раньше 2010-01-01 — похоже на опечатку, секции не добавлены.', 1;
        IF @cnt + DATEDIFF(day, @from_date, @mn) > 14500
            THROW 50009, N'client_item_partitions: секций вышло бы больше 14 500 (лимит 15 000) — секции не добавлены.', 1;
        IF EXISTS (SELECT 1 FROM dbo.I_CLIENT_ITEM_DAY WITH (NOLOCK) WHERE DATA < @mn)
           OR EXISTS (SELECT 1 FROM dbo.I_CLIENT_ITEM_KEY_DAY WITH (NOLOCK) WHERE DATA < @mn)
            THROW 50010, N'client_item_partitions: в первой секции есть строки — SPLIT назад небезопасен, секции не добавлены.', 1;

        SET @b = @mn;
        WHILE @b > @from_date
        BEGIN
            SET @b = DATEADD(day, -1, @b);
            SET @sql = N'ALTER PARTITION SCHEME ps_CLIENT_ITEM_DAY NEXT USED ' + QUOTENAME(@fg) + N';';
            EXEC sys.sp_executesql @sql;
            ALTER PARTITION FUNCTION pf_CLIENT_ITEM_DAY () SPLIT RANGE (@b);
        END
    END
END
GO

-- ============================== 3. КЛЮЧИ ОДНОГО ДНЯ (внутренняя) ==============================
-- Вызывается внутри транзакции дня (load) или rebuild_keys. Замок и транзакцию не берёт. Читает только I_CLIENT_ITEM_DAY.
CREATE OR ALTER PROCEDURE dbo.client_item_keys_day
      @d    date
    , @rows bigint OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @idc int = 1, @pn int, @sql nvarchar(max), @ver int;

    -- у дня должна быть своя секция (иначе TRUNCATE секции стёр бы соседние дни)
    IF $PARTITION.pf_CLIENT_ITEM_DAY(@d) = 1
       OR $PARTITION.pf_CLIENT_ITEM_DAY(@d) = $PARTITION.pf_CLIENT_ITEM_DAY(DATEADD(day, 1, @d))
       OR $PARTITION.pf_CLIENT_ITEM_DAY(@d) = $PARTITION.pf_CLIENT_ITEM_DAY(DATEADD(day, -1, @d))
        THROW 50005, N'client_item_keys_day: у дня нет своей секции — ключи не пересобраны (проверить pf_CLIENT_ITEM_DAY).', 1;

    -- миссия ← ext-категория (товар во всех своих миссиях; служебное — не миссия); сотни строк;
    -- TRY_CAST: нечисловой код в I_MISSION не роняет шаг (в дневной таблице такие коды тоже NULL)
    DROP TABLE IF EXISTS #miss;
    SELECT DISTINCT EXT5 = TRY_CAST(m.ID_CATEGORY_5_ext AS bigint), m.ID_MISSION
    INTO #miss
    FROM dbo.I_MISSION AS m WITH (NOLOCK)
    WHERE m.ID_COMPANY = @idc AND m.MISSION <> N'Служебное (не миссия)' AND m.ID_MISSION <> 999
      AND TRY_CAST(m.ID_CATEGORY_5_ext AS bigint) IS NOT NULL;
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_miss ON #miss;

    -- категория CVM ← товар (справочник нашей категории; HOW > 0 — товар сопоставлен); ~312 тыс. строк, один раз на вызов
    -- ID_PRODUCT bigint и первичный ключ справочника — без DISTINCT и без CAST; ID_CVM_CATEGORY int → bigint под ID_KEY
    DROP TABLE IF EXISTS #kd_cat;
    SELECT pc.ID_PRODUCT, ID_CAT = CAST(pc.ID_CVM_CATEGORY AS bigint)
         , CHK = CAST(SUBSTRING(HASHBYTES('SHA2_256', CAST(pc.ID_PRODUCT AS binary(8)) + CAST(pc.ID_CVM_CATEGORY AS binary(4))
                                                       + CAST(pc.[HOW] AS binary(1))), 1, 4) AS int)   -- для версии справочника
    INTO #kd_cat
    FROM dbo.I_CVM_PRODUCT_CATEGORY AS pc WITH (NOLOCK)
    WHERE pc.[HOW] > 0;
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_kd_cat ON #kd_cat;
    -- версия справочника — один раз на вызов, по тем же строкам, что пошли в ключи (совпадает с расчётом в CVM_CLIENT_ITEM_HISTORY.sql)
    SELECT @ver = ISNULL(CHECKSUM_AGG(CHK), 0) FROM #kd_cat;

    SET @pn = $PARTITION.pf_CLIENT_ITEM_DAY(@d);
    SET @sql = CONCAT(N'TRUNCATE TABLE dbo.I_CLIENT_ITEM_KEY_DAY WITH (PARTITIONS (', @pn, N'));');
    EXEC sys.sp_executesql @sql;

    -- один оператор, сортировка по (тип ключа, ключ) внутри секции, один поток
    INSERT INTO dbo.I_CLIENT_ITEM_KEY_DAY WITH (TABLOCK)
           (DATA, ID_COMPANY, ID_ORGANIZATION, ID_CONTACT, KEY_TYPE, ID_KEY, CHECKS, SUMMA)
    SELECT k.DATA, k.ID_COMPANY, k.ID_ORGANIZATION, k.ID_CONTACT, k.KEY_TYPE, k.ID_KEY, k.CHECKS, k.SUMMA
    FROM (
        -- 1. товары: строка дневной таблицы = один чек × товар, поэтому число чеков = число строк
        SELECT DATA = @d, ID_COMPANY = @idc, d.ID_ORGANIZATION, d.ID_CONTACT, KEY_TYPE = CAST(1 AS tinyint)
             , ID_KEY = CAST(d.ID_PRODUCT AS bigint), CHECKS = COUNT(*), SUMMA = ISNULL(SUM(d.SUMMA), 0)
        FROM dbo.I_CLIENT_ITEM_DAY AS d WITH (NOLOCK)
        WHERE d.DATA = @d AND d.ID_COMPANY = @idc
        GROUP BY d.ID_ORGANIZATION, d.ID_CONTACT, d.ID_PRODUCT

        UNION ALL
        -- 2. категории CVM: товар → категория по справочнику; чек считается один раз на категорию в день
        SELECT @d, @idc, g.ID_ORGANIZATION, g.ID_CONTACT, CAST(2 AS tinyint), g.ID_CAT
             , COUNT(*), ISNULL(SUM(g.SUMMA), 0)
        FROM (SELECT d.ID_ORGANIZATION, d.ID_CONTACT, d.ID_CHECK, c.ID_CAT, SUMMA = SUM(d.SUMMA)
              FROM dbo.I_CLIENT_ITEM_DAY AS d WITH (NOLOCK)
              JOIN #kd_cat AS c ON c.ID_PRODUCT = d.ID_PRODUCT              -- оба bigint
              WHERE d.DATA = @d AND d.ID_COMPANY = @idc
              GROUP BY d.ID_ORGANIZATION, d.ID_CONTACT, d.ID_CHECK, c.ID_CAT) AS g
        GROUP BY g.ID_ORGANIZATION, g.ID_CONTACT, g.ID_CAT

        UNION ALL
        -- 3. миссии: товар → миссия по ext-категории; чек считается один раз на миссию
        SELECT @d, @idc, g.ID_ORGANIZATION, g.ID_CONTACT, CAST(3 AS tinyint), CAST(g.ID_MISSION AS bigint)
             , COUNT(*), ISNULL(SUM(g.SUMMA), 0)
        FROM (SELECT d.ID_ORGANIZATION, d.ID_CONTACT, d.ID_CHECK, m.ID_MISSION, SUMMA = SUM(d.SUMMA)
              FROM dbo.I_CLIENT_ITEM_DAY AS d WITH (NOLOCK)
              JOIN #miss AS m ON m.EXT5 = d.ID_CATEGORY_5_EXT
              WHERE d.DATA = @d AND d.ID_COMPANY = @idc
              GROUP BY d.ID_ORGANIZATION, d.ID_CONTACT, d.ID_CHECK, m.ID_MISSION) AS g
        GROUP BY g.ID_ORGANIZATION, g.ID_CONTACT, g.ID_MISSION
    ) AS k
    ORDER BY k.KEY_TYPE, k.ID_KEY
    OPTION (MAXDOP 1, RECOMPILE);   -- план под объём своего дня (иначе кеш от малого дня — сбросы в tempdb)
    SET @rows = @@ROWCOUNT;

    -- отметка версии справочника дня (транзакция дня — у вызывающей процедуры; один вызов на день под замком витрины)
    UPDATE dbo.CVM_CLIENT_ITEM_CAT_VER SET CAT_VER = @ver, BUILT_AT = SYSDATETIME() WHERE DATA = @d;
    IF @@ROWCOUNT = 0
        INSERT dbo.CVM_CLIENT_ITEM_CAT_VER (DATA, CAT_VER, BUILT_AT) VALUES (@d, @ver, SYSDATETIME());
END
GO

-- ============================== 4. ЗАГРУЗКА ДНЕЙ ==============================
-- @mode = 'night' — дни из service.loading_log с dt_load позже обработанной (требуется предыдущая первичная загрузка);
-- @mode = 'init'  — дни @date_from … @date_to, ОБЯЗАТЕЛЬНЫЕ (история грузится диапазонами за несколько вечеров; повтор диапазона
--                   безопасен — день перезаливается целиком). Если @date_from раньше первой секции — секции добавляются назад.
-- Метка прогресса (LOADING_DT) при 'init' ставится один раз — первым прогоном, пока в журнале её нет.
CREATE OR ALTER PROCEDURE dbo.client_item_load
      @mode      varchar(10)
    , @date_from date = NULL
    , @date_to   date = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @idc int = 1;
    DECLARE @today date = CAST(GETDATE() AS date);
    DECLARE @yesterday date = DATEADD(day, -1, @today);
    DECLARE @log_id int, @last_dt datetime2(7), @max_dt datetime2(7), @marker datetime2(7) = NULL, @lk int;
    DECLARE @ndays int = 0, @rows bigint = 0, @n bigint, @krows bigint = 0, @kn bigint;
    DECLARE @ch int, @nch int, @rn int, @rmax int, @d date, @pn int, @cmin date, @cmax date, @first date;
    DECLARE @sql nvarchar(max), @msg nvarchar(2000);

    IF @mode NOT IN ('night', 'init')
        THROW 50001, N'client_item_load: режим должен быть night или init.', 1;

    INSERT dbo.CVM_CLIENT_ITEM_LOG (STARTED, MODE, DATE_FROM, DATE_TO, STATUS)
    VALUES (SYSDATETIME(), @mode, @date_from, @date_to, 'RUN');
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY
        -- одна загрузка или пересборка за раз (вечерний init и ночной шаг не пересекаются: SPLIT секций под чужой
        -- загрузкой сдвинул бы номера секций). Занято — сразу ошибка: ночью шаг уходит дальше, метка не сдвигается,
        -- дни подберёт следующий прогон.
        EXEC @lk = sys.sp_getapplock @Resource = N'CVM_CLIENT_ITEM', @LockMode = N'Exclusive', @LockOwner = N'Session', @LockTimeout = 0;
        IF @lk < 0
            THROW 50004, N'client_item_load: уже идёт загрузка дней или пересборка ключей витрины покупок — прогон пропущен.', 1;

        IF @mode = 'init'
        BEGIN
            IF @date_from IS NULL OR @date_to IS NULL
                THROW 50006, N'client_item_load: в режиме init обязательны @date_from и @date_to.', 1;
            IF @date_to > @yesterday SET @date_to = @yesterday;
            IF @date_from > @date_to
                THROW 50006, N'client_item_load: пустой диапазон (@date_from позже @date_to или позже вчера) — ничего не загружено.', 1;
            IF DATEDIFF(day, @date_from, @date_to) > 3999
                THROW 50006, N'client_item_load: диапазон init больше 4000 дней — разбить на части.', 1;
            EXEC dbo.client_item_partitions @from_date = @date_from;     -- при необходимости — SPLIT назад
        END
        ELSE
            EXEC dbo.client_item_partitions;

        SELECT @max_dt = MAX(dt_load) FROM service.loading_log WITH (NOLOCK) WHERE table_name = N'I_CHECK_DELTA';
        SELECT @last_dt = MAX(LOADING_DT) FROM dbo.CVM_CLIENT_ITEM_LOG WITH (NOLOCK) WHERE STATUS = 'OK' AND LOADING_DT IS NOT NULL;

        -- ---- список дней
        DROP TABLE IF EXISTS #days;
        CREATE TABLE #days (DATA date NOT NULL, RN int NOT NULL, CH int NOT NULL DEFAULT 0);   -- до 4000 строк

        IF @mode = 'night'
        BEGIN
            IF @last_dt IS NULL
                THROW 50002, N'client_item_load: нет первичной загрузки (в журнале нет обработанной метки) — сначала режим init.', 1;
            IF @max_dt IS NULL OR @max_dt <= @last_dt
            BEGIN
                SET @marker = @last_dt;                                    -- новых записей нет
            END
            ELSE
            BEGIN
                SET @marker = @max_dt;

                -- первая граница секций: раньше неё дни не грузятся (в секциях истории их нет)
                SELECT @first = MIN(CAST(v.value AS date))
                FROM sys.partition_range_values AS v
                JOIN sys.partition_functions AS f ON f.function_id = v.function_id
                WHERE f.name = N'pf_CLIENT_ITEM_DAY';

                -- диапазоны перезалитых загрузок, обрезанные первой секцией и вчерашним днём
                -- (min_date/max_date приводятся к date: если в журнале они со временем, DISTINCT не задвоит день)
                DROP TABLE IF EXISTS #rng;
                CREATE TABLE #rng (lo date NOT NULL, hi date NOT NULL);       -- десятки строк
                INSERT #rng (lo, hi)
                SELECT lo = CASE WHEN CAST(l.min_date AS date) < @first THEN @first ELSE CAST(l.min_date AS date) END
                     , hi = CASE WHEN CAST(l.max_date AS date) > @yesterday THEN @yesterday ELSE CAST(l.max_date AS date) END
                FROM service.loading_log AS l WITH (NOLOCK)
                WHERE l.table_name = N'I_CHECK_DELTA' AND l.min_date IS NOT NULL AND l.max_date IS NOT NULL
                  AND l.dt_load > @last_dt AND l.dt_load <= @max_dt;
                DELETE #rng WHERE lo > hi;

                IF EXISTS (SELECT 1 FROM #rng WHERE DATEDIFF(day, lo, hi) > 3999)
                    THROW 50007, N'client_item_load: перезаливка I_CHECK шире 4000 дней — ночной прогон не берётся, загрузить диапазоны вручную (init), затем сдвинуть метку (шаблон в секции 6).', 1;

                ;WITH nums AS (
                    SELECT TOP (4000) k = ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1
                    FROM sys.all_columns AS a CROSS JOIN sys.all_columns AS b
                ), dd AS (
                    SELECT DISTINCT DATA = DATEADD(day, nums.k, r.lo)
                    FROM #rng AS r JOIN nums ON nums.k <= DATEDIFF(day, r.lo, r.hi)
                )
                INSERT #days (DATA, RN)
                SELECT DATA, ROW_NUMBER() OVER (ORDER BY DATA) FROM dd;
            END
        END
        ELSE
        BEGIN
            -- метку прогресса ставит первый init-прогон (как только в журнале её нет)
            IF @last_dt IS NULL SET @marker = @max_dt;
            ;WITH nums AS (
                SELECT TOP (4000) k = ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1
                FROM sys.all_columns AS a CROSS JOIN sys.all_columns AS b
            )
            INSERT #days (DATA, RN)
            SELECT DATEADD(day, k, @date_from), ROW_NUMBER() OVER (ORDER BY k)
            FROM nums WHERE k <= DATEDIFF(day, @date_from, @date_to);
        END

        SELECT @ndays = COUNT(*), @date_from = MIN(DATA), @date_to = MAX(DATA) FROM #days;

        -- у каждого дня — своя секция (граница и на самом дне, и на следующем); иначе TRUNCATE секции стёр бы соседние дни
        IF EXISTS (SELECT 1 FROM #days
                   WHERE $PARTITION.pf_CLIENT_ITEM_DAY(DATA) = 1
                      OR $PARTITION.pf_CLIENT_ITEM_DAY(DATA) = $PARTITION.pf_CLIENT_ITEM_DAY(DATEADD(day, 1, DATA))
                      OR $PARTITION.pf_CLIENT_ITEM_DAY(DATA) = $PARTITION.pf_CLIENT_ITEM_DAY(DATEADD(day, -1, DATA)))
            THROW 50005, N'client_item_load: у дня нет своей секции — ничего не загружено (проверить pf_CLIENT_ITEM_DAY).', 1;

        -- порции: подряд идущие дни, не больше 7 в порции. Разрыв в списке (перезаливки разных месяцев) — новая порция,
        -- чтобы I_CHECK не читался за дни между ними (G — номер «острова» подряд идущих дней)
        ;WITH a AS (SELECT DATA, G = DATEDIFF(day, '20000101', DATA) - RN FROM #days)
        , b AS (SELECT DATA, G, K = (ROW_NUMBER() OVER (PARTITION BY G ORDER BY DATA) - 1) / 7 FROM a)
        , c AS (SELECT DATA, CH = DENSE_RANK() OVER (ORDER BY G, K) FROM b)
        UPDATE dd SET dd.CH = c.CH
        FROM #days AS dd
        JOIN c ON c.DATA = dd.DATA;

        -- ---- справочник товаров: ext-категория (одна строка на товар)
        DROP TABLE IF EXISTS #prod;
        SELECT ID_PRODUCT = CAST(p.ID_PRODUCT AS bigint), EXT5 = MAX(TRY_CAST(p.ID_CATEGORY_5_ext AS bigint))
        INTO #prod
        FROM dbo.I_PRODUCT AS p WITH (NOLOCK)
        GROUP BY CAST(p.ID_PRODUCT AS bigint);
        CREATE CLUSTERED COLUMNSTORE INDEX CCI_prod ON #prod;

        -- ---- порции: до 7 подряд идущих дней; I_CHECK читается один раз на порцию
        DROP TABLE IF EXISTS #chunk;
        CREATE TABLE #chunk (DATA date NOT NULL, ID_ORGANIZATION int NOT NULL, ID_CONTACT int NOT NULL, ID_CHECK bigint NOT NULL,
                             ID_PRODUCT bigint NOT NULL, SUMMA decimal(18,2) NULL, QTY decimal(18,3) NULL, EXT5 bigint NULL);
        CREATE CLUSTERED COLUMNSTORE INDEX CCI_chunk ON #chunk;

        SET @nch = (SELECT ISNULL(MAX(CH), 0) FROM #days);
        SET @ch = 1;
        WHILE @ch <= @nch
        BEGIN
            SELECT @cmin = MIN(DATA), @cmax = MAX(DATA), @rn = MIN(RN), @rmax = MAX(RN) FROM #days WHERE CH = @ch;

            -- дни порции идут подряд, поэтому хватает диапазона DATA (без join к #days по CAST(c.DATA AS date))
            TRUNCATE TABLE #chunk;
            INSERT INTO #chunk WITH (TABLOCK) (DATA, ID_ORGANIZATION, ID_CONTACT, ID_CHECK, ID_PRODUCT, SUMMA, QTY, EXT5)
            SELECT x.DATA, x.ID_ORGANIZATION, x.ID_CONTACT, x.ID_CHECK, x.ID_PRODUCT, x.SUMMA, x.QTY, p.EXT5
            FROM (SELECT DATA = CAST(c.DATA AS date), c.ID_ORGANIZATION, c.ID_CONTACT, c.ID_CHECK, ID_PRODUCT = CAST(c.ID_PRODUCT AS bigint)
                       , SUMMA = SUM(c.COST_DISCOUNT), QTY = SUM(c.QUANTITY)
                  FROM dbo.I_CHECK AS c WITH (NOLOCK)
                  WHERE c.ID_COMPANY = @idc AND c.ID_ORGANIZATION IN (1, 3)
                    AND c.ID_CONTACT <> 0
                    AND c.DATA >= @cmin AND c.DATA < DATEADD(day, 1, @cmax)
                  GROUP BY CAST(c.DATA AS date), c.ID_ORGANIZATION, c.ID_CONTACT, c.ID_CHECK, CAST(c.ID_PRODUCT AS bigint)) AS x
            LEFT JOIN #prod AS p ON p.ID_PRODUCT = x.ID_PRODUCT;

            -- по дню: одна транзакция «очистить секцию дня в обеих таблицах + вставить товарную строку дня + ключи дня»
            -- (день либо старый, либо новый целиком в обеих таблицах); номер секции берётся в момент очистки
            WHILE @rn <= @rmax
            BEGIN
                SELECT @d = DATA FROM #days WHERE RN = @rn;
                BEGIN TRANSACTION;
                SET @pn = $PARTITION.pf_CLIENT_ITEM_DAY(@d);
                SET @sql = CONCAT(N'TRUNCATE TABLE dbo.I_CLIENT_ITEM_DAY WITH (PARTITIONS (', @pn, N'));');
                EXEC sys.sp_executesql @sql;
                -- сортировка по товару внутри секции, один поток
                -- (после утверждения справочника категорий: LEFT JOIN к его таблице вместо NULL в ID_CATEGORY_CVM)
                INSERT INTO dbo.I_CLIENT_ITEM_DAY WITH (TABLOCK)
                       (DATA, ID_COMPANY, ID_ORGANIZATION, ID_CONTACT, ID_CHECK, ID_PRODUCT, SUMMA, QTY, ID_CATEGORY_5_EXT, ID_CATEGORY_CVM)
                SELECT DATA, @idc, ID_ORGANIZATION, ID_CONTACT, ID_CHECK, ID_PRODUCT, SUMMA, QTY, EXT5, NULL
                FROM #chunk
                WHERE DATA = @d
                ORDER BY ID_PRODUCT
                OPTION (MAXDOP 1);
                SET @n = @@ROWCOUNT;
                -- ключи дня сразу после товарной строки дня (TRUNCATE секции дня в KEY_DAY + вставка внутри)
                EXEC dbo.client_item_keys_day @d = @d, @rows = @kn OUTPUT;
                COMMIT TRANSACTION;
                SET @rows += @n;
                SET @krows += @kn;
                SET @rn += 1;
            END
            SET @ch += 1;
        END

        UPDATE dbo.CVM_CLIENT_ITEM_LOG
        SET FINISHED = SYSDATETIME(), DATE_FROM = @date_from, DATE_TO = @date_to, DAYS_DONE = @ndays, ROWS_ = @rows,
            KEY_ROWS = @krows, LOADING_DT = @marker, STATUS = 'OK'
        WHERE ID = @log_id;

        EXEC sys.sp_releaseapplock @Resource = N'CVM_CLIENT_ITEM', @LockOwner = N'Session';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        IF APPLOCK_MODE(N'public', N'CVM_CLIENT_ITEM', N'Session') <> N'NoLock'
            EXEC sys.sp_releaseapplock @Resource = N'CVM_CLIENT_ITEM', @LockOwner = N'Session';
        SET @msg = LEFT(ERROR_MESSAGE(), 2000);
        UPDATE dbo.CVM_CLIENT_ITEM_LOG
        SET FINISHED = SYSDATETIME(), DAYS_DONE = @ndays, ROWS_ = @rows, KEY_ROWS = @krows, STATUS = 'ERROR', ERROR_MSG = @msg
        WHERE ID = @log_id;
        THROW;
    END CATCH
END
GO

-- ============================== 5. ПЕРЕСБОРКА КЛЮЧЕЙ ЗА ДИАПАЗОН ==============================
-- Ключи (KEY_DAY) за @date_from … @date_to заново из I_CLIENT_ITEM_DAY, по дню (TRUNCATE секции дня + вставка, одна транзакция
-- на день). I_CHECK не читается. Нужна после заполнения ID_CATEGORY_CVM по справочнику (секция 7). Товарную таблицу не меняет.
CREATE OR ALTER PROCEDURE dbo.client_item_rebuild_keys
      @date_from date
    , @date_to   date
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @d date, @ndays int = 0, @krows bigint = 0, @kn bigint, @log_id int, @lk int, @msg nvarchar(2000);

    IF @date_from IS NULL OR @date_to IS NULL OR @date_from > @date_to
        THROW 50006, N'client_item_rebuild_keys: нужны @date_from и @date_to, @date_from не позже @date_to.', 1;

    INSERT dbo.CVM_CLIENT_ITEM_LOG (STARTED, MODE, DATE_FROM, DATE_TO, STATUS)
    VALUES (SYSDATETIME(), 'keys', @date_from, @date_to, 'RUN');
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY
        -- один замок с загрузкой дней; занято — сразу ошибка, ключи прежние
        EXEC @lk = sys.sp_getapplock @Resource = N'CVM_CLIENT_ITEM', @LockMode = N'Exclusive', @LockOwner = N'Session', @LockTimeout = 0;
        IF @lk < 0
            THROW 50004, N'client_item_rebuild_keys: уже идёт загрузка дней или пересборка ключей витрины покупок — пересборка пропущена.', 1;

        SET @d = @date_from;
        WHILE @d <= @date_to
        BEGIN
            BEGIN TRANSACTION;
            EXEC dbo.client_item_keys_day @d = @d, @rows = @kn OUTPUT;     -- сам проверит, что у дня своя секция
            COMMIT TRANSACTION;
            SET @krows += @kn;
            SET @ndays += 1;
            SET @d = DATEADD(day, 1, @d);
        END

        UPDATE dbo.CVM_CLIENT_ITEM_LOG
        SET FINISHED = SYSDATETIME(), DAYS_DONE = @ndays, KEY_ROWS = @krows, STATUS = 'OK'
        WHERE ID = @log_id;

        EXEC sys.sp_releaseapplock @Resource = N'CVM_CLIENT_ITEM', @LockOwner = N'Session';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        IF APPLOCK_MODE(N'public', N'CVM_CLIENT_ITEM', N'Session') <> N'NoLock'
            EXEC sys.sp_releaseapplock @Resource = N'CVM_CLIENT_ITEM', @LockOwner = N'Session';
        SET @msg = LEFT(ERROR_MESSAGE(), 2000);
        UPDATE dbo.CVM_CLIENT_ITEM_LOG
        SET FINISHED = SYSDATETIME(), DAYS_DONE = @ndays, KEY_ROWS = @krows, STATUS = 'ERROR', ERROR_MSG = @msg
        WHERE ID = @log_id;
        THROW;
    END CATCH
END
GO

-- ============================== 6. НОЧНОЙ ВХОД И ШАГ ЗАДАНИЯ ==============================
CREATE OR ALTER PROCEDURE dbo.client_item_night
AS
BEGIN
    SET NOCOUNT ON;
    EXEC dbo.client_item_load @mode = 'night';      -- ключи дня (KEY_DAY) заполняются внутри
END
GO

SET NOEXEC OFF;
GO

-- Первичная загрузка истории (вечером, когда задание не идёт; первый прогон ставит метку для ночного режима). Ничего не запускается само.
-- Чтение одного дня I_CHECK — ~3 с (замер 19.10.2026); полное время дня с ключами — по первому прогону (журнал CVM_CLIENT_ITEM_LOG).
-- Диапазоны подряд, повтор диапазона безопасен. Рекомендуемый порядок: сначала последние недели до вчера (после них можно добавить
-- шаг задания), затем назад до @history_from — без дыр между диапазонами:
--   EXEC dbo.client_item_load @mode = 'init', @date_from = '2026-08-01', @date_to = '2026-10-18';
--   EXEC dbo.client_item_load @mode = 'init', @date_from = '2026-05-01', @date_to = '2026-07-31';   -- и так далее назад
-- Диапазон не длиннее 4000 дней; сколько дней за вечер — по времени первого прогона.
--
-- После ошибки 50007 (ночь не берёт перезаливку шире 4000 дней): метку двигает только успешный ночной прогон, поэтому после
-- загрузки дней той перезаливки через init метку сдвинуть вручную, иначе ночь будет падать с 50007 каждый раз. Все загрузки
-- с dt_load до новой метки должны быть покрыты init (их дни min_date … max_date):
--   INSERT dbo.CVM_CLIENT_ITEM_LOG (STARTED, FINISHED, MODE, STATUS, LOADING_DT)
--   SELECT SYSDATETIME(), SYSDATETIME(), 'mark', 'OK', MAX(l.dt_load)
--   FROM service.loading_log AS l WITH (NOLOCK)
--   WHERE l.table_name = N'I_CHECK_DELTA' AND l.dt_load <= '<dt_load широкой перезаливки>';

-- ---------- шаг в «MCI Regular update» сразу после CVM_CONTACT; при успехе и при ошибке — к следующему шагу ----------
-- Порядок задания с 16.10.2026: каждый шаг при успехе — следующий, без переходов «на шаг N»; новый шаг вставляется на своё
-- место. sp_add_jobstep с @step_id = (CVM_CONTACT + 1) сдвигает следующие шаги на 1 и сам пересчитывает их переходы
-- (в том числе «при ошибке — send error mail»). Повторный запуск шаг не задвоит. Запускать, когда задание не идёт.
USE msdb
GO
SET NOCOUNT ON;
DECLARE @job sysname = N'MCI Regular update';
DECLARE @cc int, @cc_ok int, @rc int = 0;

SELECT @cc = s.step_id, @cc_ok = s.on_success_action
FROM msdb.dbo.sysjobsteps AS s WITH (NOLOCK)
JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = s.job_id
WHERE j.name = @job AND s.step_name = N'CVM_CONTACT';

IF OBJECT_ID(N'mci_model.dbo.client_item_night', N'P') IS NULL OR OBJECT_ID(N'mci_model.dbo.I_CLIENT_ITEM_KEY_DAY', N'U') IS NULL
   OR OBJECT_ID(N'mci_model.dbo.CVM_CLIENT_ITEM_LOG', N'U') IS NULL
BEGIN
    RAISERROR(N'СТОП: процедуры dbo.client_item_night или таблиц I_CLIENT_ITEM_KEY_DAY / CVM_CLIENT_ITEM_LOG в mci_model нет (верх скрипта не выполнился) — задание не изменено.', 16, 1);
END
-- ключи есть И в журнале есть метка прогресса (успешный init): без метки ночной прогон каждый раз падал бы с 50002,
-- а при ошибке шаг молча уходит дальше (init, оборвавшийся на середине, оставляет ключи, но не метку)
ELSE IF NOT EXISTS (SELECT 1 FROM mci_model.dbo.I_CLIENT_ITEM_KEY_DAY WITH (NOLOCK))
     OR NOT EXISTS (SELECT 1 FROM mci_model.dbo.CVM_CLIENT_ITEM_LOG WITH (NOLOCK) WHERE STATUS = 'OK' AND LOADING_DT IS NOT NULL)
BEGIN
    RAISERROR(N'Шаг пока не добавлен: I_CLIENT_ITEM_KEY_DAY пустая или нет успешной первичной загрузки с меткой. После первой успешной вечерней загрузки (client_item_load init) запусти этот блок ещё раз.', 10, 1);
END
ELSE IF @cc IS NULL OR @cc_ok <> 3
   OR EXISTS (SELECT 1 FROM msdb.dbo.sysjobsteps AS s WITH (NOLOCK) JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = s.job_id
              WHERE j.name = @job AND s.step_name = N'CLIENT_ITEM')
BEGIN
    RAISERROR(N'СТОП: шаг CVM_CONTACT не найден, при успехе идёт не на следующий шаг или шаг CLIENT_ITEM уже есть — задание не изменено.', 16, 1);
END
ELSE IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobactivity AS a WITH (NOLOCK)
                JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = a.job_id
                WHERE j.name = @job
                  AND a.session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions WITH (NOLOCK))
                  AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL)
BEGIN
    RAISERROR(N'СТОП: задание «MCI Regular update» сейчас идёт — номера шагов не сдвигаю, задание не изменено. Запусти блок после окончания.', 16, 1);
END
ELSE
BEGIN
    SET @cc += 1;
    EXEC @rc = msdb.dbo.sp_add_jobstep @job_name = @job, @step_id = @cc, @step_name = N'CLIENT_ITEM',
         @subsystem = N'TSQL', @database_name = N'mci_model', @command = N'exec dbo.client_item_night;',
         @on_success_action = 3, @on_fail_action = 3;
    IF (@@ERROR <> 0 OR @rc <> 0)
        RAISERROR(N'СТОП: шаг не добавлен (ошибка sp_add_jobstep) — задание не изменено.', 16, 1);
END
GO

SELECT s.step_id, s.step_name, s.command, s.on_success_action, s.on_success_step_id, s.on_fail_action, s.on_fail_step_id
FROM msdb.dbo.sysjobsteps AS s WITH (NOLOCK)
JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = s.job_id
WHERE j.name = N'MCI Regular update'
ORDER BY s.step_id;
GO

-- ============================== 7. ЗАПОЛНЕНИЕ НАШЕЙ КАТЕГОРИИ (ПОСЛЕ УТВЕРЖДЕНИЯ СПРАВОЧНИКА) ==============================
-- Таблица справочника — после утверждения (ID сейчас временные, в базе справочника нет). Шаблон разового UPDATE по товару,
-- по одной неделе (секции отсекаются по DATA); после каждого диапазона недель — пересборка ключей этих дней:
--
-- USE mci_model
-- UPDATE d SET d.ID_CATEGORY_CVM = r.ID_CATEGORY_CVM
-- FROM dbo.I_CLIENT_ITEM_DAY AS d
-- JOIN dbo.<таблица_справочника_товар_категория> AS r WITH (NOLOCK) ON r.ID_PRODUCT = d.ID_PRODUCT
-- WHERE d.DATA >= '2026-08-31' AND d.DATA < '2026-09-07';
-- EXEC dbo.client_item_rebuild_keys @date_from = '2026-08-31', @date_to = '2026-09-06';
--
-- Ночная вставка (client_item_load) должна тогда подставлять категорию сама: LEFT JOIN к тому же справочнику
-- в INSERT вместо NULL (место отмечено комментарием в процедуре).

-- ============================== 8. ШАБЛОНЫ ЗАПРОСОВ (комментарием, не объекты) ==============================
-- 8.1 Подсчёт списка: клиенты, у которых по КАЖДОМУ выбранному ключу за [@d1, @d2] SUM(CHECKS) >= @n.
--     «За всё время» — @d1 = первая дата истории, @d2 = вчера. Отсечение секций — по DATA (без функций над колонкой).
--     Для одного ключа достаточно EXISTS (SELECT 1 FROM … WHERE … GROUP BY … HAVING SUM(CHECKS) >= @n) или того же запроса без @keys.
--     Только офлайн — добавить AND k.ID_ORGANIZATION = 1 (по умолчанию оба канала, суммируются по клиенту).
--
-- DECLARE @d1 date = '2026-09-01', @d2 date = '2026-09-01', @n int = 1;
-- DECLARE @keys TABLE (KEY_TYPE tinyint NOT NULL, ID_KEY bigint NOT NULL, PRIMARY KEY (KEY_TYPE, ID_KEY));   -- единицы строк
-- INSERT @keys VALUES (3, 5);                                    -- например, миссия 5; (1, <товар>), (2, <категория CVM>)
-- DECLARE @nk int = (SELECT COUNT(*) FROM @keys);
-- SELECT t.ID_CONTACT
-- FROM (SELECT k.ID_CONTACT, k.KEY_TYPE, k.ID_KEY
--       FROM dbo.I_CLIENT_ITEM_KEY_DAY AS k WITH (NOLOCK)
--       JOIN @keys AS s ON s.KEY_TYPE = k.KEY_TYPE AND s.ID_KEY = k.ID_KEY
--       WHERE k.DATA >= @d1 AND k.DATA <= @d2 AND k.ID_COMPANY = 1
--       GROUP BY k.ID_CONTACT, k.KEY_TYPE, k.ID_KEY
--       HAVING SUM(k.CHECKS) >= @n) AS t
-- GROUP BY t.ID_CONTACT
-- HAVING COUNT(*) = @nk;
--
-- 8.2 Число чеков с ключом за диапазон (основа календаря) — точное, чек в одном дне:
--
-- SELECT SUM(k.CHECKS) FROM dbo.I_CLIENT_ITEM_KEY_DAY AS k WITH (NOLOCK)
-- WHERE k.DATA >= @d1 AND k.DATA <= @d2 AND k.KEY_TYPE = 3 AND k.ID_KEY = 5;
--
-- 8.3 Проверка дыр в загруженной истории (дни без строк между первой и последней загруженной датой):
--
-- DECLARE @a date, @b date;
-- SELECT @a = MIN(DATA), @b = MAX(DATA) FROM dbo.I_CLIENT_ITEM_DAY WITH (NOLOCK);
-- ;WITH nums AS (SELECT TOP (4000) k = ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 FROM sys.all_columns AS a CROSS JOIN sys.all_columns AS b)
-- SELECT DATEADD(day, nums.k, @a) AS DAY_MISSING
-- FROM nums
-- WHERE nums.k <= DATEDIFF(day, @a, @b)
--   AND NOT EXISTS (SELECT 1 FROM dbo.I_CLIENT_ITEM_DAY AS d WITH (NOLOCK) WHERE d.DATA = DATEADD(day, nums.k, @a));
