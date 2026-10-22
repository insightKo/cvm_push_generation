-- ============================================================================================
-- CVM_CLIENT_ITEM_CATVER_20261023 — запоминание версии справочника категорий CVM по дням витрины покупок клиента.
-- Для обменника (CVM_API_nightly). Идемпотентный: повтор безопасен.
-- ДЕЙСТВИЯ ПО ОБЪЕКТАМ
--   dbo.CVM_CLIENT_ITEM_CAT_VER   создаётся, если нет (день → версия справочника; маленькая служебная).
--   dbo.client_item_keys_day      заменяется (CREATE OR ALTER): та же сборка ключей дня (товар/категория CVM/миссия) + запись версии
--                                 справочника дня в CVM_CLIENT_ITEM_CAT_VER в той же транзакции дня. Текст — из CVM_CLIENT_ITEM.sql (b742270).
--   НЕ ТРОГАЕТСЯ: таблицы витрины и их данные, журнал, остальные процедуры, задания (работающее задание истории продолжает работу;
--                 дни, загруженные с этого момента, получают отметку версии).
-- ============================================================================================
USE mci_model;
GO
-- 1.5 Версия справочника категорий по дням (маленькая служебная, точечные UPDATE по дню — без колоночного индекса)
IF OBJECT_ID('dbo.CVM_CLIENT_ITEM_CAT_VER') IS NULL
    CREATE TABLE dbo.CVM_CLIENT_ITEM_CAT_VER (
          DATA      date         NOT NULL PRIMARY KEY
        , CAT_VER   int          NOT NULL                      -- версия справочника, с которой собраны ключи дня
        , BUILT_AT  datetime2(0) NULL                          -- когда собраны
    );
GO
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
