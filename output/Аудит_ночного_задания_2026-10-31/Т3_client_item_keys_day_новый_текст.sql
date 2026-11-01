CREATE   PROCEDURE dbo.client_item_keys_day
      @d    date
    , @rows bigint OUTPUT
    , @check_same bit = 0           -- Т3 (01.11.2026): 1 — товарные строки дня не менялись, ключи сначала сравнить с секцией (ниже)
    , @kept bit = 0 OUTPUT          -- Т3: 1 — секция ключей уже совпадала и не переписывалась
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @idc int = 1, @pn int, @sql nvarchar(max), @ver int;
    DECLARE @k_new bigint, @k_old bigint, @k_diff bigint;                       -- Т3
    SET @kept = 0;

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

    -- Т3 (01.11.2026, бэклог ночного задания): ночной шаг передаёт @check_same = 1 у дня, чьи товарные строки совпали с секцией
    -- (client_item_load). Справочники категорий и миссий меняются и при тех же чеках, поэтому ключи такого дня собираются во
    -- временную тем же запросом, что в ветке ниже (два запроса держать одинаковыми), и сравниваются с секцией ключей дня: то же
    -- число строк и ни одной строки временной, которой нет в секции (строки уникальны по клиенту, типу и ключу) — секция уже
    -- такая, какой её сделала бы перезапись, и не трогается; иначе — TRUNCATE секции и вставка из временной в том же порядке.
    IF @check_same = 1
    BEGIN
        DROP TABLE IF EXISTS #kd_new;
        CREATE TABLE #kd_new (ID_ORGANIZATION int NOT NULL, ID_CONTACT int NOT NULL, KEY_TYPE tinyint NOT NULL, ID_KEY bigint NOT NULL,
                              CHECKS int NOT NULL, SUMMA decimal(18, 2) NOT NULL, INDEX CCI_kd_new CLUSTERED COLUMNSTORE);
        INSERT INTO #kd_new WITH (TABLOCK) (ID_ORGANIZATION, ID_CONTACT, KEY_TYPE, ID_KEY, CHECKS, SUMMA)
        SELECT k.ID_ORGANIZATION, k.ID_CONTACT, k.KEY_TYPE, k.ID_KEY, k.CHECKS, k.SUMMA
        FROM (
            SELECT d.ID_ORGANIZATION, d.ID_CONTACT, KEY_TYPE = CAST(1 AS tinyint)
                 , ID_KEY = CAST(d.ID_PRODUCT AS bigint), CHECKS = COUNT(*), SUMMA = ISNULL(SUM(d.SUMMA), 0)
            FROM dbo.I_CLIENT_ITEM_DAY AS d WITH (NOLOCK)
            WHERE d.DATA = @d AND d.ID_COMPANY = @idc
            GROUP BY d.ID_ORGANIZATION, d.ID_CONTACT, d.ID_PRODUCT

            UNION ALL
            SELECT g.ID_ORGANIZATION, g.ID_CONTACT, CAST(2 AS tinyint), g.ID_CAT
                 , COUNT(*), ISNULL(SUM(g.SUMMA), 0)
            FROM (SELECT d.ID_ORGANIZATION, d.ID_CONTACT, d.ID_CHECK, c.ID_CAT, SUMMA = SUM(d.SUMMA)
                  FROM dbo.I_CLIENT_ITEM_DAY AS d WITH (NOLOCK)
                  JOIN #kd_cat AS c ON c.ID_PRODUCT = d.ID_PRODUCT
                  WHERE d.DATA = @d AND d.ID_COMPANY = @idc
                  GROUP BY d.ID_ORGANIZATION, d.ID_CONTACT, d.ID_CHECK, c.ID_CAT) AS g
            GROUP BY g.ID_ORGANIZATION, g.ID_CONTACT, g.ID_CAT

            UNION ALL
            SELECT g.ID_ORGANIZATION, g.ID_CONTACT, CAST(3 AS tinyint), CAST(g.ID_MISSION AS bigint)
                 , COUNT(*), ISNULL(SUM(g.SUMMA), 0)
            FROM (SELECT d.ID_ORGANIZATION, d.ID_CONTACT, d.ID_CHECK, m.ID_MISSION, SUMMA = SUM(d.SUMMA)
                  FROM dbo.I_CLIENT_ITEM_DAY AS d WITH (NOLOCK)
                  JOIN #miss AS m ON m.EXT5 = d.ID_CATEGORY_5_EXT
                  WHERE d.DATA = @d AND d.ID_COMPANY = @idc
                  GROUP BY d.ID_ORGANIZATION, d.ID_CONTACT, d.ID_CHECK, m.ID_MISSION) AS g
            GROUP BY g.ID_ORGANIZATION, g.ID_CONTACT, g.ID_MISSION
        ) AS k
        OPTION (RECOMPILE);
        SET @k_new = @@ROWCOUNT;
        SELECT @k_old = COUNT_BIG(*) FROM dbo.I_CLIENT_ITEM_KEY_DAY WITH (NOLOCK) WHERE DATA = @d;
        IF @k_new = @k_old
        BEGIN
            -- секция ключей — сначала во временную: EXCEPT прямо по витрине заставил бы сервер создать статистику по SUMMA
            -- на всю таблицу (её нет); полный подсчёт разницы, не NOT EXISTS — цель «первая строка» толкает план к вложенным циклам
            DROP TABLE IF EXISTS #kd_old;
            CREATE TABLE #kd_old (ID_ORGANIZATION int NOT NULL, ID_CONTACT int NOT NULL, KEY_TYPE tinyint NOT NULL, ID_KEY bigint NOT NULL,
                                  CHECKS int NOT NULL, SUMMA decimal(18, 2) NOT NULL, INDEX CCI_kd_old CLUSTERED COLUMNSTORE);
            INSERT INTO #kd_old WITH (TABLOCK) (ID_ORGANIZATION, ID_CONTACT, KEY_TYPE, ID_KEY, CHECKS, SUMMA)
            SELECT ID_ORGANIZATION, ID_CONTACT, KEY_TYPE, ID_KEY, CHECKS, SUMMA
            FROM dbo.I_CLIENT_ITEM_KEY_DAY WITH (NOLOCK)
            WHERE DATA = @d AND ID_COMPANY = @idc;
            SELECT @k_diff = COUNT_BIG(*)
            FROM (SELECT ID_ORGANIZATION, ID_CONTACT, KEY_TYPE, ID_KEY, CHECKS, SUMMA FROM #kd_new
                  EXCEPT
                  SELECT ID_ORGANIZATION, ID_CONTACT, KEY_TYPE, ID_KEY, CHECKS, SUMMA FROM #kd_old) AS x
            OPTION (RECOMPILE);
            DROP TABLE #kd_old;
            IF @k_diff = 0
                SET @kept = 1;
        END
        IF @kept = 1
            SET @rows = @k_new;
        ELSE
        BEGIN
            EXEC sys.sp_executesql @sql;
            INSERT INTO dbo.I_CLIENT_ITEM_KEY_DAY WITH (TABLOCK)
                   (DATA, ID_COMPANY, ID_ORGANIZATION, ID_CONTACT, KEY_TYPE, ID_KEY, CHECKS, SUMMA)
            SELECT @d, @idc, ID_ORGANIZATION, ID_CONTACT, KEY_TYPE, ID_KEY, CHECKS, SUMMA
            FROM #kd_new
            ORDER BY KEY_TYPE, ID_KEY
            OPTION (MAXDOP 1, RECOMPILE);
            SET @rows = @@ROWCOUNT;
        END
        DROP TABLE #kd_new;
    END
    ELSE
    BEGIN   -- Т3: прежний путь (ночью — день с изменившимися товарными строками; загрузка истории, пересборка ключей)
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
    END     -- Т3

    -- отметка версии справочника дня (транзакция дня — у вызывающей процедуры; один вызов на день под замком витрины)
    UPDATE dbo.CVM_CLIENT_ITEM_CAT_VER SET CAT_VER = @ver, BUILT_AT = SYSDATETIME() WHERE DATA = @d;
    IF @@ROWCOUNT = 0
        INSERT dbo.CVM_CLIENT_ITEM_CAT_VER (DATA, CAT_VER, BUILT_AT) VALUES (@d, @ver, SYSDATETIME());
END
