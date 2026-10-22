-- ============================================================================================
-- CVM_CLIENT_ITEM_CATEGORIES — категории CVM в ключах витрины покупок клиента (I_CLIENT_ITEM_KEY_DAY, KEY_TYPE = 2).
-- Решение Елены 20.10.2026: категория берётся из товарной витрины соединением товара со справочником
-- dbo.I_CVM_PRODUCT_CATEGORY в момент сборки ключей дня (как миссии через ext-категорию). Колонка I_CLIENT_ITEM_DAY.ID_CATEGORY_CVM
-- НЕ заполняется (UPDATE по миллиардам строк колоночной таблицы не делаем): остаётся NULL и нигде не используется;
-- шаблон UPDATE в секции 7 CVM_CLIENT_ITEM.sql больше не нужен.
--
-- ПЕРЕЧЕНЬ ДЕЙСТВИЙ ПО ОБЪЕКТАМ
--   dbo.I_CVM_PRODUCT_CATEGORY   только чтение (NOLOCK): товар → ID_CVM_CATEGORY, HOW (1 код целиком, 2 правило слов, 3 поштучно,
--                                0 несопоставлен). В ключи идут товары с HOW > 0. ID_CVM_GROUP не используется (ключ — категория).
--   dbo.I_CVM_CATEGORY           не читается и не меняется (названия и иерархия нужны экрану, не сборке ключей).
--   dbo.client_item_keys_day     CREATE OR ALTER: меняется только блок «2. категории CVM» — вместо ID_CATEGORY_CVM из товарной
--                                таблицы соединение с рабочей копией справочника (#kd_cat, один раз на вызов), ID_KEY = ID_CVM_CATEGORY,
--                                чек считается один раз на категорию в день, сумма — по строкам чека в категории.
--                                Товары, миссии, проверка своей секции, TRUNCATE секции дня, ORDER BY, OPTION — без изменений.
--                                Ночной и вечерний load/init подхватывают категории сами (процедуру вызывают внутри).
--   Рабочая копия справочника #kd_cat  временная, на вызов: ~312 тыс. строк (товар → категория; ID_PRODUCT — ключ справочника,
--                                у товара одна категория), кластерный колоночный
--                                индекс по правилу проекта для #temp; хватает одной группы строк, join с дневной таблицей — по хешу.
--   Цикл догрузки (секция 2)     для дней, уже загруженных до этого скрипта (ключей-категорий в них нет): вызывает
--                                client_item_rebuild_keys порциями по 7 дней от последних дней назад; журнал пишет сама процедура
--                                (CVM_CLIENT_ITEM_LOG, MODE = 'keys'), замок applock CVM_CLIENT_ITEM берёт она же.
--   НЕ ТРОГАЕТСЯ И НЕ УДАЛЯЕТСЯ: I_CLIENT_ITEM_DAY (ни строки, ни ID_CATEGORY_CVM), структура I_CLIENT_ITEM_KEY_DAY,
--         client_item_load / client_item_rebuild_keys / client_item_night / client_item_partitions, шаг CLIENT_ITEM задания,
--         I_CHECK, I_CHECKHEADER, I_PRODUCT, I_MISSION, I_CVM_CATEGORY. Ничего не удаляется.
--
-- ПОРЯДОК ЗАПУСКА
--   1. Справочник должен появиться (20.10.2026 после 06:00). Секция 0 это проверяет сама: нет таблицы/колонок или нет строк
--      с HOW > 0 — сообщение, остальной файл пропускается.
--   2. Секция 1 (ALTER процедуры) — когда ночное задание «MCI Regular update» не идёт. После неё новые и перезалитые дни
--      получают категории автоматически.
--   3. Секция 2 (цикл догрузки) — вечером, после окончания задания и вечерней загрузки истории (CVM_CLIENT_ITEM_HISTORY.sql):
--      они не должны идти одновременно, замок CVM_CLIENT_ITEM второму не даст (процедура вернёт 50004 — цикл остановится
--      с сообщением, повторный запуск продолжит). По умолчанию цикл выключен (@run = 0): в секции 2 поставить @run = 1.
--      Запускать много вечеров подряд одним и тем же текстом: сам находит дни без категорий и останавливается до 04:00.
--      Дни, которые вечерняя история ещё не загрузила, цикл не трогает — их загрузит история, уже с категориями.
--   ВАЖНО: если после этого скрипта заново выполнить CVM_CLIENT_ITEM.sql целиком, он вернёт старую client_item_keys_day
--      (без категорий). Секция 2 это заметит (в определении процедуры нет справочника) и остановится; повторить секцию 1.
--
-- ЧТО СЧИТАЕТСЯ
--   KEY_TYPE = 2, ID_KEY = ID_CVM_CATEGORY, CHECKS = число разных чеков за день с товаром категории, SUMMA = сумма строк
--   этих товаров. У товара в справочнике одна категория (ID_PRODUCT — первичный ключ), чек — по разу на категорию.
-- ============================================================================================

USE mci_model
GO

-- ============================== 0. ПРОВЕРКИ ==============================
SET NOCOUNT ON;

IF    COL_LENGTH('dbo.I_CVM_PRODUCT_CATEGORY', 'ID_PRODUCT') IS NULL
   OR COL_LENGTH('dbo.I_CVM_PRODUCT_CATEGORY', 'ID_CVM_CATEGORY') IS NULL
   OR COL_LENGTH('dbo.I_CVM_PRODUCT_CATEGORY', 'HOW') IS NULL
   OR COL_LENGTH('dbo.I_CLIENT_ITEM_DAY', 'ID_PRODUCT') IS NULL
   OR COL_LENGTH('dbo.I_CLIENT_ITEM_DAY', 'ID_CHECK') IS NULL
   OR COL_LENGTH('dbo.I_CLIENT_ITEM_KEY_DAY', 'KEY_TYPE') IS NULL
   OR COL_LENGTH('dbo.I_MISSION', 'ID_MISSION') IS NULL OR COL_LENGTH('dbo.I_MISSION', 'ID_CATEGORY_5_ext') IS NULL
   OR OBJECT_ID(N'dbo.client_item_rebuild_keys', N'P') IS NULL OR OBJECT_ID(N'dbo.CVM_CLIENT_ITEM_LOG', N'U') IS NULL
BEGIN
    RAISERROR(N'СТОП: справочника I_CVM_PRODUCT_CATEGORY (ID_PRODUCT, ID_CVM_CATEGORY, HOW) или объектов витрины покупок (CVM_CLIENT_ITEM.sql) ещё нет — ничего не меняю.', 16, 1);
    SET NOEXEC ON;
END
GO

-- справочник не пустой (в динамическом SQL: при остановке выше пакет не выполняется)
DECLARE @cnt int = 0;
EXEC sys.sp_executesql
     N'SELECT @n = COUNT(*) FROM (SELECT TOP (1) 1 AS x FROM dbo.I_CVM_PRODUCT_CATEGORY AS pc WITH (NOLOCK) WHERE pc.[HOW] > 0 AND pc.ID_CVM_CATEGORY IS NOT NULL) AS t;'
   , N'@n int OUTPUT', @n = @cnt OUTPUT;
IF @cnt = 0
BEGIN
    RAISERROR(N'СТОП: в I_CVM_PRODUCT_CATEGORY нет ни одного товара с HOW > 0 (справочник ещё не заполнен) — ничего не меняю.', 16, 1);
    SET NOEXEC ON;
END
GO

-- ============================== 1. КЛЮЧИ ОДНОГО ДНЯ (внутренняя) ==============================
-- Текст — из CVM_CLIENT_ITEM.sql секция 3; изменён только блок «2. категории CVM» и рабочая таблица #kd_cat.
-- Имя #kd_cat, не #cat: DROP TABLE IF EXISTS во вложенной процедуре удалил бы #cat окна SSMS (так названа таблица в скриптах выборок).
-- Вызывается внутри транзакции дня (load) или rebuild_keys. Замок и транзакцию не берёт.
CREATE OR ALTER PROCEDURE dbo.client_item_keys_day
      @d    date
    , @rows bigint OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @idc int = 1, @pn int, @sql nvarchar(max);

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
    INTO #kd_cat
    FROM dbo.I_CVM_PRODUCT_CATEGORY AS pc WITH (NOLOCK)
    WHERE pc.[HOW] > 0;
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_kd_cat ON #kd_cat;

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
END
GO

-- ============================== 2. ДОГРУЗКА КАТЕГОРИЙ В УЖЕ ЗАГРУЖЕННЫЕ ДНИ ==============================
-- Запускать вечером, когда «MCI Regular update» не идёт; можно много вечеров подряд одним и тем же текстом.
-- Цикл выключен по умолчанию: чтобы F5 по всему файлу не начал пересборку днём, поставить @run = 1.
DECLARE @run bit = 0;                                            -- 1 = запустить цикл

DECLARE @chunk_days int = 7;                                     -- порция, дней
DECLARE @stop_at datetime = DATEADD(hour, 4, CAST(CAST(GETDATE() + CASE WHEN DATEPART(hour, GETDATE()) >= 4 THEN 1 ELSE 0 END AS date) AS datetime));
DECLARE @yesterday date = DATEADD(day, -1, CAST(GETDATE() AS date));
DECLARE @pf int = (SELECT function_id FROM sys.partition_functions WHERE name = N'pf_CLIENT_ITEM_DAY');
DECLARE @first date = (SELECT MIN(CAST(v.value AS date)) FROM sys.partition_range_values AS v WHERE v.function_id = @pf);
DECLARE @alter_dt datetime = (SELECT modify_date FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.client_item_keys_day'));
DECLARE @def nvarchar(max) = OBJECT_DEFINITION(OBJECT_ID(N'dbo.client_item_keys_day'));
DECLARE @from date, @to date, @prev date, @t0 datetime, @max_sec int = 0, @sec int, @err bit = 0, @left int, @msg nvarchar(2000);

IF @run = 0
BEGIN
    RAISERROR(N'Цикл догрузки категорий выключен: чтобы запустить вечером, поставить @run = 1.', 0, 1) WITH NOWAIT;
    RETURN;
END;

IF @pf IS NULL OR @first IS NULL OR @first > @yesterday OR @alter_dt IS NULL
   OR OBJECT_ID(N'dbo.client_item_rebuild_keys', N'P') IS NULL OR OBJECT_ID(N'dbo.I_CLIENT_ITEM_KEY_DAY', N'U') IS NULL
   OR OBJECT_ID(N'dbo.CVM_CLIENT_ITEM_LOG', N'U') IS NULL
BEGIN
    RAISERROR(N'СТОП: нет функции секций, таблиц витрины или процедур client_item_* — сначала CVM_CLIENT_ITEM.sql.', 16, 1);
    RETURN;
END;

IF @def IS NULL OR CHARINDEX(N'I_CVM_PRODUCT_CATEGORY', @def) = 0
BEGIN
    RAISERROR(N'СТОП: client_item_keys_day без категорий (нет соединения со справочником) — сначала выполнить секцию 1 этого файла.', 16, 1);
    RETURN;
END;

-- замок, оставшийся у этого окна после «Стоп» посреди порции (владелец — сессия, снимается только ею)
WHILE APPLOCK_MODE(N'public', N'CVM_CLIENT_ITEM', N'Session') <> N'NoLock'
    EXEC sys.sp_releaseapplock @Resource = N'CVM_CLIENT_ITEM', @LockOwner = N'Session';

-- календарь дней (~2 200 строк, маленькая служебная): ST = 0 — день загружен и ещё не проверен, 1 — не требуется
DROP TABLE IF EXISTS #cal;
CREATE TABLE #cal (DATA date NOT NULL PRIMARY KEY, ST tinyint NOT NULL DEFAULT 1);
INSERT #cal (DATA)
SELECT DATEADD(day, t.k, @first)
FROM (SELECT TOP (DATEDIFF(day, @first, @yesterday) + 1) k = ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1
      FROM sys.all_columns AS a CROSS JOIN sys.all_columns AS b) AS t;

-- день загружен, если в секции дня в KEY_DAY есть строки (метаданные, без чтения таблицы; RANGE RIGHT: секция n = день границы n-1)
UPDATE c SET ST = 0
FROM #cal AS c
WHERE EXISTS (SELECT 1 FROM sys.partitions AS p
              JOIN sys.partition_range_values AS v ON v.function_id = @pf AND v.boundary_id = p.partition_number - 1
              WHERE p.object_id = OBJECT_ID(N'dbo.I_CLIENT_ITEM_KEY_DAY') AND p.index_id IN (0, 1) AND p.rows > 0
                AND CAST(v.value AS date) = c.DATA);

-- дни, уже собранные ПОСЛЕ замены процедуры (успешные keys и init — их диапазоны сплошные): категории в них учтены,
-- даже если категорий в самом дне нет вовсе (иначе такой день пересобирался бы каждый вечер)
UPDATE c SET ST = 1
FROM #cal AS c
WHERE c.ST = 0
  AND EXISTS (SELECT 1 FROM dbo.CVM_CLIENT_ITEM_LOG AS l WITH (NOLOCK)
              WHERE l.MODE IN ('keys', 'init') AND l.STATUS = 'OK' AND l.STARTED >= @alter_dt
                AND c.DATA BETWEEN l.DATE_FROM AND l.DATE_TO);

-- порции: самый поздний загруженный день без ключей-категорий и назад до 7 дней. Проверка дня — один EXISTS по секции дня
-- и типу ключа (отсечение секции и групп строк), без прохода по таблице.
WHILE 1 = 1
BEGIN
    IF DATEADD(second, @max_sec, GETDATE()) >= @stop_at BREAK;       -- следующая порция не успеет до 04:00

    SET @to = (SELECT MAX(DATA) FROM #cal WHERE ST = 0);
    IF @to IS NULL BREAK;

    IF EXISTS (SELECT 1 FROM dbo.I_CLIENT_ITEM_KEY_DAY AS k WITH (NOLOCK) WHERE k.DATA = @to AND k.KEY_TYPE = 2)
    BEGIN
        UPDATE #cal SET ST = 1 WHERE DATA = @to;                     -- категории уже есть
        CONTINUE;
    END;

    -- порция: подряд идущие дни без категорий окна [@to − 6, @to], не раньше первой секции и не заходя за день с ST = 1
    -- (незагруженный день или уже с категориями — его не пересобираем, как в CVM_CLIENT_ITEM_HISTORY.sql)
    SET @prev = (SELECT MAX(DATA) FROM #cal WHERE DATA < @to AND ST = 1);
    SET @from = DATEADD(day, 1 - @chunk_days, @to);
    IF @from < @first SET @from = @first;
    IF @from <= @prev SET @from = DATEADD(day, 1, @prev);

    SET @msg = CONCAT(CONVERT(char(19), GETDATE(), 120), N'  категории: ', CONVERT(char(10), @from, 104), N' — ', CONVERT(char(10), @to, 104), N' …');
    RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;
    SET @t0 = GETDATE();
    BEGIN TRY
        EXEC dbo.client_item_rebuild_keys @date_from = @from, @date_to = @to;     -- журнал (MODE = 'keys') и замок — внутри
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        SET @msg = CONCAT(N'СТОП: ошибка на порции ', CONVERT(char(10), @from, 104), N' — ', CONVERT(char(10), @to, 104), N': ',
                          ERROR_MESSAGE(), N' Повторный запуск продолжит с дней без категорий.');
        RAISERROR(N'%s', 16, 1, @msg);                               -- через %s: в тексте ошибки может быть «%»
        SET @err = 1;
        BREAK;
    END CATCH;
    SET @sec = DATEDIFF(second, @t0, GETDATE());
    IF @sec > @max_sec SET @max_sec = @sec;                          -- запас по самой долгой порции

    UPDATE #cal SET ST = 1 WHERE DATA BETWEEN @from AND @to;
END;

IF @err = 0
BEGIN
    SET @left = (SELECT COUNT(*) FROM #cal WHERE ST = 0);
    IF @left = 0
        RAISERROR(N'Категории есть во всех загруженных днях.', 0, 1) WITH NOWAIT;
    ELSE
    BEGIN
        SET @msg = CONCAT(N'Остановлено до 04:00. Дней ещё не проверено или без категорий: до ', @left,
                          N', самый поздний — ', CONVERT(char(10), (SELECT MAX(DATA) FROM #cal WHERE ST = 0), 104),
                          N'. Следующим вечером запустить этот же скрипт (@run = 1) — продолжит отсюда.');
        RAISERROR(N'%s', 0, 1, @msg) WITH NOWAIT;
    END;
END;

-- ход
SELECT TOP (30) ID, STARTED, FINISHED, MODE, DATE_FROM, DATE_TO, DAYS_DONE, KEY_ROWS, STATUS, ERROR_MSG
FROM dbo.CVM_CLIENT_ITEM_LOG WITH (NOLOCK)
ORDER BY ID DESC;
GO

SET NOEXEC OFF;
GO

-- Проверка после секции 1 и первых порций (комментарием, не объект): категории по дню и сверка с товарами —
-- число чеков категории за день не больше числа чеков её товаров.
-- SELECT TOP (20) k.ID_KEY, k.CHECKS, k.SUMMA FROM dbo.I_CLIENT_ITEM_KEY_DAY AS k WITH (NOLOCK)
-- WHERE k.DATA = '2026-10-18' AND k.KEY_TYPE = 2 AND k.ID_CONTACT = <клиент> ORDER BY k.ID_KEY;
