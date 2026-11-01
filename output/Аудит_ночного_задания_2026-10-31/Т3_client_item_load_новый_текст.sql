
-- ============================== 4. ЗАГРУЗКА ДНЕЙ ==============================
-- @mode = 'night' — дни из service.loading_log с dt_load позже обработанной (требуется предыдущая первичная загрузка);
-- @mode = 'init'  — дни @date_from … @date_to, ОБЯЗАТЕЛЬНЫЕ (история грузится диапазонами за несколько вечеров; повтор диапазона
--                   безопасен — день перезаливается целиком). Если @date_from раньше первой секции — секции добавляются назад.
-- Метка прогресса (LOADING_DT) при 'init' ставится один раз — первым прогоном, пока в журнале её нет.
CREATE   PROCEDURE dbo.client_item_load
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
    DECLARE @n_new bigint, @n_old bigint, @n_diff bigint, @same bit, @kept bit, @skip_items int = 0, @skip_keys int = 0;   -- Т3

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
        -- Т3: секция дня для сравнения с порцией (ночью, только у дня с равным числом строк; ~6–7 млн строк)
        DROP TABLE IF EXISTS #day_old;
        CREATE TABLE #day_old (ID_ORGANIZATION int NOT NULL, ID_CONTACT int NOT NULL, ID_CHECK bigint NOT NULL, ID_PRODUCT bigint NOT NULL,
                               SUMMA decimal(18,2) NULL, QTY decimal(18,3) NULL, EXT5 bigint NULL, INDEX CCI_day_old CLUSTERED COLUMNSTORE);

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
                -- Т3 (01.11.2026, бэклог ночного задания): ночью перезаливка MCI отдаёт заново 10 последних дней, часть из них не
                -- меняется. День, чьи товарные строки уже такие, какие дала бы перезапись, не переписывается: то же число строк в
                -- секции дня и ни одной строки порции, которой нет в секции (строки порции уникальны по чеку и товару — GROUP BY выше;
                -- строки других компаний или с заполненной ID_CATEGORY_CVM в секции дают неравное число — день переписывается).
                -- Ключи такого дня сравнивает client_item_keys_day (@check_same = 1). Загрузка истории (init) — как прежде.
                SET @same = 0;
                SET @kept = 0;
                IF @mode = 'night'
                BEGIN
                    SELECT @n_new = COUNT_BIG(*) FROM #chunk WHERE DATA = @d;
                    SELECT @n_old = COUNT_BIG(*) FROM dbo.I_CLIENT_ITEM_DAY WITH (NOLOCK) WHERE DATA = @d;
                    IF @n_new > 0 AND @n_new = @n_old
                    BEGIN
                        -- секция дня — сначала во временную: EXCEPT прямо по витрине заставил бы сервер создать статистики по
                        -- SUMMA и QTY на всю таблицу (их нет); полный подсчёт разницы, не NOT EXISTS — цель «первая строка»
                        -- толкает план к вложенным циклам
                        TRUNCATE TABLE #day_old;
                        INSERT INTO #day_old WITH (TABLOCK) (ID_ORGANIZATION, ID_CONTACT, ID_CHECK, ID_PRODUCT, SUMMA, QTY, EXT5)
                        SELECT ID_ORGANIZATION, ID_CONTACT, ID_CHECK, ID_PRODUCT, SUMMA, QTY, ID_CATEGORY_5_EXT
                        FROM dbo.I_CLIENT_ITEM_DAY WITH (NOLOCK)
                        WHERE DATA = @d AND ID_COMPANY = @idc AND ID_CATEGORY_CVM IS NULL;
                        SELECT @n_diff = COUNT_BIG(*)
                        FROM (SELECT ID_ORGANIZATION, ID_CONTACT, ID_CHECK, ID_PRODUCT, SUMMA, QTY, EXT5
                              FROM #chunk WHERE DATA = @d
                              EXCEPT
                              SELECT ID_ORGANIZATION, ID_CONTACT, ID_CHECK, ID_PRODUCT, SUMMA, QTY, EXT5
                              FROM #day_old) AS x
                        OPTION (RECOMPILE);
                        IF @n_diff = 0
                            SET @same = 1;
                    END
                END
                BEGIN TRANSACTION;
                IF @same = 0
                BEGIN   -- Т3
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
                END     -- Т3
                ELSE
                BEGIN   -- Т3: товарные строки дня не меняются
                    SET @n = @n_new;
                    SET @skip_items += 1;
                END
                -- ключи дня сразу после товарной строки дня (TRUNCATE секции дня в KEY_DAY + вставка внутри)
                EXEC dbo.client_item_keys_day @d = @d, @rows = @kn OUTPUT, @check_same = @same, @kept = @kept OUTPUT;   -- Т3: + @check_same, @kept
                COMMIT TRANSACTION;
                SET @skip_keys += @kept;                                                                                 -- Т3
                SET @rows += @n;
                SET @krows += @kn;
                SET @rn += 1;
            END
            SET @ch += 1;
        END

        UPDATE dbo.CVM_CLIENT_ITEM_LOG
        SET FINISHED = SYSDATETIME(), DATE_FROM = @date_from, DATE_TO = @date_to, DAYS_DONE = @ndays, ROWS_ = @rows,
            KEY_ROWS = @krows, LOADING_DT = @marker, STATUS = 'OK'
            -- Т3: ночью — сколько дней не переписано (журнал читают портал и история по STATUS/DATE_TO/LOADING_DT, это поле — нет)
          , ERROR_MSG = CASE WHEN @mode = 'night' AND @ndays > 0
                             THEN CONCAT(N'Т3: дней без перезаписи — товарные строки ', @skip_items, N' из ', @ndays,
                                         N', ключи ', @skip_keys, N' из ', @ndays) END
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
