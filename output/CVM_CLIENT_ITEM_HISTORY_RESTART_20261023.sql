-- ============================================================================================
-- CVM_CLIENT_ITEM_HISTORY_RESTART_20261023 — перезапуск задания истории витрины покупок клиента с пересборкой категорий
-- по версии справочника (решение Елены 20.10.2026). Для обменника (CVM_API_nightly); повтор безопасен.
-- ДЕЙСТВИЯ ПО ОБЪЕКТАМ
--   Проверка: процедура ключей дня уже с версией справочника (скрипт CVM_CLIENT_ITEM_CATVER_20261023.sql) — иначе ошибка
--             (обменник повторит следующим проходом), ничего не трогается.
--   Задание SQL Agent CVM_CLIENT_ITEM_HISTORY — если идёт со СТАРЫМ текстом dbo.client_item_history (в тексте нет CAT_VER или
--             задание запущено раньше последней замены процедуры): sp_stop_job и ожидание остановки до 10 мин (незавершённый день
--             откатывается целиком — день атомарен; запись RUN в журнале новое задание закроет само). Если идёт уже с новым
--             текстом — не трогается. Проверка в контексте mci_model (OBJECT_DEFINITION работает только в текущей базе), msdb — трёхчастными именами.
--   Дальше — CVM_CLIENT_ITEM_HISTORY.sql без изменений (b742270): процедуры client_item_history / client_item_history_wait
--             заменяются (CREATE OR ALTER), старое задание удаляется, новое создаётся и запускается; оно продолжает догрузку
--             истории с того же места и потом пересобирает категории в днях без отметки версии или со старой версией.
--   НЕ ТРОГАЕТСЯ: данные витрины (кроме пересборки ключей дней внутри задания), процедуры загрузки, шаг CLIENT_ITEM ночного прохода.
-- ============================================================================================
USE mci_model;
GO
SET NOCOUNT ON;
IF OBJECT_ID(N'dbo.CVM_CLIENT_ITEM_CAT_VER', N'U') IS NULL
   OR ISNULL(OBJECT_DEFINITION(OBJECT_ID(N'dbo.client_item_keys_day')), N'') NOT LIKE N'%SHA2_256%'
    THROW 50101, N'CVM_CLIENT_ITEM_HISTORY_RESTART: нет версии справочника в процедуре ключей дня — сначала CVM_CLIENT_ITEM_CATVER_20261023.sql (повтор следующим проходом).', 1;
GO
-- контекст mci_model (не msdb): OBJECT_DEFINITION / sys.objects ниже смотрят текущую базу; msdb — трёхчастными именами
SET NOCOUNT ON;
DECLARE @job sysname = N'CVM_CLIENT_ITEM_HISTORY', @i int = 0, @running bit = 0, @started datetime;
SELECT @running = 1, @started = a.start_execution_date
FROM msdb.dbo.sysjobactivity AS a WITH (NOLOCK)
JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = a.job_id
WHERE j.name = @job
  AND a.session_id = (SELECT MAX(s.session_id) FROM msdb.dbo.syssessions AS s WITH (NOLOCK))
  AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL;

IF @running = 1
   AND (ISNULL(OBJECT_DEFINITION(OBJECT_ID(N'dbo.client_item_history')), N'') NOT LIKE N'%CVM_CLIENT_ITEM_CAT_VER%'
        OR @started < (SELECT o.modify_date FROM sys.objects AS o WHERE o.object_id = OBJECT_ID(N'dbo.client_item_history')))
BEGIN
    RAISERROR(N'CVM_CLIENT_ITEM_HISTORY_RESTART: останавливаю задание истории со старым текстом процедуры.', 10, 1) WITH NOWAIT;
    BEGIN TRY
        EXEC msdb.dbo.sp_stop_job @job_name = @job;     -- задание могло закончиться само между проверкой и стопом (Msg 22022)
    END TRY
    BEGIN CATCH
        DECLARE @em nvarchar(2048) = ERROR_MESSAGE();
        RAISERROR(N'CVM_CLIENT_ITEM_HISTORY_RESTART: sp_stop_job — %s (состояние решает проверка ниже).', 10, 1, @em) WITH NOWAIT;
    END CATCH
    WHILE @running = 1 AND @i < 60                      -- до 10 минут: откат незавершённого дня
    BEGIN
        WAITFOR DELAY '00:00:10';
        SET @i += 1;
        SET @running = 0;
        SELECT @running = 1
        FROM msdb.dbo.sysjobactivity AS a WITH (NOLOCK)
        JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = a.job_id
        WHERE j.name = @job
          AND a.session_id = (SELECT MAX(s.session_id) FROM msdb.dbo.syssessions AS s WITH (NOLOCK))
          AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL;
    END
    IF @running = 1
        THROW 50102, N'CVM_CLIENT_ITEM_HISTORY_RESTART: задание истории не остановилось за 10 минут — ничего не меняю (повтор следующим проходом).', 1;
END
GO
-- ============================== CVM_CLIENT_ITEM_HISTORY.sql (b742270) без изменений ==============================
-- ============================================================================================
-- CVM_CLIENT_ITEM_HISTORY — ВСЯ дальнейшая работа с витриной покупок клиента ОДНИМ ЗАПУСКОМ (19.10.2026):
-- категории в уже загруженных днях (в том числе последние 7 недель, если пересборку остановили) и догрузка истории.
-- Заменяет секцию 2 CVM_CLIENT_ITEM_CATEGORIES.sql и ручные client_item_rebuild_keys / client_item_load 'init'.
--
-- ПОСЛЕ ПРАВКИ СПРАВОЧНИКА КАТЕГОРИЙ (dbo.I_CVM_PRODUCT_CATEGORY) ЗАПУСТИТЬ ЭТОТ ЖЕ СКРИПТ ВЕЧЕРОМ — он пересоберёт категории по всей
-- истории витрины (решение Елены 20.10.2026), чеки не читаются.
--
-- ПЕРЕД ЗАПУСКОМ: остановить всё ручное по витрине и ЗАКРЫТЬ вкладки SSMS, где шли загрузка или пересборка (после «Стоп»
-- вкладка держит замок витрины, пока открыта). Потом F5 этого файла целиком. Дальше ничего ловить не нужно: скрипт создаёт
-- и запускает разовое задание SQL Agent «CVM_CLIENT_ITEM_HISTORY», оно работает само; после успешного окончания удаляет себя
-- (при ошибке остаётся — причина в журнале). Новые дни грузит ночной шаг CLIENT_ITEM в «MCI Regular update».
--
-- ЧТО ДЕЛАЕТ ЗАДАНИЕ (dbo.client_item_history)
--   1. Проверяет, что процедура ключей дня — с нашими категориями и отметкой версии справочника (иначе стоп с сообщением).
--   2. Ждёт: витрину никто не пишет (замок CVM_CLIENT_ITEM свободен) и 10 минут в журнале тишина; не идёт «MCI Regular update»;
--      с 00:20 до 01:30 куски не начинаются. «MCI Reload every day» не ждётся: он пишет только сырые chequeitems_retro/rules,
--      а витрина читает I_CHECK / I_CHECKHEADER, которые пишет «MCI Regular update» (19.10.2026 загрузка висит с 18.10 01:00 —
--      ожидание её остановило бы задание).
--      Проверка каждые 5 минут; ждать больше суток подряд — стоп с ошибкой (задание остаётся, причина — в истории задания).
--   3. Под замком витрины закрывает в журнале CVM_CLIENT_ITEM_LOG записи, оставшиеся в RUN от остановленных прогонов.
--   4. Сам смотрит по базе:
--      — история — от первой секции pf_CLIENT_ITEM_DAY (первый день чеков) до последнего дня успешной загрузки (init/night);
--      — день загружен — в секции дня I_CLIENT_ITEM_DAY есть строки; категории есть — в KEY_DAY есть KEY_TYPE = 2 за день;
--      — день, уже успешно обработанный после установки процедуры с категориями, второй раз не берётся (день законно без
--        категорий или без чеков не крутится при каждом запуске).
--   5. Сначала пересобирает ключи у загруженных дней без категорий (client_item_rebuild_keys, I_CHECK не читает), потом
--      догружает дни, которых нет (client_item_load 'init', категории ставятся сами). От новых дней к старым (сначала 52 недели —
--      ДЦО и конструктор), кусками по 7 дней. Кусок, не взявший замок, — повтор через 5 минут (до 12 раз); другая ошибка
--      куска пишется в журнал, задание идёт дальше и в конце завершается с ошибкой.
--   6. Потом, в том же запуске, если время позволяет: дни, загруженные в витрине, у которых нет строки в CVM_CLIENT_ITEM_CAT_VER или версия
--      справочника категорий в ней не равна текущей, — client_item_rebuild_keys порциями до 7 подряд идущих дней, от последних дней назад
--      (I_CHECK не читается). Версия = CHECKSUM_AGG от первых 4 байт SHA2_256(ID_PRODUCT, ID_CVM_CATEGORY, HOW) по строкам справочника
--      HOW > 0, как в client_item_keys_day. Порция не начинается (с полуночи до полудня), если уже 04:00 или позже либо до 04:00 остаётся
--      меньше, чем заняли бы её дни по скорости прошлой порции (для первой порции — только проверка 04:00): остаток доберёт следующий вечерний запуск этого же скрипта. Ошибка порции — стоп (причина в журнале);
--      при ошибке в п. 5 пересборка по версии справочника не начинается.
--
-- ПЕРЕЧЕНЬ ДЕЙСТВИЙ ПО ОБЪЕКТАМ
--   dbo.client_item_history_wait   создаётся/заменяется: ожидание (п. 2).
--   dbo.client_item_history        создаётся/заменяется: проверки, план кусков, выполнение (п. 1, 3–5).
--   dbo.CVM_CLIENT_ITEM_CAT_VER    только чтение (NOLOCK) в п. 6; строки дней пишет client_item_keys_day из CVM_CLIENT_ITEM.sql.
--   dbo.I_CVM_PRODUCT_CATEGORY     только чтение (NOLOCK): текущая версия справочника (п. 6).
--   dbo.CVM_CLIENT_ITEM_LOG        UPDATE только записей STATUS = 'RUN' под замком витрины (п. 3); строки кусков пишут сами
--                                  client_item_rebuild_keys и client_item_load.
--   msdb: задание CVM_CLIENT_ITEM_HISTORY — если есть и не идёт, удаляется и создаётся заново (удаление после успеха), затем
--         запускается; если идёт — не трогается. «MCI Regular update» и «MCI Reload every day» не меняются.
--   НЕ ТРОГАЕТСЯ И НЕ УДАЛЯЕТСЯ: процедуры и таблицы витрины (CVM_CLIENT_ITEM.sql), I_CHECK, I_CHECKHEADER, справочники.
--
-- ХОД И ОСТАНОВКА
--   Ход:        SELECT TOP 30 * FROM mci_model.dbo.CVM_CLIENT_ITEM_LOG WITH (NOLOCK) ORDER BY ID DESC;
--   Остановить: EXEC msdb.dbo.sp_stop_job @job_name = N'CVM_CLIENT_ITEM_HISTORY';
--               (прерванный день откатывается целиком; повторный F5 этого файла продолжит с того, чего нет).
--   Пока задание пишет витрину, сборка файла покупок конструктора ждёт.
-- ============================================================================================

USE mci_model
GO

-- ============================== 1. ОЖИДАНИЕ ==============================
CREATE OR ALTER PROCEDURE dbo.client_item_history_wait
    @quiet  bit = 0                 -- 1 — ещё и 10 минут тишины в журнале витрины (перед первым куском)
  , @reload bit = 0                 -- не используется (оставлен для совместимости вызова)
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @n int = 0, @t time;
    WHILE 1 = 1
    BEGIN
        SET @t = CAST(GETDATE() AS time);
        IF NOT EXISTS (SELECT 1
                       FROM msdb.dbo.sysjobactivity AS a WITH (NOLOCK)
                       JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = a.job_id
                       WHERE j.name = N'MCI Regular update'
                         AND a.session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions WITH (NOLOCK))
                         AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL)
           AND APPLOCK_TEST(N'public', N'CVM_CLIENT_ITEM', N'Exclusive', N'Session') = 1
           AND NOT (@t >= '00:20' AND @t < '01:30')
           AND (@quiet = 0 OR NOT EXISTS (SELECT 1 FROM dbo.CVM_CLIENT_ITEM_LOG WITH (NOLOCK)
                                          WHERE FINISHED >= DATEADD(minute, -10, SYSDATETIME())))
            BREAK;
        SET @n += 1;
        IF @n > 288
            THROW 50011, N'client_item_history: витрина занята или задания MCI идут больше суток — задание остановлено. Закрыть вкладки SSMS с загрузкой витрины и запустить файл снова.', 1;
        WAITFOR DELAY '00:05:00';
    END
END
GO

-- ============================== 2. КАТЕГОРИИ И ИСТОРИЯ ==============================
CREATE OR ALTER PROCEDURE dbo.client_item_history
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @from date, @upto date, @alter_dt datetime, @lk int, @i int = 1, @imax int, @kind tinyint, @d1 date, @d2 date,
            @tries int = 0, @err int = 0, @msg nvarchar(2000);
    DECLARE @ver int, @nst int, @nstale int, @j int = 1, @jmax int, @dur int = 0, @t0 datetime2(0), @now time, @done int = 0, @cut bit = 0,
            @per_day int = 0;

    -- 2.1 процедура ключей — с нашими категориями
    IF NOT EXISTS (SELECT 1 FROM sys.sql_modules WHERE object_id = OBJECT_ID(N'dbo.client_item_keys_day')
                   AND definition LIKE N'%I_CVM_PRODUCT_CATEGORY%' AND definition LIKE N'%CVM_CLIENT_ITEM_CAT_VER%'
                   AND definition LIKE N'%SHA2_256%')     -- та же формула версии, что в п. 2.7 (иначе все дни «старые» при каждом запуске)
       OR OBJECT_ID(N'dbo.CVM_CLIENT_ITEM_CAT_VER', N'U') IS NULL
    BEGIN
        RAISERROR(N'client_item_history: dbo.client_item_keys_day без категорий или без отметки версии справочника — сначала выполнить CVM_CLIENT_ITEM.sql (версия с I_CVM_PRODUCT_CATEGORY и CVM_CLIENT_ITEM_CAT_VER).', 16, 1);
        RETURN;
    END
    SET @alter_dt = (SELECT modify_date FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.client_item_keys_day'));

    -- 2.2 дождаться тишины и закрыть зависшие RUN под замком витрины
    EXEC dbo.client_item_history_wait @quiet = 1;
    EXEC @lk = sys.sp_getapplock @Resource = N'CVM_CLIENT_ITEM', @LockMode = N'Exclusive', @LockOwner = N'Session', @LockTimeout = 600000;
    IF @lk < 0
    BEGIN
        RAISERROR(N'client_item_history: витрину пишет другой прогон дольше 10 минут — задание остановлено, запустить файл позже.', 16, 1);
        RETURN;
    END
    UPDATE dbo.CVM_CLIENT_ITEM_LOG
    SET FINISHED = SYSDATETIME(), STATUS = 'ERROR',
        ERROR_MSG = LEFT(CONCAT(ISNULL(ERROR_MSG + N' | ', N''), N'Не завершён (остановлен) — закрыт client_item_history ',
                                CONVERT(varchar(19), SYSDATETIME(), 120)), 2000)
    WHERE STATUS = 'RUN';
    EXEC sys.sp_releaseapplock @Resource = N'CVM_CLIENT_ITEM', @LockOwner = N'Session';

    -- 2.3 границы истории
    SELECT @from = MIN(CAST(prv.value AS date))
    FROM sys.partition_range_values AS prv
    JOIN sys.partition_functions AS pf ON pf.function_id = prv.function_id
    WHERE pf.name = N'pf_CLIENT_ITEM_DAY';
    SELECT @upto = MAX(DATE_TO) FROM dbo.CVM_CLIENT_ITEM_LOG WITH (NOLOCK) WHERE STATUS = 'OK' AND MODE IN ('init', 'night');
    IF @upto IS NULL OR @upto >= CAST(GETDATE() AS date)
       OR NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobsteps AS s WITH (NOLOCK) JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = s.job_id
                      WHERE j.name = N'MCI Regular update' AND s.step_name = N'CLIENT_ITEM')
        SET @upto = DATEADD(day, -1, CAST(GETDATE() AS date));     -- ночного шага нет — новые дни тоже здесь
    IF @from IS NULL OR @from > @upto
    BEGIN
        RAISERROR(N'client_item_history: нет секций витрины или нечего догружать — задание завершено.', 10, 1) WITH NOWAIT;
        RETURN;
    END

    -- 2.4 дни: загружен ли (строки в секции дня; RANGE RIGHT → секция = номер границы + 1) и есть ли категории
    DROP TABLE IF EXISTS #d, #cat, #work;
    SELECT DATA = CAST(prv.value AS date),
           HAS_DAY = CASE WHEN ISNULL(s.R, 0) > 0 THEN 1 ELSE 0 END
    INTO #d
    FROM sys.partition_range_values AS prv
    JOIN sys.partition_functions AS pf ON pf.function_id = prv.function_id AND pf.name = N'pf_CLIENT_ITEM_DAY'
    LEFT JOIN (SELECT partition_number, R = SUM(row_count)
               FROM sys.dm_db_partition_stats
               WHERE object_id = OBJECT_ID(N'dbo.I_CLIENT_ITEM_DAY') AND index_id IN (0, 1)
               GROUP BY partition_number) AS s ON s.partition_number = prv.boundary_id + 1
    WHERE CAST(prv.value AS date) BETWEEN @from AND @upto;

    SELECT DISTINCT k.DATA
    INTO #cat
    FROM dbo.I_CLIENT_ITEM_KEY_DAY AS k WITH (NOLOCK)
    WHERE k.KEY_TYPE = 2 AND k.DATA BETWEEN @from AND @upto
    OPTION (RECOMPILE);

    -- 2.5 план: KIND 1 — пересборка ключей (загружен, без категорий), KIND 2 — загрузка дня (нет строк); дни, уже успешно
    --     обработанные после установки процедуры с категориями, не берутся; куски по 7 дней от новых к старым
    ;WITH x AS (
        SELECT d.DATA, KIND = CAST(CASE WHEN d.HAS_DAY = 1 THEN 1 ELSE 2 END AS tinyint)
        FROM #d AS d
        WHERE (d.HAS_DAY = 0 OR NOT EXISTS (SELECT 1 FROM #cat AS c WHERE c.DATA = d.DATA))
          AND NOT EXISTS (SELECT 1 FROM dbo.CVM_CLIENT_ITEM_LOG AS l WITH (NOLOCK)
                          WHERE l.STATUS = 'OK' AND l.STARTED >= @alter_dt AND d.DATA BETWEEN l.DATE_FROM AND l.DATE_TO
                            AND (l.MODE = 'init' OR (l.MODE = 'keys' AND d.HAS_DAY = 1)))
    ), g AS (
        SELECT DATA, KIND, GRP = DATEDIFF(day, '20000101', DATA) - ROW_NUMBER() OVER (PARTITION BY KIND ORDER BY DATA)
        FROM x
    ), c AS (
        SELECT DATA, KIND, GRP, CH = (ROW_NUMBER() OVER (PARTITION BY KIND, GRP ORDER BY DATA DESC) - 1) / 7
        FROM g
    )
    SELECT KIND, D1 = MIN(DATA), D2 = MAX(DATA),
           ORD = ROW_NUMBER() OVER (ORDER BY KIND, MAX(DATA) DESC)
    INTO #work
    FROM c
    GROUP BY KIND, GRP, CH;

    SELECT @imax = COUNT(*) FROM #work;
    SET @msg = CONCAT(N'client_item_history: история ', CONVERT(char(10), @from, 23), N' — ', CONVERT(char(10), @upto, 23),
                      N'; пересборка категорий: ', (SELECT COUNT(*) FROM #work WHERE KIND = 1), N' кусков, загрузка дней: ',
                      (SELECT COUNT(*) FROM #work WHERE KIND = 2), N' кусков.');
    RAISERROR(N'%s', 10, 1, @msg) WITH NOWAIT;

    -- 2.6 выполнение
    WHILE @i <= @imax
    BEGIN
        SELECT @kind = KIND, @d1 = D1, @d2 = D2 FROM #work WHERE ORD = @i;
        DECLARE @rl bit = CASE WHEN @kind = 2 AND @d2 >= DATEADD(day, -100, CAST(GETDATE() AS date)) THEN 1 ELSE 0 END;
        EXEC dbo.client_item_history_wait @reload = @rl;
        BEGIN TRY
            IF @kind = 1
                EXEC dbo.client_item_rebuild_keys @date_from = @d1, @date_to = @d2;
            ELSE
                EXEC dbo.client_item_load @mode = 'init', @date_from = @d1, @date_to = @d2;
            SET @i += 1;
            SET @tries = 0;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
            IF ERROR_NUMBER() = 50004 AND @tries < 12
            BEGIN
                SET @tries += 1;           -- замок держит ночной шаг или ручной прогон — тот же кусок позже
                WAITFOR DELAY '00:05:00';
            END
            ELSE
            BEGIN
                SET @err += 1;
                SET @msg = CONCAT(N'client_item_history: кусок ', CONVERT(char(10), @d1, 23), N' — ', CONVERT(char(10), @d2, 23),
                                  N' не выполнен: ', LEFT(ERROR_MESSAGE(), 1500));
                RAISERROR(N'%s', 10, 1, @msg) WITH NOWAIT;
                SET @i += 1;
                SET @tries = 0;
            END
        END CATCH
    END

    -- 2.7 категории по версии справочника: загруженные дни без отметки или со старой версией — пересборка ключей (чеки не читаются)
    IF @err > 0
        RAISERROR(N'client_item_history: были ошибки кусков — пересборку по версии справочника категорий не начинаю.', 10, 1) WITH NOWAIT;
    ELSE
    BEGIN
        -- текущая версия справочника — по тем же строкам и формуле, что в client_item_keys_day
        SELECT @ver = ISNULL(CHECKSUM_AGG(CAST(SUBSTRING(HASHBYTES('SHA2_256', CAST(pc.ID_PRODUCT AS binary(8))
                                    + CAST(pc.ID_CVM_CATEGORY AS binary(4)) + CAST(pc.[HOW] AS binary(1))), 1, 4) AS int)), 0)
        FROM dbo.I_CVM_PRODUCT_CATEGORY AS pc WITH (NOLOCK)
        WHERE pc.[HOW] > 0;

        -- загруженные дни заново (п. 5 мог догрузить дни); RIGHT-секции: секция = номер границы + 1
        DROP TABLE IF EXISTS #d2, #stale, #work2;
        SELECT DATA = CAST(prv.value AS date)
        INTO #d2
        FROM sys.partition_range_values AS prv
        JOIN sys.partition_functions AS pf ON pf.function_id = prv.function_id AND pf.name = N'pf_CLIENT_ITEM_DAY'
        JOIN (SELECT partition_number, R = SUM(row_count)
              FROM sys.dm_db_partition_stats
              WHERE object_id = OBJECT_ID(N'dbo.I_CLIENT_ITEM_DAY') AND index_id IN (0, 1)
              GROUP BY partition_number
              HAVING SUM(row_count) > 0) AS s ON s.partition_number = prv.boundary_id + 1
        WHERE CAST(prv.value AS date) BETWEEN @from AND @upto;

        SELECT v0.DATA
        INTO #stale
        FROM #d2 AS v0
        LEFT JOIN dbo.CVM_CLIENT_ITEM_CAT_VER AS v WITH (NOLOCK) ON v.DATA = v0.DATA
        WHERE v.DATA IS NULL OR v.CAT_VER <> @ver;

        SELECT @nst = COUNT(*) FROM #stale;

        -- порции: подряд идущие дни, до 7, от последних дней назад
        ;WITH g AS (
            SELECT DATA, GRP = DATEDIFF(day, '20000101', DATA) - ROW_NUMBER() OVER (ORDER BY DATA)
            FROM #stale
        ), c AS (
            SELECT DATA, GRP, CH = (ROW_NUMBER() OVER (PARTITION BY GRP ORDER BY DATA DESC) - 1) / 7
            FROM g
        )
        SELECT D1 = MIN(DATA), D2 = MAX(DATA), N = COUNT(*),
               ORD = ROW_NUMBER() OVER (ORDER BY MAX(DATA) DESC)
        INTO #work2
        FROM c
        GROUP BY GRP, CH;

        SELECT @jmax = COUNT(*) FROM #work2;
        SET @tries = 0;
        SET @msg = CONCAT(N'client_item_history: категории по версии справочника — дней со старой версией или без отметки: ', @nst,
                          N', порций: ', @jmax, N'.');
        RAISERROR(N'%s', 10, 1, @msg) WITH NOWAIT;

        WHILE @j <= @jmax
        BEGIN
            EXEC dbo.client_item_history_wait @reload = 0;

            SELECT @d1 = D1, @d2 = D2, @nstale = N FROM #work2 WHERE ORD = @j;

            -- остановка до 04:00: с полуночи до полудня порция не начинается, если уже 04:00 или позже либо до 04:00 остаётся меньше,
            -- чем заняли бы её дни по скорости прошлой порции (порции бывают от 1 до 7 дней)
            SET @now = CAST(GETDATE() AS time);
            IF @now < '12:00' AND (@now >= '04:00' OR DATEDIFF(second, @now, CAST('04:00' AS time)) < @per_day * @nstale)
            BEGIN
                SET @cut = 1;
                BREAK;
            END

            SET @t0 = SYSDATETIME();
            BEGIN TRY
                EXEC dbo.client_item_rebuild_keys @date_from = @d1, @date_to = @d2;
                SET @dur = DATEDIFF(second, @t0, SYSDATETIME());
                SET @per_day = (@dur + @nstale - 1) / @nstale;     -- секунд на день, с округлением вверх
                SET @done += @nstale;
                SET @j += 1;
                SET @tries = 0;
                SET @msg = CONCAT(N'client_item_history: категории пересобраны ', CONVERT(char(10), @d1, 23), N' — ',
                                  CONVERT(char(10), @d2, 23), N' (', @nstale, N' дн., ', @dur, N' с).');
                RAISERROR(N'%s', 10, 1, @msg) WITH NOWAIT;
            END TRY
            BEGIN CATCH
                IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
                IF ERROR_NUMBER() = 50004 AND @tries < 12
                BEGIN
                    SET @tries += 1;           -- замок держит ночной шаг или ручной прогон — та же порция позже
                    WAITFOR DELAY '00:05:00';
                END
                ELSE
                BEGIN
                    SET @err += 1;
                    SET @msg = CONCAT(N'client_item_history: категории не пересобраны за ', CONVERT(char(10), @d1, 23), N' — ',
                                      CONVERT(char(10), @d2, 23), N', стоп: ', LEFT(ERROR_MESSAGE(), 1500));
                    RAISERROR(N'%s', 10, 1, @msg) WITH NOWAIT;
                    BREAK;
                END
            END CATCH
        END

        IF @err = 0
        BEGIN
            SET @nst -= @done;
            IF @nst > 0
                SET @msg = CONCAT(N'client_item_history: осталось дней со старыми категориями: ', @nst,
                                  CASE WHEN @cut = 1 THEN N' (время до 04:00 вышло)' ELSE N'' END,
                                  N' — следующим вечером запустить этот же скрипт.');
            ELSE
                SET @msg = N'client_item_history: категории во всей загруженной истории соответствуют текущему справочнику.';
            RAISERROR(N'%s', 10, 1, @msg) WITH NOWAIT;
        END
    END

    IF @err > 0
    BEGIN
        SET @msg = CONCAT(N'client_item_history: не выполнено кусков — ', @err, N'; причины — в CVM_CLIENT_ITEM_LOG (STATUS = ERROR). ',
                          N'Повторный запуск файла догрузит то, чего нет.');
        RAISERROR(N'%s', 16, 1, @msg);
    END
END
GO

-- ============================== 3. РАЗОВОЕ ЗАДАНИЕ И ЗАПУСК ==============================
USE msdb
GO
SET NOCOUNT ON;
DECLARE @job sysname = N'CVM_CLIENT_ITEM_HISTORY', @rc int = 0;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobactivity AS a WITH (NOLOCK)
           JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = a.job_id
           WHERE j.name = @job
             AND a.session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions WITH (NOLOCK))
             AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL)
BEGIN
    RAISERROR(N'Задание CVM_CLIENT_ITEM_HISTORY уже идёт — не трогаю.', 10, 1);
    RETURN;
END

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WITH (NOLOCK) WHERE name = @job)
BEGIN
    EXEC @rc = msdb.dbo.sp_delete_job @job_name = @job;
    IF @rc <> 0 BEGIN RAISERROR(N'СТОП: старое задание CVM_CLIENT_ITEM_HISTORY не удалено — новое не создано.', 16, 1); RETURN; END
END

EXEC @rc = msdb.dbo.sp_add_job @job_name = @job, @enabled = 1, @delete_level = 1,
     @description = N'Разовая работа с витриной покупок клиента: категории в загруженных днях и догрузка истории (CVM_CLIENT_ITEM_HISTORY.sql); после успеха удаляется.';
IF @rc <> 0 BEGIN RAISERROR(N'СТОП: задание не создано (sp_add_job).', 16, 1); RETURN; END
EXEC @rc = msdb.dbo.sp_add_jobstep @job_name = @job, @step_name = N'history', @subsystem = N'TSQL',
     @database_name = N'mci_model', @command = N'exec dbo.client_item_history;',
     @on_success_action = 1, @on_fail_action = 2;
IF @rc <> 0 BEGIN EXEC msdb.dbo.sp_delete_job @job_name = @job; RAISERROR(N'СТОП: шаг задания не создан — задание удалено.', 16, 1); RETURN; END
EXEC @rc = msdb.dbo.sp_add_jobserver @job_name = @job;
IF @rc <> 0 BEGIN EXEC msdb.dbo.sp_delete_job @job_name = @job; RAISERROR(N'СТОП: задание не привязано к серверу — удалено.', 16, 1); RETURN; END

EXEC @rc = msdb.dbo.sp_start_job @job_name = @job;
IF @rc <> 0 RAISERROR(N'СТОП: задание создано, но не запустилось — запустить вручную: EXEC msdb.dbo.sp_start_job @job_name = N''CVM_CLIENT_ITEM_HISTORY'';', 16, 1);
GO

-- что запущено
SELECT j.name, j.enabled, a.start_execution_date, a.stop_execution_date
FROM msdb.dbo.sysjobs AS j WITH (NOLOCK)
LEFT JOIN msdb.dbo.sysjobactivity AS a WITH (NOLOCK)
       ON a.job_id = j.job_id AND a.session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions WITH (NOLOCK))
WHERE j.name = N'CVM_CLIENT_ITEM_HISTORY';
GO
