-- ============================================================================================
-- CVM_MISSIONS_REBUILD — пересчёт отчёта по миссиям заново (I_CHECK_MISSION, I_MISSION_REPORT) за 202401–202608
-- текущей процедурой dbo.mission_report (v4), по вечерам. Запустить ОДИН РАЗ целиком в SSMS (F5).
--
-- Почему по вечерам: месяц миссий считается ~1 час (журнал джоба 10.10: шаг 44 — 56 мин), 32 месяца — больше суток;
--   одним запуском это пересеклось бы с ночными джобами MCI (шаг 44 вызывает ту же процедуру, с 01:00 — перезагрузка данных).
-- Что делает:
--   1. dbo.mission_report (текст процедуры — из базы на 10.10.2026; расчёт месяца не меняется):
--      — ночной вызов без параметров (шаг 44) считает, как for_i_report, со следующего месяца после последнего месяца
--        в витрине I_MISSION_REPORT, но не позже окна перезаливки (месяц «сегодня − 10 дней»). Раньше — только окно
--        перезаливки, и месяцы, которые ночная очистка clear_data_update удаляет в первую пятницу месяца (с 1-го числа
--        три месяца назад), не возвращались: 06.11.2026 пропали бы август и сентябрь. Теперь ночь первой пятницы
--        считает их заново (~1 ч на месяц);
--      — необязательный параметр @YM_STOP (граница окна, не включается) — для пересчёта истории по месяцам.
--   2. Служебная таблица dbo.CVM_MISSIONS_REBUILD — с какого месяца продолжать и сколько месяцев за вечер (PORTION).
--   3. Процедура dbo.cvm_missions_rebuild_step — вечерний проход: считает месяцы по одному, не больше PORTION за проход;
--      отметка сдвигается после каждого посчитанного месяца (упал месяц — нет). Месяц не начинается, если идёт ночной
--      джоб MCI или CVM REBUILD ONCE, если сейчас не вечер (17:00–00:30) или месяц не успеет до 00:30 (оценка — время
--      прошлого месяца этого прохода, для первого — 1 ч). Второй экземпляр прохода не стартует. Дошла до конца — выключает свой джоб.
--   4. Джоб «CVM MISSIONS REBUILD» — каждый день в 17:00 (после ночного прохода, до перезагрузки в 01:00).
--   5. Разовый джоб «CVM REBUILD ONCE» (полная пересборка трендов и миссий за ~66 ч) выключается — не удаляется.
--   6. Первый проход стартует сразу, если сейчас вечер (17:00–00:30) и ночной джоб MCI не идёт.
--   — чтения витрины, msdb и служебной таблицы — с (nolock) (чек-лист п. 13, Елена 11.10.2026); результат не меняется.
-- PORTION = 5 месяцев (~5 ч), 32 месяца — ~7 вечеров. Сентябрь 2026 уже посчитан новой процедурой 10.10.
--   Сменить: UPDATE dbo.CVM_MISSIONS_REBUILD SET PORTION = 3;  Ход и ошибки: SELECT * FROM dbo.CVM_MISSIONS_REBUILD;
--   Внеплановый проход: EXEC msdb.dbo.sp_start_job N'CVM MISSIONS REBUILD';  (не EXEC процедуры в окне SSMS:
--   прерванный по Stop, он держит замок прохода, пока окно открыто, и вечерние проходы пропускаются)
--   Ночной джоб MCI шёл после 17:00 — вечер пропускается (LAST_NOTE), следующий проход — завтра в 17:00 или вручную.
-- Пока месяц пересчитывается (~1 ч), его нет в отчёте «Миссии»; упал месяц — его нет до следующего успешного прохода.
-- Ценовой индекс и проникновение в пересчитанных месяцах — по текущим ценовым сегментам клиентов
--   (I_CONTACT_PRICE_SEGMENT без даты — так устроена процедура v4, ночной расчёт тоже).
-- ============================================================================================

USE mci_model
GO
SET NOCOUNT ON;

-- ============================== 0. ПРОВЕРКИ ==============================
DECLARE @abort bit = 0;
-- isnull: нет процедуры / нет прав на определение → NULL, и без isnull проверка молча проходила бы
IF ISNULL(OBJECT_DEFINITION(OBJECT_ID('dbo.mission_report')), N'') NOT LIKE N'%dbo.mission_report — v4%'
BEGIN
    RAISERROR(N'СТОП: dbo.mission_report в базе не v4 — текст процедуры изменился, ничего не меняю.', 16, 1);
    SET @abort = 1;
END
IF OBJECT_ID('dbo.CVM_MISSIONS_REBUILD') IS NOT NULL
BEGIN
    RAISERROR(N'СТОП: пересчёт миссий уже заведён (таблица dbo.CVM_MISSIONS_REBUILD есть) — ничего не меняю. Если джоба «CVM MISSIONS REBUILD» нет — выдели и выполни только разделы 4 и 6.', 16, 1);
    SET @abort = 1;
END
-- выключение (раздел 5) уже идущий джоб не останавливает: его шаг 2 (mission_report с 202401 без границы)
-- удалил бы и пересчитал все месяцы с 202401 параллельно порциям
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
-- ALTER (раздел 1) не ждёт идущего расчёта: если mission_report сейчас выполняется (ночной шаг 44, ~1 ч), идущий вызов
-- при компиляции очередного оператора (операторы с #yearmonth, #mission_cats компилируются по ходу) может упасть
-- с ошибкой 2801 «определение объекта изменилось после компиляции» — шаг 44 ночного джоба упадёт
IF EXISTS (SELECT 1
           FROM sys.dm_exec_requests AS r
                CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
           WHERE t.dbid = DB_ID() AND t.objectid = OBJECT_ID('dbo.mission_report'))
BEGIN
    RAISERROR(N'СТОП: сейчас выполняется dbo.mission_report (ночной шаг 44 или другой запуск) — запустить скрипт после окончания расчёта, ничего не меняю.', 16, 1);
    SET @abort = 1;
END
IF @abort = 1 SET NOEXEC ON;

-- ============================== 1. ПРОЦЕДУРА МИССИЙ: граница окна ==============================
-- (заголовок раздела — до GO: в текст процедуры в базе попадает только её собственный заголовок, как в исходнике)
GO
-- ============================== 3. dbo.mission_report — v4 ==============================
-- Источник: dixy-dashboard/данные/sql_mission_report_v4.sql; вместо TRUNCATE — окно @YM_START (ночной инкремент).
CREATE OR ALTER PROCEDURE [dbo].[mission_report] @YM_START int = NULL
    , @YM_STOP int = NULL   -- граница окна, месяц НЕ включается; NULL — до текущего месяца
AS
SET NOCOUNT ON;

DECLARE @nightly bit = IIF(@YM_START IS NULL, 1, 0);   -- ночной режим: вызов без @YM_START (шаг 44)
DECLARE @last_month int;

IF @YM_START IS NULL
    SET @YM_START = (SELECT YEAR_MONTH FROM dbo.I_CALENDAR (nolock)
                     WHERE [DATE] = convert(date, dateadd(day, -10, getdate())));

-- ─── #mission_cats: категорийный срез каталога ──────────────────────────
-- Daily-миссия 100 «Ежедневное» лежит в самом I_MISSION (см.
-- sql_migrate_daily_mission.sql — миграция, удалившая 101-106
-- и вставившая 100). Здесь читаем I_MISSION напрямую без переразметки.
-- ─────────────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS #mission_cats;
SELECT    ID_MISSION
        , ID_CATEGORY_5_ext
        , ID_COMPANY
        , MAIN_CAT     = MAX(MAIN)
        , COUNT_MAIN   = MAX(COUNT_MAIN)
        , MISSION_RANK = MAX(MISSION_RANK)
INTO      #mission_cats
FROM      I_MISSION (NOLOCK)
GROUP BY  ID_MISSION, ID_CATEGORY_5_ext, ID_COMPANY;

CREATE NONCLUSTERED INDEX IX_mc ON #mission_cats (ID_COMPANY, ID_CATEGORY_5_ext, ID_MISSION);


DROP TABLE IF EXISTS #c;
SELECT   ID_COMPANY
       , RN = ROW_NUMBER() OVER (ORDER BY ID_COMPANY)
INTO     #c
FROM     I_ORGANIZATION (NOLOCK)
GROUP BY ID_COMPANY;

DECLARE @c   INT = 1;
DECLARE @idc INT = (SELECT ID_COMPANY FROM #c WHERE RN = @c);

WHILE @c <= 1
BEGIN

        -- ночной режим — как for_i_report: со следующего месяца после последнего месяца в витрине, но не позже окна
        -- перезаливки. Так возвращаются и месяцы, удалённые ночной очисткой clear_data_update (в первую пятницу месяца —
        -- с 1-го числа три месяца назад). Пустая витрина — только окно перезаливки (историю считает вечерний пересчёт).
        IF @nightly = 1
        BEGIN
            SET @last_month = (SELECT max(YEAR_MONTH) FROM I_MISSION_REPORT (nolock) WHERE ID_COMPANY = @idc AND YEAR_MONTH < @YM_START);
            IF @last_month IS NOT NULL
                SET @YM_START = (SELECT min(YEAR_MONTH) FROM I_CALENDAR (NOLOCK) WHERE YEAR_MONTH > @last_month);
        END

        -- окно пересборки: свои данные с @YM_START удаляются и считаются заново (идемпотентно)
        DELETE FROM I_CHECK_MISSION  WHERE ID_COMPANY = @idc AND YEAR_MONTH >= @YM_START AND YEAR_MONTH < ISNULL(@YM_STOP, 2147483647);
        DELETE FROM I_MISSION_REPORT WHERE ID_COMPANY = @idc AND YEAR_MONTH >= @YM_START AND YEAR_MONTH < ISNULL(@YM_STOP, 2147483647);

    DROP TABLE IF EXISTS #yearmonth;
    SELECT   YEAR_MONTH
           , RN = ROW_NUMBER() OVER (ORDER BY YEAR_MONTH)
    INTO     #yearmonth
    FROM     I_CALENDAR (NOLOCK)
    GROUP BY YEAR_MONTH;

    DECLARE @act_month INT = (SELECT MAX(YEAR_MONTH) FROM I_CHECKHEADER (NOLOCK) WHERE ID_COMPANY = @idc AND ID_ORGANIZATION = 1);
    DECLARE @YM INT = @YM_START;

    WHILE @YM < @act_month AND (@YM_STOP IS NULL OR @YM < @YM_STOP)
    BEGIN

        ------------------------------------------------------------
        -- I_CHECK_MISSION: одна строка на (миссия, чек, контакт)
        ------------------------------------------------------------
        INSERT INTO I_CHECK_MISSION
            ( ID_MISSION,  YEAR_MONTH,  ID_CONTACT,  ID_CHECK,
              MAIN,        COUNT_MAIN,  MISSION_RANK,
              COST_DISCOUNT, TOTAL_CHECK_COST, TOTAL_CHECK_SKU, REAL_DISCOUNT,
              WEIGHT_MISSION, QUANTITY_MISSION, ID_COMPANY )
        SELECT  c.ID_MISSION,
                a.YEAR_MONTH,
                a.ID_CONTACT,
                a.ID_CHECK,
                MAIN              = CASE
                                        WHEN c.ID_MISSION = 100
                                        THEN COUNT(DISTINCT CASE WHEN c.MAIN_CAT = 1 THEN a.ID_PRODUCT END)            -- daily: по distinct продуктам
                                        ELSE COUNT(DISTINCT CASE WHEN c.MAIN_CAT = 1 THEN p.ID_CATEGORY_5_ext END)     -- прочие: по distinct категориям
                                    END,
                COUNT_MAIN        = MAX(c.COUNT_MAIN),
                MISSION_RANK      = MAX(c.MISSION_RANK),
                COST_DISCOUNT     = SUM(a.COST_DISCOUNT),
                TOTAL_CHECK_COST  = MAX(b.COST_DISCOUNT),
                TOTAL_CHECK_SKU   = MAX(b.SKU),
                REAL_DISCOUNT     = SUM(a.REAL_DISCOUNT),
                WEIGHT_MISSION    = SUM(a.COST_DISCOUNT) / NULLIF(MAX(b.COST_DISCOUNT), 0),
                QUANTITY_MISSION  = COUNT(DISTINCT a.ID_PRODUCT) * 1.00 / NULLIF(MAX(b.SKU) * 1.00, 0),
                a.ID_COMPANY
        FROM    I_CHECK         AS a (NOLOCK)
                INNER JOIN I_CHECKHEADER AS b (NOLOCK)
                    ON  a.ID_CHECK   = b.ID_CHECK
                    AND a.YEAR_MONTH = b.YEAR_MONTH
                    AND a.ID_COMPANY = b.ID_COMPANY
                    AND a.ID_COMPANY = @idc
                INNER JOIN I_PRODUCT     AS p (NOLOCK)
                    ON  a.ID_PRODUCT = p.ID_PRODUCT
                    AND a.ID_COMPANY = p.ID_COMPANY
                INNER JOIN #mission_cats AS c
                    ON  p.ID_CATEGORY_5_ext = c.ID_CATEGORY_5_ext
                    AND p.ID_COMPANY        = c.ID_COMPANY
        WHERE   a.YEAR_MONTH      = @YM
            AND a.ID_ORGANIZATION = 1
            AND a.ID_CONTACT     <> 0
        GROUP BY c.ID_MISSION, a.YEAR_MONTH, a.ID_CONTACT, a.ID_CHECK, a.ID_COMPANY;


        ------------------------------------------------------------
        -- I_MISSION_REPORT: агрегат по миссии × месяцу
        ------------------------------------------------------------
        ;WITH t AS
        (
            SELECT  a.YEAR_MONTH,
                    CONTACTS  = COUNT(DISTINCT a.ID_CONTACT) * 1.00,
                    CHECKS    = COUNT(a.ID_CHECK) * 1.00,
                    COST      = SUM(a.COST_DISCOUNT),
                    REAL_DI   = SUM(a.REAL_DISCOUNT),
                    AVG_SKU   = AVG(a.SKU * 1.00),
                    PRICE_INDEX_ALL       = AVG(c.PRICE_INDEX),
                    PENETRATION_CHECK_ALL = AVG(c.PENETRATION_CHECK),
                    a.ID_COMPANY,
                    a.ID_ORGANIZATION
            FROM    [mci_model].[dbo].[I_CHECKHEADER] AS a (NOLOCK)
                    LEFT JOIN I_CONTACT_PRICE_SEGMENT AS c (NOLOCK)
                        ON  a.ID_CONTACT      = c.ID_CONTACT
                        AND a.ID_COMPANY      = c.ID_COMPANY
                        AND a.ID_ORGANIZATION = c.ID_ORGANIZATION
            WHERE   a.YEAR_MONTH      = @YM
                AND a.ID_COMPANY      = @idc
                AND a.ID_ORGANIZATION = 1
                AND a.ID_CONTACT     <> 0
            GROUP BY a.YEAR_MONTH, a.ID_COMPANY, a.ID_ORGANIZATION
        )
        INSERT INTO I_MISSION_REPORT
            ( ID_MISSION, MISSION, YEAR_MONTH,
              COUNT_CHECK, SHARE_CHECKS, COUNT_CLIENT, SHARE_CLIENTS,
              [TO], TOTAL_CHECK_TO, SHARE_TO,
              REAL_DISCOUNT, WEIGHT_MISSION, QUANTITY_MISSION,
              PRICE_INDEX_to_all, PENETRATION_CHECK_to_all,
              AVG_SKU_PER_CHECK, BASE_AVG_SKU, AVG_SKU_to_all,
              AVG_CHECK_MISSION, BASE_AVG_CHECK, AVG_CHECK_to_all,
              BASE_TO, BASE_CHECKS, BASE_CONTACTS,
              ID_COMPANY )
        SELECT  a.ID_MISSION,
                MISSION         = c.MISSION,
                a.YEAR_MONTH,
                COUNT_CHECK     = COUNT(ID_CHECK),
                SHARE_CHECKS    = (COUNT(ID_CHECK) * 1.00) / (CHECKS * 1.00),
                COUNT_CLIENT    = COUNT(DISTINCT a.ID_CONTACT),
                SHARE_CLIENTS   = (1.00 * COUNT(DISTINCT a.ID_CONTACT)) / (CONTACTS * 1.00),
                [TO]            = SUM(a.COST_DISCOUNT),
                TOTAL_CHECK_TO  = SUM(a.TOTAL_CHECK_COST),
                SHARE_TO        = SUM(a.COST_DISCOUNT) / COST,
                REAL_DISCOUNT   = SUM(a.REAL_DISCOUNT),
                WEIGHT_MISSION  = AVG(a.WEIGHT_MISSION),
                QUANTITY_MISSION= AVG(a.QUANTITY_MISSION),
                PRICE_INDEX_to_all       = (AVG(b.PRICE_INDEX)       - PRICE_INDEX_ALL)       / PRICE_INDEX_ALL,
                PENETRATION_CHECK_to_all = (AVG(b.PENETRATION_CHECK) - PENETRATION_CHECK_ALL) / PENETRATION_CHECK_ALL,
                AVG_SKU_PER_CHECK        = AVG(a.TOTAL_CHECK_SKU * 1.00),
                BASE_AVG_SKU             = AVG_SKU,
                AVG_SKU_to_all           = (AVG(a.TOTAL_CHECK_SKU * 1.00) - AVG_SKU) / NULLIF(AVG_SKU, 0),
                AVG_CHECK_MISSION        = SUM(a.COST_DISCOUNT) * 1.00 / NULLIF(COUNT(ID_CHECK), 0),
                BASE_AVG_CHECK           = COST * 1.00 / NULLIF(CHECKS, 0),
                AVG_CHECK_to_all         = (SUM(a.COST_DISCOUNT) * 1.00 / NULLIF(COUNT(ID_CHECK), 0)
                                            - COST * 1.00 / NULLIF(CHECKS, 0))
                                           / NULLIF(COST * 1.00 / NULLIF(CHECKS, 0), 0),
                BASE_TO         = COST,
                BASE_CHECKS     = CHECKS,
                BASE_CONTACTS   = CONTACTS,
                a.ID_COMPANY
        FROM    I_CHECK_MISSION AS a (nolock)
                INNER JOIN t
                    ON  t.YEAR_MONTH = a.YEAR_MONTH
                    AND a.ID_COMPANY = t.ID_COMPANY
                LEFT JOIN I_CONTACT_PRICE_SEGMENT AS b (NOLOCK)
                    ON  a.ID_CONTACT      = b.ID_CONTACT
                    AND a.ID_COMPANY      = b.ID_COMPANY
                    AND t.ID_ORGANIZATION = b.ID_ORGANIZATION
                INNER JOIN (
                    SELECT   ID_MISSION, MISSION, ID_COMPANY
                    FROM     I_MISSION (NOLOCK)
                    WHERE    ID_COMPANY = @idc
                    GROUP BY ID_MISSION, MISSION, ID_COMPANY
                ) AS c
                    ON  a.ID_MISSION = c.ID_MISSION
                    AND a.ID_COMPANY = c.ID_COMPANY
        WHERE   a.MAIN > 0
            AND a.MAIN >= a.COUNT_MAIN
            AND a.YEAR_MONTH = @YM             -- ← ПРАВКА #1: убирает O(N²)
            AND a.ID_COMPANY = @idc
        GROUP BY a.ID_MISSION, c.MISSION, a.YEAR_MONTH,
                 CHECKS, COST, REAL_DI, CONTACTS,
                 AVG_SKU,
                 PRICE_INDEX_ALL, PENETRATION_CHECK_ALL,
                 a.ID_COMPANY
        ORDER BY a.YEAR_MONTH DESC, SUM(a.COST_DISCOUNT) DESC;


        SET @YM         = (SELECT YEAR_MONTH FROM #yearmonth
                           WHERE RN = (SELECT RN + 1 FROM #yearmonth WHERE YEAR_MONTH = @YM));

    END

    SET @c = @c + 1;
    SET @idc = (SELECT ID_COMPANY FROM #c WHERE RN = @c);

END
GO

-- процедура не обновилась (ALTER упал) — дальше ничего не создаём: иначе таблица отметки уже есть, повтор скрипта
-- остановится на охране, а шаг звал бы mission_report с лишним параметром
IF NOT EXISTS (SELECT 1 FROM sys.parameters WHERE object_id = OBJECT_ID('dbo.mission_report') AND name = N'@YM_STOP')
BEGIN
    RAISERROR(N'СТОП: dbo.mission_report не обновилась (нет параметра @YM_STOP) — таблицу, шаг и джоб не создаю.', 16, 1);
    SET NOEXEC ON;
END
GO

-- ============================== 2. ОТМЕТКА ХОДА ==============================
CREATE TABLE dbo.CVM_MISSIONS_REBUILD (
      NEXT_YM   int           NOT NULL   -- с какого месяца следующая порция
    , STOP_YM   int           NOT NULL   -- до какого месяца (не включается)
    , PORTION   int           NOT NULL   -- месяцев за вечер
    , DONE      bit           NOT NULL
    , UPDATED   datetime2(0)  NOT NULL
    , LAST_NOTE nvarchar(300) NULL
);
INSERT INTO dbo.CVM_MISSIONS_REBUILD (NEXT_YM, STOP_YM, PORTION, DONE, UPDATED, LAST_NOTE)
VALUES (202401, 202609, 5, 0, sysdatetime(), N'заведено');
GO

-- ============================== 3. ВЕЧЕРНИЙ ПРОХОД ==============================
CREATE OR ALTER PROCEDURE dbo.cvm_missions_rebuild_step
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @next int, @stop int, @portion int, @done bit, @to int, @n int = 0, @lock int
          , @t0 datetime2(0), @now datetime2(0), @deadline datetime2(0), @last_sec int
          , @msg nvarchar(300), @note nvarchar(300);

    -- второй экземпляр (ручной EXEC при идущем джобе) не начинает: тот же месяц посчитался бы дважды.
    -- Замок сессионный: EXEC, прерванный в окне SSMS (Stop — это отмена, а не ошибка, CATCH её не ловит), держит его,
    -- пока окно открыто, и все проходы до закрытия окна пропускаются — внеплановый проход только через sp_start_job.
    EXEC @lock = sp_getapplock @Resource = N'cvm_missions_rebuild_step', @LockMode = N'Exclusive',
                               @LockOwner = N'Session', @LockTimeout = 0;
    IF @lock < 0
    BEGIN
        UPDATE dbo.CVM_MISSIONS_REBUILD
        SET UPDATED = sysdatetime(),
            LAST_NOTE = N'пропуск: идёт другой проход — джоб или EXEC в окне SSMS (прерванный EXEC держит замок, пока окно открыто)';
        RETURN;
    END

    SELECT TOP (1) @next = NEXT_YM, @stop = STOP_YM, @portion = PORTION, @done = DONE FROM dbo.CVM_MISSIONS_REBUILD (nolock);

    -- граница вечера: ближайшие 00:30 — за полчаса до перезагрузки «MCI Reload every day» в 01:00
    SET @now = sysdatetime();
    SET @deadline = dateadd(minute, 30, convert(datetime2(0), convert(date, @now)));   -- 00:30 сегодня
    IF @deadline <= @now SET @deadline = dateadd(day, 1, @deadline);                   -- прошло → 00:30 завтра

    WHILE @done = 0 AND @next < @stop AND @n < @portion
    BEGIN
        -- Проверки, граница месяца и расчёт — в одном TRY: любая ошибка обрывает проход — отметка не сдвигается,
        -- шаг джоба падает, текст — в LAST_NOTE. Без TRY ошибка, которая обрывает один оператор, а не батч,
        -- не останавливала бы процедуру: нечитаемая msdb (права владельца джоба) пропустила бы проверку MCI — IF с ошибкой
        -- в условии не выполняет ни одну ветку — и месяц пошёл бы при идущем джобе; неверный NEXT_YM (ручная правка,
        -- например 202413) оставил бы @to = NULL — mission_report без верхней границы, то есть ночной режим: удаление
        -- всех месяцев с NEXT_YM; сбой внутри mission_report (усечение, NULL в NOT NULL, нехватка места, «слишком много
        -- аргументов», если процедуру заменят) — UPDATE ниже сдвинул бы отметку через недосчитанный месяц, и он остался
        -- бы дырой.
        BEGIN TRY
            SET @now = sysdatetime();
            -- с 00:30 до 17:00 идут ночные джобы MCI (перезагрузка с 01:00, ночной проход ~03:15–15:30)
            IF convert(time(0), @now) >= '00:30' AND convert(time(0), @now) < '17:00'
            BEGIN
                SET @note = N'остановлено: не вечер (месяцы считаются с 17:00 до 00:30)';
                BREAK;
            END
            -- ночной джоб MCI или старый разовый догон идёт — месяц не начинаем (шаг 44 и CVM REBUILD ONCE пишут те же таблицы;
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
            -- месяц не успеет до 00:30: оценка — время прошлого месяца этого прохода, для первого — 1 ч (журнал 10.10: 56 мин)
            IF dateadd(second, ISNULL(@last_sec, 3600), @now) > @deadline
            BEGIN
                SET @note = N'остановлено: следующий месяц не успеет до 00:30';
                BREAK;
            END

            SET @to = year(dateadd(month, 1, datefromparts(@next / 100, @next % 100, 1))) * 100
                    + month(dateadd(month, 1, datefromparts(@next / 100, @next % 100, 1)));
            SET @t0 = sysdatetime();
            EXEC dbo.mission_report @YM_START = @next, @YM_STOP = @to;
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0 ROLLBACK;
            SET @msg = concat(N'ошибка в месяце ', @next, N': ', ERROR_MESSAGE());
            UPDATE dbo.CVM_MISSIONS_REBUILD SET UPDATED = sysdatetime(), LAST_NOTE = @msg;
            EXEC sp_releaseapplock @Resource = N'cvm_missions_rebuild_step', @LockOwner = N'Session';
            THROW;
        END CATCH

        -- отметка сдвигается только после посчитанного месяца: упавший месяц пересчитается целиком в следующий раз
        SET @last_sec = datediff(second, @t0, sysdatetime());
        SET @msg = concat(N'посчитан ', @next, N' за ', @last_sec / 60, N' мин');
        SET @next = @to;
        SET @n += 1;
        UPDATE dbo.CVM_MISSIONS_REBUILD
        SET NEXT_YM = @next, DONE = IIF(@next >= @stop, 1, 0), UPDATED = sysdatetime(), LAST_NOTE = @msg;
    END

    IF @next >= @stop
    BEGIN
        UPDATE dbo.CVM_MISSIONS_REBUILD SET UPDATED = sysdatetime(), LAST_NOTE = N'пересчёт закончен';
        EXEC msdb.dbo.sp_update_job @job_name = N'CVM MISSIONS REBUILD', @enabled = 0;
    END
    ELSE IF @next IS NOT NULL
        UPDATE dbo.CVM_MISSIONS_REBUILD
        SET UPDATED = sysdatetime(),
            LAST_NOTE = concat(N'за проход посчитано месяцев: ', @n, N'; следующий — ', @next, ISNULL(N'; ' + @note, N''));

    EXEC sp_releaseapplock @Resource = N'cvm_missions_rebuild_step', @LockOwner = N'Session';
END
GO

-- ============================== 4. ДЖОБ: каждый день в 17:00 ==============================
-- владелец — как у работающего ночного джоба: Windows-владелец Agent проверяет в домене при каждом запуске
DECLARE @owner sysname = (SELECT TOP (1) SUSER_SNAME(owner_sid) FROM msdb.dbo.sysjobs (nolock) WHERE name = N'MCI Regular update');
IF @owner IS NULL SET @owner = suser_sname();
IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs (nolock) WHERE name = N'CVM MISSIONS REBUILD')
    EXEC msdb.dbo.sp_delete_job @job_name = N'CVM MISSIONS REBUILD';   -- остаток прошлого незавершённого запуска этого же скрипта
EXEC msdb.dbo.sp_add_job @job_name = N'CVM MISSIONS REBUILD', @enabled = 1, @owner_login_name = @owner,
     @description = N'Пересчёт отчёта по миссиям 202401–202608 порциями (CVM_MISSIONS_REBUILD.sql). Сам выключается в конце.';
EXEC msdb.dbo.sp_add_jobstep @job_name = N'CVM MISSIONS REBUILD', @step_id = 1, @step_name = N'mission_report portion',
     @subsystem = N'TSQL', @database_name = N'mci_model', @command = N'exec dbo.cvm_missions_rebuild_step;',
     @on_success_action = 1, @on_fail_action = 2;
EXEC msdb.dbo.sp_add_jobschedule @job_name = N'CVM MISSIONS REBUILD', @name = N'CVM MISSIONS REBUILD 17:00',
     @enabled = 1, @freq_type = 4, @freq_interval = 1, @freq_subday_type = 1, @active_start_time = 170000;   -- ежедневно, один раз в 17:00
EXEC msdb.dbo.sp_add_jobserver @job_name = N'CVM MISSIONS REBUILD';
GO

-- ============================== 5. РАЗОВЫЙ «CVM REBUILD ONCE» — ВЫКЛЮЧИТЬ ==============================
IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs (nolock) WHERE name = N'CVM REBUILD ONCE')
    EXEC msdb.dbo.sp_update_job @job_name = N'CVM REBUILD ONCE', @enabled = 0;
GO

-- ============================== 6. ПЕРВЫЙ ПРОХОД — СРАЗУ, ЕСЛИ СЕЙЧАС ВЕЧЕР И MCI НЕ ИДЁТ ==============================
-- успеет ли месяц до 00:30, проход проверяет сам перед каждым месяцем
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
ELSE
BEGIN
    EXEC msdb.dbo.sp_start_job @job_name = N'CVM MISSIONS REBUILD';
    PRINT N'Первый проход запущен. Ход: SELECT * FROM dbo.CVM_MISSIONS_REBUILD;';
END
GO

SET NOEXEC OFF;
GO

-- ============================== 7. ИТОГ ==============================
SELECT j.name, j.enabled, s.name AS schedule_name, s.active_start_time
FROM msdb.dbo.sysjobs AS j (nolock)
     LEFT JOIN msdb.dbo.sysjobschedules AS js (nolock) ON js.job_id = j.job_id
     LEFT JOIN msdb.dbo.sysschedules AS s (nolock) ON s.schedule_id = js.schedule_id
WHERE j.name IN (N'CVM MISSIONS REBUILD', N'CVM REBUILD ONCE');
SELECT YEAR_MONTH, n = count(*) FROM dbo.I_MISSION_REPORT (nolock) WHERE ID_COMPANY = 1 GROUP BY YEAR_MONTH ORDER BY YEAR_MONTH;
GO
-- отдельным батчем: после «СТОП» таблицы может не быть — ошибка 208 оборвала бы батч и скрыла итог выше
SELECT * FROM dbo.CVM_MISSIONS_REBUILD (nolock);
