-- ============================================================================================
-- CVM_CHECK_COMPLETENESS — полнота чеков и строк в mci_model после ночной загрузки из CDP:
-- чеки по дням, часам, магазинам и каналам за всю историю, дни с провалом по каналу, письмо каждый день.
--
-- Перезаливку и интеграционные процедуры скрипт НЕ трогает: задания MCI Reload*, MCI Regular update, их шаги и процедуры
-- загрузки (check_insert, clear_data_update, check_checkheader_processing и др.) не меняются. По просьбе Елены заменяется
-- только старая сверка dbo.check_data (шаг 13 CHECKING_DATA, не интеграционная): вместо I_MCI_CHECK* она запускает
-- задание проверки полноты (раздел 6).
-- Таблицы MCI только читаются (NOLOCK); скрипт пишет только в свои новые таблицы I_CHECK_COMPLETENESS* и создаёт своё
-- задание SQL Agent «CVM check completeness».
--
-- Что считается (компания 1 = ДИКСИ):
--   dbo.I_CHECK_COMPLETENESS        день × час × магазин × канал (ID_ORGANIZATION): чеков в шапке I_CHECKHEADER,
--                                   уникальных чеков, чеков с картой, ТО; уникальных чеков и строк в I_CHECK.
--   dbo.I_CHECK_COMPLETENESS_DAY    день × канал: факт, норма, отклонение, признаки проблемы (FLAG).
--   dbo.I_CHECK_COMPLETENESS_SHOP   магазины, выпавшие из загрузки: в этот день ни одного чека, а накануне
--                                   и на следующий день чеки есть (для последнего дня — накануне и неделю назад).
--   dbo.I_CHECK_COMPLETENESS_ORG    день × канал: строк шапки и уникальных номеров чеков — для задвоений.
--   dbo.I_CHECK_COMPLETENESS_LOG    журнал ежедневных прогонов.
--
-- Как находится провал (порогов «на глаз» нет):
--   Норма дня = медиана того же дня недели за ±4 недели (8 соседних дат, какие есть). Отклонение дня —
--   логарифм факт/норма. Обычный разброс отклонений — по всей истории канала (медиана и MAD).
--   Провал / всплеск — отклонение дальше @K робастных σ от обычного (модифицированный z-score,
--   Iglewicz–Hoaglin; @K = 3,5 по умолчанию). Так же по часам (норма — тот же час того же дня недели),
--   по чекам с картой, по строкам. Выпавшие магазины — само число за день против обычного разброса канала.
--   Задвоение номеров чеков (лишние строки шапки на один ID_CHECK в канале за день — в любом магазине и часе) и расхождение шапок
--   и строк (Σ по магазинам за день |уникальных чеков в шапке − уникальных чеков в строках|) — через норму, как чеки:
--   норма — медиана того же дня недели ±4 недели, отклонение ln((факт + 1) / (норма + 1)) против обычного разброса
--   канала за всю историю, признак при z > @K. Чек через полночь (шапка берёт последнюю дату, строки — свою) даёт
--   расхождение каждый день, это входит в норму. Если в истории канала всегда 0 — признак на любое расхождение / задвоение.
--   z ограничен ±99999 (колонки Z_* — decimal(9,2)).
--   Оценивается история по вчерашний день: неполный сегодняшний день в оценку не попадает.
--   Канал 2 (тестовые данные) не оценивается — как в диагностике портала.
--
-- Порядок: весь файл — одним запуском (F5), вечером, когда задания MCI не идут. Он сам: создаёт таблицы и процедуры
-- (1–2), заполняет историю (3, по месяцу, печатает время; долго — часы), оценивает её и показывает проблемные дни (4),
-- создаёт задание (5), заменяет старую сверку dbo.check_data (6, с выводом слепка), включает задание (7).
-- Если задание MCI идёт, стартовало или упало после удаления дней — 6 и 7 ничего не меняют; повторный запуск продолжает
-- историю (с 1-го числа месяца последнего посчитанного дня, не позже 1-го числа четыре месяца назад).
--   0.   Скрипт рассчитан на установку с нуля. Если раньше ставилась другая версия этого скрипта — сначала откат
--        (раздел 8, первый блок — объекты проверки), затем установка целиком: таблицы прежних версий не дописываются.
--   5.   Задание SQL Agent «CVM check completeness»: каждый час 07:00–22:00 ищет в истории заданий за 3 дня
--        последний успешный шаг «INSERT I_CHECK_DELTA» задания MCI Regular update; если он позже последней проверенной
--        загрузки — один раз пересчитывает дни всех загрузок после проверенной (service.loading_log), всю оценку и
--        шлёт письмо (по расписанию и сразу после шага 12: его запускает шаг 13 — новая dbo.check_data). В письме — проблемные дни
--        (день×канал) среди перезалитых загрузками дней по service.loading_log (сколько дней перезатёрто, те и смотрим;
--        ночью — 10 дней, в месячную — ~3 месяца), в пределах пересчитанных этим прогоном; перезалитые, но не
--        пересчитанные дни (раньше нижней границы пересчёта — 1-е число четыре месяца назад — или позже вчерашнего
--        дня) — пометкой; нет записи в loading_log — пересчитываются последние 10 дней (@NIGHT_WINDOW_DAYS) по вчера,
--        письмо — среди пересчитанных, с такой подписью.
--        Нет чеков за вчера ни в одном канале — первым в теме. Остальная история — числом, разбор — страница
--        «Сверка данных» и раздел 4. Праздники (I_CALENDAR.DAYTYPE = 3,
--        выходной = 2) остаются в таблице, в письмо — только числом. Если к 20:00 загрузки нет —
--        письмо «сегодня чеки не загружены». При ошибке или прерванном прогоне та же загрузка повторяется каждый час,
--        не больше @MAX_TRIES раз (письмо об ошибке одно), дальше — ручной запуск с @FORCE = 1; к 20:00 по такой
--        загрузке, если она сегодняшняя, — письмо «проверка полноты не выполнена», если более ранняя — пометка о
--        неудачных прогонах в письме «сегодня чеки не загружены». Успешный ручной прогон закрывает загрузку, задание её
--        не повторяет. Ручные прогоны (@FORCE = 1, IS_MANUAL = 1 в журнале) попытки задания не расходуют и повторное
--        письмо об ошибке задания не подавляют.
--        Первый прогон (в журнале нет OK / MANUAL) берёт только сегодняшнюю загрузку: вчерашняя и раньше — «новой
--        загрузки нет». Если история кончается раньше вчерашнего дня, он пересчитывает подряд по вчера от более раннего
--        из двух дней — начала дельты сегодняшней загрузки (service.loading_log за сутки до окончания шага, не раньше
--        нижней границы; нет записи — сегодня минус 10 дней) или следующего за последним днём истории (без канала 2, без
--        нижней границы); вся дельта вне границ (окно дельты пустое) — от следующего за последним днём истории. История
--        не кончается раньше вчерашнего дня — окно дельты как в обычном прогоне (нет записи — последние 10 дней).
--        Если после последней загрузки запускался шаг 1 «CLEAR DATA» задания MCI Regular update (при любом исходе)
--        и успешного шага 12 после него ещё не было, или у нового запуска ещё идёт первый шаг, прогон
--        ничего не считает и в журнал не пишется — это не попытка. К 20:00 без сегодняшней загрузки — письмо «сегодня
--        чеки не загружены» с пометкой о новом проходе; если сегодняшняя загрузка так и не проверена — «проверка
--        полноты не выполнена»; по уже проверенной сегодняшней загрузке письма нет. Ручной прогон в это время —
--        ошибка 50006. Перезапуск MCI с шага после 12 дни не удаляет, но проверка ждёт, пока идёт его первый шаг
--        (по истории заданий не видно, с какого шага он начат).
--        Сбой почты не повторяет пересчёт: статус OK / NO_LOAD пишется до письма, после письма — MAIL_SENT = 1.
--        MAIL_SENT = 1 значит «письмо поставлено в очередь Database Mail» (sp_send_dbmail вернул 0); доставку видно
--        в msdb.dbo.sysmail_allitems. Ненулевой код возврата — ошибка 50005 в задании.
--        Создаётся выключенным, включает раздел 7 после заполненной и оценённой истории. Повторный запуск файла обновляет
--        команду шага.
--   6.   Старая сверка dbo.check_data (шаг 13 MCI Regular update) заменяется запуском задания проверки — только если
--        история заполнена и оценена в этом же запуске; перед заменой sp_helptext выводит слепок текущего текста.
--   7.   Включение задания — при тех же условиях и если check_data уже заменена.
--   8.   Откат (закомментирован, два блока): 8 — объекты проверки, 8.1 — старый текст dbo.check_data.
-- ============================================================================================

USE mci_model
GO

SET NOCOUNT ON;
-- флаги «история заполнена / оценена в этом запуске» — сброс в начале файла (разделы 6 и 7 работают только при 1)
EXEC sp_set_session_context @key = N'cc_history_ok', @value = 0;
EXEC sp_set_session_context @key = N'cc_history_filled', @value = 0;

-- ============================== 1. ТАБЛИЦЫ ==============================
IF OBJECT_ID('dbo.I_CHECK_COMPLETENESS') IS NULL
BEGIN
    CREATE TABLE dbo.I_CHECK_COMPLETENESS (
          ID_COMPANY        int            NOT NULL
        , DATA              date           NOT NULL
        , CLOCK             smallint       NOT NULL   -- час чека; -1 = не указан
        , ID_SHOP           int            NOT NULL   -- -1 = не указан
        , ID_ORGANIZATION   int            NOT NULL   -- канал: 1 офлайн, 3 e-com, 2 тест
        , CHECKS            int            NOT NULL   -- строк шапки I_CHECKHEADER
        , CHECKS_UNIQ       int            NOT NULL   -- уникальных ID_CHECK в шапке
        , CHECKS_IDENT      int            NOT NULL   -- из них с картой (ID_CONTACT <> 0)
        , TURNOVER          decimal(18,2)  NOT NULL   -- COST_DISCOUNT шапки
        , LINE_CHECKS       int            NOT NULL   -- уникальных ID_CHECK в строках I_CHECK
        , LINES             int            NOT NULL   -- строк I_CHECK
        , CALC_DT           datetime       NOT NULL
    );
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_CHECK_COMPLETENESS ON dbo.I_CHECK_COMPLETENESS;
END
GO

IF OBJECT_ID('dbo.I_CHECK_COMPLETENESS_DAY') IS NULL
BEGIN
    CREATE TABLE dbo.I_CHECK_COMPLETENESS_DAY (
          ID_COMPANY        int            NOT NULL
        , ID_ORGANIZATION   int            NOT NULL
        , ORGANIZATION      nvarchar(150)  NOT NULL   -- как I_ORGANIZATION.ORGANIZATION
        , DATA              date           NOT NULL
        , DOW               nvarchar(20)   NULL
        , IN_NIGHT_WINDOW   bit            NOT NULL   -- день в окне ночной перезаливки (последние N дней)
        , CHECKS            bigint         NOT NULL
        , BASE_CHECKS       decimal(18,1)  NULL       -- норма: медиана того же дня недели ±4 недели
        , DEV_CHECKS        decimal(18,4)  NULL       -- факт / норма - 1 (18,4: при норме меньше 1 и десятках тысяч чеков (9,4) переполняется)
        , Z_CHECKS          decimal(9,2)   NULL
        , CHECKS_IDENT      bigint         NOT NULL
        , BASE_IDENT        decimal(18,1)  NULL
        , DEV_IDENT         decimal(18,4)  NULL
        , Z_IDENT           decimal(9,2)   NULL
        , LINES             bigint         NOT NULL
        , BASE_LINES        decimal(18,1)  NULL
        , DEV_LINES         decimal(18,4)  NULL
        , Z_LINES           decimal(9,2)   NULL
        , SHOPS             int            NOT NULL   -- магазинов с чеками
        , SHOPS_MISSING     int            NOT NULL   -- выпавших магазинов (см. I_CHECK_COMPLETENESS_SHOP)
        , Z_SHOPS_MISSING   decimal(9,2)   NULL
        , HOURS_LOW         nvarchar(100)  NULL       -- часы с провалом, через запятую
        , DUP_CHECKS        bigint         NOT NULL   -- лишних строк шапки на один ID_CHECK в канале за день (любой магазин и час)
        , Z_DUP             decimal(9,2)   NULL
        , MISMATCH_CHECKS   bigint         NOT NULL   -- Σ по магазинам |чеков в шапке − чеков в строках|
        , Z_MISMATCH        decimal(9,2)   NULL
        , FLAG              nvarchar(1000) NOT NULL   -- признаки проблемы через «; », пусто — норма
        , IS_PROBLEM        bit            NOT NULL
        , CALC_DT           datetime       NOT NULL
        , IS_HOLIDAY        bit            NOT NULL CONSTRAINT DF_I_CHECK_COMPLETENESS_DAY_HOLIDAY DEFAULT (0)  -- праздник по I_CALENDAR.DAYTYPE: в письме только числом
    );
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_CHECK_COMPLETENESS_DAY ON dbo.I_CHECK_COMPLETENESS_DAY;
END
GO

-- день × канал: строк шапки и уникальных номеров чеков за день — задвоение номера чека в канале за день
-- в любом магазине и часе (строк шапки − уникальных ID_CHECK)
IF OBJECT_ID('dbo.I_CHECK_COMPLETENESS_ORG') IS NULL
BEGIN
    CREATE TABLE dbo.I_CHECK_COMPLETENESS_ORG (
          ID_COMPANY        int       NOT NULL
        , DATA              date      NOT NULL
        , ID_ORGANIZATION   int       NOT NULL
        , HDR_ROWS          bigint    NOT NULL   -- строк шапки I_CHECKHEADER
        , HDR_CHECKS        bigint    NOT NULL   -- уникальных ID_CHECK в канале за день
        , CALC_DT           datetime  NOT NULL
    );
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_CHECK_COMPLETENESS_ORG ON dbo.I_CHECK_COMPLETENESS_ORG;
END
GO

IF OBJECT_ID('dbo.I_CHECK_COMPLETENESS_SHOP') IS NULL
BEGIN
    CREATE TABLE dbo.I_CHECK_COMPLETENESS_SHOP (
          ID_COMPANY        int       NOT NULL
        , ID_ORGANIZATION   int       NOT NULL
        , DATA              date      NOT NULL   -- день без чеков
        , ID_SHOP           int       NOT NULL
        , CHECKS_PREV_DAY   int       NOT NULL   -- чеков накануне
        , CONTROL_DATA      date      NOT NULL   -- следующий день (для последнего дня — неделя назад)
        , CHECKS_CONTROL    int       NOT NULL   -- чеков в контрольный день
        , CALC_DT           datetime  NOT NULL
    );
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_CHECK_COMPLETENESS_SHOP ON dbo.I_CHECK_COMPLETENESS_SHOP;
END
GO

IF OBJECT_ID('dbo.I_CHECK_COMPLETENESS_LOG') IS NULL
BEGIN
    CREATE TABLE dbo.I_CHECK_COMPLETENESS_LOG (
          RUN_ID        int IDENTITY(1,1) NOT NULL CONSTRAINT PK_I_CHECK_COMPLETENESS_LOG PRIMARY KEY
        , STARTED       datetime        NOT NULL
        , FINISHED      datetime        NULL
        , LOAD_DT       datetime        NULL     -- окончание шага «INSERT I_CHECK_DELTA», на который отвечает прогон
        , D_FROM        date            NULL     -- пересчитанные дни; D_FROM > D_TO — пересчёта не было
        , D_TO          date            NULL
        , REL_FROM      date            NULL     -- перезалитые загрузкой дни (service.loading_log); NULL — записи нет
        , REL_TO        date            NULL
        , DAYS_FLAGGED  int             NULL     -- проблемных день×канал без праздников в окне письма: REL_FROM..REL_TO ∩ D_FROM..D_TO;
                                                 -- нет записи в loading_log — D_FROM..D_TO; пустое окно — 0
        , MAIL_SENT     bit             NOT NULL CONSTRAINT DF_I_CHECK_COMPLETENESS_LOG_MAIL DEFAULT (0)   -- 1 — письмо в очереди Database Mail (msdb.dbo.sysmail_allitems)
        , STATUS        varchar(10)     NOT NULL -- RUN / OK / MANUAL / NO_LOAD / ERROR
        , MSG           nvarchar(4000)  NULL
        , IS_MANUAL     bit             NOT NULL CONSTRAINT DF_I_CHECK_COMPLETENESS_LOG_MANUAL DEFAULT (0) -- 1 — ручной прогон (@FORCE = 1), не попытка задания
    );
END
GO

-- ============================== 2. ПРОЦЕДУРЫ ==============================

-- 2.1 Пересчёт дней @D_FROM..@D_TO: день за днём, каждый день удаляется и считается заново.
CREATE OR ALTER PROCEDURE dbo.check_completeness_fill
      @ID_COMPANY  int  = 1
    , @D_FROM      date
    , @D_TO        date
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @D_FROM IS NULL OR @D_TO IS NULL OR @D_FROM > @D_TO
        THROW 50001, N'check_completeness_fill: нужно @D_FROM <= @D_TO', 1;

    DECLARE @d date = @D_FROM, @rc int;

    -- Чтение I_CHECKHEADER / I_CHECK — WITH (NOLOCK): разделяемые блокировки на таблицах в миллиарды строк задержали бы
    -- шаги MCI, которые их пишут (1 clear_data_update, 4 check_product_update, 12 check_insert задания MCI Regular update;
    -- шаг 6 reload_clear_data выключенного задания MCI Reload).
    -- Полуудалённый день защищают вызывающие, не эта процедура: check_completeness_daily не считает, пока после загрузки
    -- шаг 1 MCI Regular update удалил дни, а шаг 12 их ещё не вставил (признак @busy); раздел 3 проверяет MCI процедурой
    -- check_completeness_mci_busy в начале, перед каждым месяцем и в конце. Ручной EXEC — только когда задания MCI не идут.
    WHILE @d <= @D_TO
    BEGIN
        DROP TABLE IF EXISTS #cc_hc, #cc_hdr, #cc_org, #cc_lin;

        -- шапки: один проход по I_CHECKHEADER за день до чека × магазин × час (миллионы строк — колоночный индекс)
        SELECT    h.ID_ORGANIZATION
                , ID_SHOP  = ISNULL(h.ID_SHOP, -1)
                , CLOCK    = ISNULL(h.CLOCK, -1)
                , h.ID_CHECK
                , N        = COUNT(*)
                , IDENT    = MAX(CASE WHEN h.ID_CONTACT <> 0 THEN 1 ELSE 0 END)
                , TURNOVER = SUM(h.COST_DISCOUNT)
        INTO #cc_hc
        FROM dbo.I_CHECKHEADER AS h WITH (NOLOCK)
        WHERE h.ID_COMPANY = @ID_COMPANY
          AND h.DATA = @d
        GROUP BY h.ID_ORGANIZATION, ISNULL(h.ID_SHOP, -1), ISNULL(h.CLOCK, -1), h.ID_CHECK;

        CREATE CLUSTERED COLUMNSTORE INDEX CCI_cc_hc ON #cc_hc;

        -- до час × магазин × канал
        SELECT    ID_ORGANIZATION, ID_SHOP, CLOCK
                , CHECKS       = SUM(N)
                , CHECKS_UNIQ  = COUNT(*)
                , CHECKS_IDENT = SUM(IDENT)
                , TURNOVER     = SUM(TURNOVER)
        INTO #cc_hdr
        FROM #cc_hc
        GROUP BY ID_ORGANIZATION, ID_SHOP, CLOCK;

        -- до канала за день: номер чека, повторённый в любом магазине и часе, — задвоение
        SELECT    ID_ORGANIZATION
                , HDR_ROWS   = SUM(CAST(N AS bigint))
                , HDR_CHECKS = COUNT_BIG(*)
        INTO #cc_org
        FROM (SELECT ID_ORGANIZATION, ID_CHECK, N = SUM(N) FROM #cc_hc GROUP BY ID_ORGANIZATION, ID_CHECK) AS x
        GROUP BY ID_ORGANIZATION;

        -- строки: так же через чек, без COUNT(DISTINCT). Час чека — как у шапки в check_insert: час последней
        -- строки (MAX), иначе чек со строками в разные часы попал бы в несколько часов и дал ложное «шапки ≠ строки»
        SELECT    x.ID_ORGANIZATION, x.ID_SHOP, x.CLOCK
                , LINE_CHECKS = COUNT(*)
                , LINES       = SUM(x.N)
        INTO #cc_lin
        FROM (
            SELECT    c.ID_ORGANIZATION
                    , ID_SHOP = ISNULL(c.ID_SHOP, -1)
                    , CLOCK   = MAX(ISNULL(c.CLOCK, -1))
                    , c.ID_CHECK
                    , N       = COUNT(*)
            FROM dbo.I_CHECK AS c WITH (NOLOCK)
            WHERE c.ID_COMPANY = @ID_COMPANY
              AND c.DATA = @d
            GROUP BY c.ID_ORGANIZATION, ISNULL(c.ID_SHOP, -1), c.ID_CHECK
        ) AS x
        GROUP BY x.ID_ORGANIZATION, x.ID_SHOP, x.CLOCK;

        BEGIN TRAN;

        -- два прогона одновременно (задание и ручной) идут по дню друг за другом, а не задваивают день
        EXEC @rc = sp_getapplock @Resource = N'I_CHECK_COMPLETENESS', @LockMode = N'Exclusive',
                                 @LockOwner = N'Transaction', @LockTimeout = -1;
        IF @rc < 0
            THROW 50003, N'check_completeness_fill: не получена блокировка I_CHECK_COMPLETENESS', 1;

        DELETE FROM dbo.I_CHECK_COMPLETENESS
        WHERE ID_COMPANY = @ID_COMPANY
          AND DATA = @d;

        INSERT INTO dbo.I_CHECK_COMPLETENESS
            (ID_COMPANY, DATA, CLOCK, ID_SHOP, ID_ORGANIZATION, CHECKS, CHECKS_UNIQ, CHECKS_IDENT, TURNOVER,
             LINE_CHECKS, LINES, CALC_DT)
        SELECT    @ID_COMPANY
                , @d
                , COALESCE(h.CLOCK, l.CLOCK)
                , COALESCE(h.ID_SHOP, l.ID_SHOP)
                , COALESCE(h.ID_ORGANIZATION, l.ID_ORGANIZATION)
                , ISNULL(h.CHECKS, 0)
                , ISNULL(h.CHECKS_UNIQ, 0)
                , ISNULL(h.CHECKS_IDENT, 0)
                , CAST(ISNULL(h.TURNOVER, 0) AS decimal(18,2))
                , ISNULL(l.LINE_CHECKS, 0)
                , ISNULL(l.LINES, 0)
                , GETDATE()
        FROM #cc_hdr AS h
        FULL JOIN #cc_lin AS l
               ON l.ID_ORGANIZATION = h.ID_ORGANIZATION
              AND l.ID_SHOP         = h.ID_SHOP
              AND l.CLOCK           = h.CLOCK;

        DELETE FROM dbo.I_CHECK_COMPLETENESS_ORG
        WHERE ID_COMPANY = @ID_COMPANY
          AND DATA = @d;

        INSERT INTO dbo.I_CHECK_COMPLETENESS_ORG (ID_COMPANY, DATA, ID_ORGANIZATION, HDR_ROWS, HDR_CHECKS, CALC_DT)
        SELECT @ID_COMPANY, @d, ID_ORGANIZATION, HDR_ROWS, HDR_CHECKS, GETDATE()
        FROM #cc_org;

        COMMIT;

        SET @d = DATEADD(day, 1, @d);
    END

    -- дневные вставки маленькие и оседают в дельта-группах — сжать
    ALTER INDEX CCI_I_CHECK_COMPLETENESS ON dbo.I_CHECK_COMPLETENESS
        REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);
    ALTER INDEX CCI_I_CHECK_COMPLETENESS_ORG ON dbo.I_CHECK_COMPLETENESS_ORG
        REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);
END
GO

-- 2.2 Оценка всей истории: дни × каналы, часы, выпавшие магазины → I_CHECK_COMPLETENESS_DAY / _SHOP.
--     Читает I_CHECK_COMPLETENESS (десятки миллионов строк), I_CHECK_COMPLETENESS_ORG и справочники I_CALENDAR,
--     I_ORGANIZATION — не чеки.
CREATE OR ALTER PROCEDURE dbo.check_completeness_eval
      @ID_COMPANY         int           = 1
    , @K                  decimal(5,2)  = 3.5   -- граница модифицированного z-score
    , @NIGHT_WINDOW_DAYS  int           = 10    -- окно ночной перезаливки (задание MCI Reload every day: getdate()-10)
    , @TEST_ORG           int           = 2     -- канал тестовых данных, не оценивается
    , @HOLIDAY_DAYTYPE    int           = 3     -- код праздника в I_CALENDAR.DAYTYPE (1 рабочий, 2 выходной, 3 праздник); в письме праздники только числом
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @today     date = CAST(GETDATE() AS date);
    DECLARE @yesterday date = DATEADD(day, -1, @today);
    DECLARE @night_from date = DATEADD(day, -@NIGHT_WINDOW_DAYS, @today);
    DECLARE @max_date  date, @rc int;

    -- чтение своих таблиц — WITH (NOLOCK); от дня, который параллельный fill сейчас переписывает, защищает разделяемая
    -- блокировка приложения на транзакцию (fill держит её же монопольно на время DELETE+INSERT дня; при ошибке или
    -- остановке транзакция откатывается — XACT_ABORT — и блокировка снимается)
    BEGIN TRAN;
    EXEC @rc = sp_getapplock @Resource = N'I_CHECK_COMPLETENESS', @LockMode = N'Shared', @LockOwner = N'Transaction', @LockTimeout = -1;
    IF @rc < 0
        THROW 50003, N'check_completeness_eval: не получена блокировка I_CHECK_COMPLETENESS', 1;

    -- последний день — по оцениваемым каналам: тестовый канал не сдвигает календарь остальных.
    -- Только по вчера: неполный сегодняшний день дал бы ложный провал и не годится в норму соседних дней
    SELECT @max_date = MAX(DATA)
    FROM dbo.I_CHECK_COMPLETENESS WITH (NOLOCK)
    WHERE ID_COMPANY = @ID_COMPANY
      AND ID_ORGANIZATION <> @TEST_ORG
      AND DATA <= @yesterday;

    IF @max_date IS NULL
    BEGIN
        COMMIT;
        RETURN;
    END

    -- магазин × день
    DROP TABLE IF EXISTS #cc_sd;
    SELECT    ID_ORGANIZATION, DATA, ID_SHOP
            , CHECKS       = SUM(CHECKS)
            , CHECKS_UNIQ  = SUM(CHECKS_UNIQ)
            , CHECKS_IDENT = SUM(CHECKS_IDENT)
            , LINE_CHECKS  = SUM(LINE_CHECKS)
            , LINES        = SUM(LINES)
    INTO #cc_sd
    FROM dbo.I_CHECK_COMPLETENESS WITH (NOLOCK)
    WHERE ID_COMPANY = @ID_COMPANY
      AND ID_ORGANIZATION <> @TEST_ORG
      AND DATA <= @max_date
    GROUP BY ID_ORGANIZATION, DATA, ID_SHOP;

    CREATE CLUSTERED COLUMNSTORE INDEX CCI_cc_sd ON #cc_sd;

    -- канал × день × час
    DROP TABLE IF EXISTS #cc_hd;
    SELECT ID_ORGANIZATION, DATA, CLOCK, CHECKS = SUM(CAST(CHECKS AS bigint))
    INTO #cc_hd
    FROM dbo.I_CHECK_COMPLETENESS WITH (NOLOCK)
    WHERE ID_COMPANY = @ID_COMPANY
      AND ID_ORGANIZATION <> @TEST_ORG
      AND DATA <= @max_date
      AND CLOCK >= 0
    GROUP BY ID_ORGANIZATION, DATA, CLOCK;

    -- выпавшие магазины: в день D чеков нет, а накануне и на следующий день есть
    DROP TABLE IF EXISTS #cc_miss;
    SELECT    p.ID_ORGANIZATION
            , DATA            = DATEADD(day, 1, p.DATA)
            , p.ID_SHOP
            , CHECKS_PREV_DAY = p.CHECKS
            , CONTROL_DATA    = n.DATA
            , CHECKS_CONTROL  = n.CHECKS
    INTO #cc_miss
    FROM #cc_sd AS p
    JOIN #cc_sd AS n
      ON n.ID_ORGANIZATION = p.ID_ORGANIZATION
     AND n.ID_SHOP         = p.ID_SHOP
     AND n.DATA            = DATEADD(day, 2, p.DATA)
     AND n.CHECKS          > 0
    WHERE p.CHECKS > 0
      AND p.ID_SHOP <> -1
      AND NOT EXISTS (SELECT 1 FROM #cc_sd AS x
                      WHERE x.ID_ORGANIZATION = p.ID_ORGANIZATION
                        AND x.ID_SHOP         = p.ID_SHOP
                        AND x.DATA            = DATEADD(day, 1, p.DATA)
                        AND x.CHECKS          > 0)
    UNION ALL
    -- последний день: следующего дня ещё нет — накануне и неделю назад
    SELECT    p.ID_ORGANIZATION
            , @max_date
            , p.ID_SHOP
            , p.CHECKS
            , w.DATA
            , w.CHECKS
    FROM #cc_sd AS p
    JOIN #cc_sd AS w
      ON w.ID_ORGANIZATION = p.ID_ORGANIZATION
     AND w.ID_SHOP         = p.ID_SHOP
     AND w.DATA            = DATEADD(day, -7, @max_date)
     AND w.CHECKS          > 0
    WHERE p.DATA = DATEADD(day, -1, @max_date)
      AND p.CHECKS > 0
      AND p.ID_SHOP <> -1
      AND NOT EXISTS (SELECT 1 FROM #cc_sd AS x
                      WHERE x.ID_ORGANIZATION = p.ID_ORGANIZATION
                        AND x.ID_SHOP         = p.ID_SHOP
                        AND x.DATA            = @max_date
                        AND x.CHECKS          > 0);

    -- канал × день по календарю (дни без единого чека — нулями)
    DROP TABLE IF EXISTS #cc_day;
    SELECT    o.ID_ORGANIZATION
            , DATA            = c.[DATE]
            , DOW             = c.DAYOFWEEK_NAME
            , IS_HOLIDAY      = CAST(CASE WHEN c.DAYTYPE = @HOLIDAY_DAYTYPE THEN 1 ELSE 0 END AS bit)
            , CHECKS          = ISNULL(s.CHECKS, 0)
            , CHECKS_IDENT    = ISNULL(s.CHECKS_IDENT, 0)
            , LINES           = ISNULL(s.LINES, 0)
            , SHOPS           = ISNULL(s.SHOPS, 0)
            , DUP_CHECKS      = ISNULL(g.HDR_ROWS - g.HDR_CHECKS, 0)
            , MISMATCH_CHECKS = ISNULL(s.MISMATCH_CHECKS, 0)
            , SHOPS_MISSING   = ISNULL(m.N, 0)
    INTO #cc_day
    FROM (SELECT ID_ORGANIZATION, D_MIN = MIN(DATA) FROM #cc_sd GROUP BY ID_ORGANIZATION) AS o
    JOIN dbo.I_CALENDAR AS c WITH (NOLOCK)
      ON c.[DATE] BETWEEN o.D_MIN AND @max_date
    LEFT JOIN (
        SELECT    ID_ORGANIZATION, DATA
                , CHECKS          = SUM(CAST(CHECKS AS bigint))
                , CHECKS_IDENT    = SUM(CAST(CHECKS_IDENT AS bigint))
                , LINES           = SUM(CAST(LINES AS bigint))
                , SHOPS           = SUM(CASE WHEN CHECKS > 0 AND ID_SHOP <> -1 THEN 1 ELSE 0 END)
                , MISMATCH_CHECKS = SUM(CAST(ABS(CHECKS_UNIQ - LINE_CHECKS) AS bigint))
        FROM #cc_sd
        GROUP BY ID_ORGANIZATION, DATA
    ) AS s
      ON s.ID_ORGANIZATION = o.ID_ORGANIZATION
     AND s.DATA            = c.[DATE]
    LEFT JOIN (SELECT ID_ORGANIZATION, DATA, N = COUNT(*) FROM #cc_miss GROUP BY ID_ORGANIZATION, DATA) AS m
      ON m.ID_ORGANIZATION = o.ID_ORGANIZATION
     AND m.DATA            = c.[DATE]
    -- задвоение номера чека в канале за день — в любом магазине и часе
    LEFT JOIN dbo.I_CHECK_COMPLETENESS_ORG AS g WITH (NOLOCK)
      ON g.ID_COMPANY      = @ID_COMPANY
     AND g.ID_ORGANIZATION = o.ID_ORGANIZATION
     AND g.DATA            = c.[DATE];

    COMMIT;   -- снимает разделяемую блокировку I_CHECK_COMPLETENESS

    -- канал × день × час по календарю: часы, в которые канал хоть раз торговал
    DROP TABLE IF EXISTS #cc_hs;
    SELECT d.ID_ORGANIZATION, d.DATA, h.CLOCK, CHECKS = ISNULL(x.CHECKS, 0)
    INTO #cc_hs
    FROM #cc_day AS d
    JOIN (SELECT DISTINCT ID_ORGANIZATION, CLOCK FROM #cc_hd) AS h
      ON h.ID_ORGANIZATION = d.ID_ORGANIZATION
    LEFT JOIN #cc_hd AS x
      ON x.ID_ORGANIZATION = d.ID_ORGANIZATION
     AND x.DATA            = d.DATA
     AND x.CLOCK           = h.CLOCK;

    -- соседние даты для нормы: тот же день недели ±4 недели
    DROP TABLE IF EXISTS #cc_off;
    CREATE TABLE #cc_off (K int NOT NULL PRIMARY KEY);
    INSERT INTO #cc_off (K) VALUES (-28), (-21), (-14), (-7), (7), (14), (21), (28);

    DROP TABLE IF EXISTS #cc_base;
    SELECT DISTINCT
              a.ID_ORGANIZATION, a.DATA
            , BASE_CHECKS = PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.CHECKS)       OVER (PARTITION BY a.ID_ORGANIZATION, a.DATA)
            , BASE_IDENT  = PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.CHECKS_IDENT) OVER (PARTITION BY a.ID_ORGANIZATION, a.DATA)
            , BASE_LINES  = PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.LINES)        OVER (PARTITION BY a.ID_ORGANIZATION, a.DATA)
            , BASE_MISMATCH = PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.MISMATCH_CHECKS) OVER (PARTITION BY a.ID_ORGANIZATION, a.DATA)
            , BASE_DUP      = PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.DUP_CHECKS)      OVER (PARTITION BY a.ID_ORGANIZATION, a.DATA)
    INTO #cc_base
    FROM #cc_day AS a
    CROSS JOIN #cc_off AS o
    JOIN #cc_day AS b
      ON b.ID_ORGANIZATION = a.ID_ORGANIZATION
     AND b.DATA            = DATEADD(day, o.K, a.DATA);

    DROP TABLE IF EXISTS #cc_hbase;
    SELECT DISTINCT
              a.ID_ORGANIZATION, a.DATA, a.CLOCK, a.CHECKS
            , BASE = PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.CHECKS) OVER (PARTITION BY a.ID_ORGANIZATION, a.DATA, a.CLOCK)
    INTO #cc_hbase
    FROM #cc_hs AS a
    CROSS JOIN #cc_off AS o
    JOIN #cc_hs AS b
      ON b.ID_ORGANIZATION = a.ID_ORGANIZATION
     AND b.CLOCK           = a.CLOCK
     AND b.DATA            = DATEADD(day, o.K, a.DATA);

    -- все показатели в длинном виде: METRIC 1 чеки, 2 чеки с картой, 3 строки, 4 выпавшие магазины,
    -- 5 расхождение шапок и строк (MISMATCH_CHECKS), 6 задвоение номеров чеков (DUP_CHECKS), 100+час — чеки часа.
    -- Отклонение = ln((факт + 1) / (норма + 1)), норма — медиана того же дня недели ±4 недели (#cc_base);
    -- у выпавших магазинов — само число.
    DROP TABLE IF EXISTS #cc_lv;
    SELECT d.ID_ORGANIZATION, d.DATA, METRIC = CAST(1 AS smallint), V = LOG((d.CHECKS + 1.0) / (b.BASE_CHECKS + 1.0))
    INTO #cc_lv
    FROM #cc_day AS d JOIN #cc_base AS b ON b.ID_ORGANIZATION = d.ID_ORGANIZATION AND b.DATA = d.DATA
    UNION ALL
    SELECT d.ID_ORGANIZATION, d.DATA, 2, LOG((d.CHECKS_IDENT + 1.0) / (b.BASE_IDENT + 1.0))
    FROM #cc_day AS d JOIN #cc_base AS b ON b.ID_ORGANIZATION = d.ID_ORGANIZATION AND b.DATA = d.DATA
    UNION ALL
    SELECT d.ID_ORGANIZATION, d.DATA, 3, LOG((d.LINES + 1.0) / (b.BASE_LINES + 1.0))
    FROM #cc_day AS d JOIN #cc_base AS b ON b.ID_ORGANIZATION = d.ID_ORGANIZATION AND b.DATA = d.DATA
    UNION ALL
    SELECT d.ID_ORGANIZATION, d.DATA, 4, CAST(d.SHOPS_MISSING AS float)
    FROM #cc_day AS d
    UNION ALL
    SELECT d.ID_ORGANIZATION, d.DATA, 5, LOG((d.MISMATCH_CHECKS + 1.0) / (b.BASE_MISMATCH + 1.0))
    FROM #cc_day AS d JOIN #cc_base AS b ON b.ID_ORGANIZATION = d.ID_ORGANIZATION AND b.DATA = d.DATA
    UNION ALL
    SELECT d.ID_ORGANIZATION, d.DATA, 6, LOG((d.DUP_CHECKS + 1.0) / (b.BASE_DUP + 1.0))
    FROM #cc_day AS d JOIN #cc_base AS b ON b.ID_ORGANIZATION = d.ID_ORGANIZATION AND b.DATA = d.DATA
    UNION ALL
    SELECT h.ID_ORGANIZATION, h.DATA, CAST(100 + h.CLOCK AS smallint), LOG((h.CHECKS + 1.0) / (h.BASE + 1.0))
    FROM #cc_hbase AS h;

    -- обычный разброс по каждому каналу и показателю за всю историю: медиана, MAD, среднее абсолютное отклонение
    DROP TABLE IF EXISTS #cc_med;
    SELECT DISTINCT ID_ORGANIZATION, METRIC
            , M = PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY V) OVER (PARTITION BY ID_ORGANIZATION, METRIC)
    INTO #cc_med
    FROM #cc_lv;

    DROP TABLE IF EXISTS #cc_spread;
    SELECT DISTINCT l.ID_ORGANIZATION, l.METRIC, m.M
            , MAD    = PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ABS(l.V - m.M)) OVER (PARTITION BY l.ID_ORGANIZATION, l.METRIC)
            , MEANAD = AVG(ABS(l.V - m.M)) OVER (PARTITION BY l.ID_ORGANIZATION, l.METRIC)
    INTO #cc_spread
    FROM #cc_lv AS l
    JOIN #cc_med AS m ON m.ID_ORGANIZATION = l.ID_ORGANIZATION AND m.METRIC = l.METRIC;

    -- модифицированный z-score; MAD = 0 → через среднее абсолютное отклонение; оба 0 → любое отличие от медианы.
    -- z зажат в ±99999: при крошечном MAD / MEANAD (массовое задвоение) CAST в decimal(9,2) переполнился бы
    DROP TABLE IF EXISTS #cc_z;
    SELECT    l.ID_ORGANIZATION, l.DATA, l.METRIC
            , Z = CASE WHEN r.Z >  99999.0 THEN  99999.0
                       WHEN r.Z < -99999.0 THEN -99999.0
                       ELSE r.Z END
    INTO #cc_z
    FROM #cc_lv AS l
    JOIN #cc_spread AS s ON s.ID_ORGANIZATION = l.ID_ORGANIZATION AND s.METRIC = l.METRIC
    CROSS APPLY (SELECT Z = CASE
                              WHEN s.MAD > 0    THEN (l.V - s.M) / (1.4826   * s.MAD)
                              WHEN s.MEANAD > 0 THEN (l.V - s.M) / (1.253314 * s.MEANAD)
                              WHEN l.V > s.M    THEN 99.0
                              WHEN l.V < s.M    THEN -99.0
                              ELSE 0.0
                            END) AS r;

    DROP TABLE IF EXISTS #cc_zd;
    SELECT    ID_ORGANIZATION, DATA
            , Z_CHECKS        = MAX(CASE WHEN METRIC = 1 THEN Z END)
            , Z_IDENT         = MAX(CASE WHEN METRIC = 2 THEN Z END)
            , Z_LINES         = MAX(CASE WHEN METRIC = 3 THEN Z END)
            , Z_SHOPS_MISSING = MAX(CASE WHEN METRIC = 4 THEN Z END)
            , Z_MISMATCH      = MAX(CASE WHEN METRIC = 5 THEN Z END)
            , Z_DUP           = MAX(CASE WHEN METRIC = 6 THEN Z END)
            , HOURS_LOW       = STRING_AGG(CASE WHEN METRIC >= 100 AND Z < -@K THEN CAST(METRIC - 100 AS varchar(2)) END, ',')
                                    WITHIN GROUP (ORDER BY METRIC)
    INTO #cc_zd
    FROM #cc_z
    GROUP BY ID_ORGANIZATION, DATA;

    -- итог день × канал
    DROP TABLE IF EXISTS #cc_res;
    SELECT    ID_COMPANY      = @ID_COMPANY
            , d.ID_ORGANIZATION
            , ORGANIZATION    = ISNULL(org.ORGANIZATION, CONCAT(N'канал ', d.ID_ORGANIZATION))
            , d.DATA
            , d.DOW
            , d.IS_HOLIDAY
            , IN_NIGHT_WINDOW = CAST(CASE WHEN d.DATA >= @night_from THEN 1 ELSE 0 END AS bit)
            , d.CHECKS
            , BASE_CHECKS     = CAST(b.BASE_CHECKS AS decimal(18,1))
            , DEV_CHECKS      = CAST(d.CHECKS       / NULLIF(b.BASE_CHECKS, 0) - 1 AS decimal(18,4))
            , Z_CHECKS        = CAST(z.Z_CHECKS AS decimal(9,2))
            , d.CHECKS_IDENT
            , BASE_IDENT      = CAST(b.BASE_IDENT AS decimal(18,1))
            , DEV_IDENT       = CAST(d.CHECKS_IDENT / NULLIF(b.BASE_IDENT, 0) - 1 AS decimal(18,4))
            , Z_IDENT         = CAST(z.Z_IDENT AS decimal(9,2))
            , d.LINES
            , BASE_LINES      = CAST(b.BASE_LINES AS decimal(18,1))
            , DEV_LINES       = CAST(d.LINES        / NULLIF(b.BASE_LINES, 0) - 1 AS decimal(18,4))
            , Z_LINES         = CAST(z.Z_LINES AS decimal(9,2))
            , d.SHOPS
            , d.SHOPS_MISSING
            , Z_SHOPS_MISSING = CAST(z.Z_SHOPS_MISSING AS decimal(9,2))
            , HOURS_LOW       = CAST(z.HOURS_LOW AS nvarchar(100))
            , d.DUP_CHECKS
            , Z_DUP           = CAST(z.Z_DUP AS decimal(9,2))
            , d.MISMATCH_CHECKS
            , Z_MISMATCH      = CAST(z.Z_MISMATCH AS decimal(9,2))
            , FLAG = CONCAT_WS(N'; '
                , CASE WHEN d.CHECKS = 0 AND b.BASE_CHECKS > 0      THEN N'нет чеков за день' END
                , CASE WHEN d.CHECKS > 0 AND z.Z_CHECKS < -@K       THEN N'провал чеков' END
                , CASE WHEN z.Z_CHECKS > @K                         THEN N'всплеск чеков' END
                , CASE WHEN d.CHECKS > 0 AND z.Z_IDENT < -@K        THEN N'провал чеков с картой' END
                , CASE WHEN d.CHECKS > 0 AND z.Z_LINES < -@K        THEN N'провал строк' END
                , CASE WHEN z.Z_LINES > @K                          THEN N'всплеск строк' END
                , CASE WHEN z.Z_SHOPS_MISSING > @K                  THEN CONCAT(N'выпали магазины: ', d.SHOPS_MISSING) END
                , CASE WHEN d.CHECKS > 0 AND z.HOURS_LOW IS NOT NULL THEN CONCAT(N'провал по часам: ', z.HOURS_LOW) END
                , CASE WHEN z.Z_DUP > @K                            THEN CONCAT(N'задвоение номеров чеков: ', d.DUP_CHECKS) END
                , CASE WHEN z.Z_MISMATCH > @K                       THEN CONCAT(N'шапки ≠ строки: ', d.MISMATCH_CHECKS) END
              )
    INTO #cc_res
    FROM #cc_day AS d
    LEFT JOIN #cc_base AS b ON b.ID_ORGANIZATION = d.ID_ORGANIZATION AND b.DATA = d.DATA
    LEFT JOIN #cc_zd   AS z ON z.ID_ORGANIZATION = d.ID_ORGANIZATION AND z.DATA = d.DATA
    -- одно название на канал: повтор строки в справочнике не должен задвоить день
    OUTER APPLY (SELECT TOP (1) o.ORGANIZATION
                 FROM dbo.I_ORGANIZATION AS o WITH (NOLOCK)
                 WHERE o.ID_ORGANIZATION = d.ID_ORGANIZATION
                   AND o.ID_COMPANY      = @ID_COMPANY
                 ORDER BY o.ORGANIZATION) AS org;

    BEGIN TRAN;

    -- одновременные оценки идут друг за другом, а не задваивают _DAY / _SHOP
    EXEC @rc = sp_getapplock @Resource = N'I_CHECK_COMPLETENESS_EVAL', @LockMode = N'Exclusive',
                             @LockOwner = N'Transaction', @LockTimeout = -1;
    IF @rc < 0
        THROW 50004, N'check_completeness_eval: не получена блокировка I_CHECK_COMPLETENESS_EVAL', 1;

    DELETE FROM dbo.I_CHECK_COMPLETENESS_DAY WHERE ID_COMPANY = @ID_COMPANY;

    INSERT INTO dbo.I_CHECK_COMPLETENESS_DAY WITH (TABLOCK)
        (ID_COMPANY, ID_ORGANIZATION, ORGANIZATION, DATA, DOW, IN_NIGHT_WINDOW,
         CHECKS, BASE_CHECKS, DEV_CHECKS, Z_CHECKS, CHECKS_IDENT, BASE_IDENT, DEV_IDENT, Z_IDENT,
         LINES, BASE_LINES, DEV_LINES, Z_LINES, SHOPS, SHOPS_MISSING, Z_SHOPS_MISSING, HOURS_LOW,
         DUP_CHECKS, Z_DUP, MISMATCH_CHECKS, Z_MISMATCH, FLAG, IS_PROBLEM, CALC_DT, IS_HOLIDAY)
    SELECT    ID_COMPANY, ID_ORGANIZATION, ORGANIZATION, DATA, DOW, IN_NIGHT_WINDOW,
              CHECKS, BASE_CHECKS, DEV_CHECKS, Z_CHECKS, CHECKS_IDENT, BASE_IDENT, DEV_IDENT, Z_IDENT,
              LINES, BASE_LINES, DEV_LINES, Z_LINES, SHOPS, SHOPS_MISSING, Z_SHOPS_MISSING, HOURS_LOW,
              DUP_CHECKS, Z_DUP, MISMATCH_CHECKS, Z_MISMATCH, FLAG
            , CAST(CASE WHEN FLAG <> N'' THEN 1 ELSE 0 END AS bit)
            , GETDATE()
            , IS_HOLIDAY
    FROM #cc_res;

    DELETE FROM dbo.I_CHECK_COMPLETENESS_SHOP WHERE ID_COMPANY = @ID_COMPANY;

    INSERT INTO dbo.I_CHECK_COMPLETENESS_SHOP WITH (TABLOCK)
        (ID_COMPANY, ID_ORGANIZATION, DATA, ID_SHOP, CHECKS_PREV_DAY, CONTROL_DATA, CHECKS_CONTROL, CALC_DT)
    SELECT @ID_COMPANY, ID_ORGANIZATION, DATA, ID_SHOP, CHECKS_PREV_DAY, CONTROL_DATA, CHECKS_CONTROL, GETDATE()
    FROM #cc_miss;

    COMMIT;
END
GO

-- 2.0 Признак «MCI занят» (только чтение истории заданий): идёт задание MCI, или задание MCI стартовало после @SINCE, или после шага «CLEAR DATA»
--     задания MCI Regular update не было успешного «INSERT I_CHECK_DELTA» (дни удалены и не вставлены — задание упало).
--     Используют раздел 3 (история): в начале, перед каждым месяцем и в конце.
CREATE OR ALTER PROCEDURE dbo.check_completeness_mci_busy
      @SINCE  datetime = NULL
    , @BUSY   bit            OUTPUT
    , @MSG    nvarchar(400)  OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @ok_dt datetime, @clr_dt datetime;
    SELECT @BUSY = 0, @MSG = NULL;

    IF EXISTS (SELECT 1
               FROM msdb.dbo.sysjobactivity AS a WITH (NOLOCK)
               JOIN msdb.dbo.sysjobs        AS j WITH (NOLOCK) ON j.job_id = a.job_id
               WHERE j.name IN (N'MCI Regular update', N'MCI Reload', N'MCI Reload every day', N'MCI Reload one time')
                 AND a.session_id = (SELECT MAX(s.session_id) FROM msdb.dbo.syssessions AS s WITH (NOLOCK))
                 AND a.start_execution_date IS NOT NULL
                 AND (a.stop_execution_date IS NULL OR a.start_execution_date >= ISNULL(@SINCE, '99991231')))
    BEGIN
        SELECT @BUSY = 1, @MSG = N'идёт задание MCI или оно стартовало во время заполнения истории';
        RETURN;
    END

    SELECT @ok_dt = MAX(DATEADD(second,
                            (h.run_time / 10000) * 3600 + (h.run_time / 100 % 100) * 60 + h.run_time % 100
                          + (h.run_duration / 10000) * 3600 + (h.run_duration / 100 % 100) * 60 + h.run_duration % 100,
                            CONVERT(datetime, CONVERT(char(8), h.run_date), 112)))
    FROM msdb.dbo.sysjobhistory AS h WITH (NOLOCK)
    JOIN msdb.dbo.sysjobs       AS j WITH (NOLOCK) ON j.job_id = h.job_id
    WHERE j.name = N'MCI Regular update' AND h.step_name = N'INSERT I_CHECK_DELTA' AND h.run_status = 1;

    SELECT @clr_dt = MAX(DATEADD(second, (h.run_time / 10000) * 3600 + (h.run_time / 100 % 100) * 60 + h.run_time % 100,
                                 CONVERT(datetime, CONVERT(char(8), h.run_date), 112)))
    FROM msdb.dbo.sysjobhistory AS h WITH (NOLOCK)
    JOIN msdb.dbo.sysjobs       AS j WITH (NOLOCK) ON j.job_id = h.job_id
    WHERE j.name = N'MCI Regular update' AND h.step_name = N'CLEAR DATA';

    IF @clr_dt > ISNULL(@ok_dt, '19000101')
        SELECT @BUSY = 1, @MSG = N'после шага «CLEAR DATA» MCI Regular update не было успешного «INSERT I_CHECK_DELTA» — дни удалены и не вставлены';
END
GO

-- 2.3 Ежедневный прогон из задания: ждёт сегодняшнюю загрузку чеков, пересчитывает её дни, оценивает, шлёт письмо.
CREATE OR ALTER PROCEDURE dbo.check_completeness_daily
      @RECIPIENTS         nvarchar(max) = NULL                       -- адреса через «;»; NULL — без письма
    , @PROFILE_NAME       sysname       = N'Mail'                    -- профиль Database Mail заданий MCI
    , @ID_COMPANY         int           = 1
    , @K                  decimal(5,2)  = 3.5
    , @NIGHT_WINDOW_DAYS  int           = 10
    , @DEADLINE_HOUR      int           = 20                         -- после этого часа без загрузки — письмо «не загружено»
    , @JOB_NAME           sysname       = N'MCI Regular update'
    , @STEP_NAME          sysname       = N'INSERT I_CHECK_DELTA'    -- шаг 12, check_insert: после него чеки в I_CHECK
    , @CLEAR_STEP_NAME    sysname       = N'CLEAR DATA'              -- шаг 1, clear_data_update: удаляет дни окна из I_CHECK
    , @FORCE             bit           = 0                          -- 1 — без ожидания загрузки (ручной прогон)
    , @HOLIDAY_DAYTYPE    int           = 3                          -- код праздника в I_CALENDAR.DAYTYPE (1 рабочий, 2 выходной, 3 праздник); в письме праздники только числом
    , @MAX_TRIES          int           = 3                          -- прогонов с ошибкой на одну загрузку, дальше ждать ручного
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @now      datetime = GETDATE();
    DECLARE @today    date     = CAST(GETDATE() AS date);
    DECLARE @yesterday date    = DATEADD(day, -1, @today);   -- последний полный день: сегодняшний не пересчитывается
    DECLARE @load_dt  datetime, @last_ok datetime, @d_from date, @d_to date, @run_id int, @hist_next date;
    DECLARE @subject  nvarchar(255), @body nvarchar(max);
    DECLARE @mail_rc  int;                          -- код возврата sp_send_dbmail: 0 — письмо в очереди Database Mail
    DECLARE @mail_err nvarchar(2048), @busy_msg nvarchar(2048);
    DECLARE @floor_msg nvarchar(400) = NULL;        -- пометка о границах окна пересчёта (по итоговому окну)
    DECLARE @rel_from date, @rel_to date;           -- перезалитые загрузкой дни (service.loading_log), до расширения и обрезки
    -- нижняя граница пересчёта дельты из service.loading_log: глубина месячной перезаливки MCI (с 1-го числа три месяца
    -- назад, задание MCI Reload every day) плюс месяц — на загрузку, закончившуюся уже в следующем месяце. Защищает от
    -- битой старой даты в дельте; перезалитые дни раньше границы письмо называет отдельной пометкой
    DECLARE @floor_from  date     = DATEFROMPARTS(YEAR(DATEADD(month, -4, @today)), MONTH(DATEADD(month, -4, @today)), 1);

    -- окончание последнего успешного шага загрузки чеков (за 3 дня)
    SELECT @load_dt = MAX(DATEADD(second,
                              (h.run_time / 10000) * 3600 + (h.run_time / 100 % 100) * 60 + h.run_time % 100
                            + (h.run_duration / 10000) * 3600 + (h.run_duration / 100 % 100) * 60 + h.run_duration % 100,
                              CONVERT(datetime, CONVERT(char(8), h.run_date), 112)))
    FROM msdb.dbo.sysjobhistory AS h WITH (NOLOCK)
    JOIN msdb.dbo.sysjobs       AS j WITH (NOLOCK) ON j.job_id = h.job_id
    WHERE j.name        = @JOB_NAME
      AND h.step_name   = @STEP_NAME
      AND h.run_status  = 1
      AND h.run_date   >= CONVERT(int, CONVERT(char(8), DATEADD(day, -3, @today), 112));

    -- загрузка обработана, если по ней был успешный прогон задания (OK) или ручной (MANUAL): после ручного
    -- запуска с @FORCE = 1 задание ту же загрузку не повторяет и не считает её зависшей
    SELECT @last_ok = MAX(LOAD_DT)
    FROM dbo.I_CHECK_COMPLETENESS_LOG WITH (NOLOCK)
    WHERE STATUS IN ('OK', 'MANUAL');

    -- после ошибки — не больше @MAX_TRIES прогонов на одну загрузку, дальше только ручной запуск (@FORCE = 1);
    -- такая загрузка считается «новой загрузки нет», чтобы к вечеру ушло письмо.
    -- RUN — прогон прерван (остановка задания, обрыв соединения): CATCH не выполнился, это тоже неудачная попытка;
    -- иначе прерываемый каждый час прогон шёл бы весь день без единого письма.
    -- Считаются только прогоны задания (IS_MANUAL = 0): ручной запуск попытки задания не расходует
    DECLARE @stuck bit = 0, @tries int = 0;
    IF @FORCE = 0 AND @load_dt IS NOT NULL AND @load_dt > ISNULL(@last_ok, '19000101')
    BEGIN
        SELECT @tries = COUNT(*) FROM dbo.I_CHECK_COMPLETENESS_LOG WITH (NOLOCK)
        WHERE STATUS IN ('ERROR', 'RUN') AND LOAD_DT = @load_dt AND IS_MANUAL = 0;
        IF @tries >= @MAX_TRIES SET @stuck = 1;
    END

    -- дни чеков сейчас могут быть удалены: после последней успешной загрузки (@load_dt) запускался шаг 1 @CLEAR_STEP_NAME
    -- (clear_data_update удаляет дни окна; вставит их только следующий успешный шаг 12) — чтение дало бы нули.
    -- Признак — строка шага 1 в истории с началом позже @load_dt при любом исходе (успех, ошибка, запуск упал или ещё
    -- идёт), либо запуск задания позже @load_dt, у которого ещё идёт первый шаг или только что закончился шаг 1
    -- (строки в истории может ещё не быть). Перезапуск с шага после 12 дни не удаляет: он останавливает проверку
    -- только пока идёт его первый шаг (с какого шага начат запуск, до конца первого шага не видно), дальше — нет.
    -- Нет загрузки за 3 дня — признак по любому шагу 1 / запуску в истории
    DECLARE @busy bit = 0, @busy_start datetime, @busy_src tinyint = 0;   -- 1 — шаг CLEAR DATA в истории, 2 — идёт первый шаг
    SELECT @busy_start = MAX(DATEADD(second, (h.run_time / 10000) * 3600 + (h.run_time / 100 % 100) * 60 + h.run_time % 100,
                                     CONVERT(datetime, CONVERT(char(8), h.run_date), 112)))
    FROM msdb.dbo.sysjobhistory AS h WITH (NOLOCK)
    JOIN msdb.dbo.sysjobs       AS j WITH (NOLOCK) ON j.job_id = h.job_id
    WHERE j.name      = @JOB_NAME
      AND h.step_name = @CLEAR_STEP_NAME
      AND h.run_date >= CONVERT(int, CONVERT(char(8), CAST(ISNULL(@load_dt, DATEADD(day, -3, @today)) AS date), 112))
      AND DATEADD(second, (h.run_time / 10000) * 3600 + (h.run_time / 100 % 100) * 60 + h.run_time % 100,
                  CONVERT(datetime, CONVERT(char(8), h.run_date), 112)) > ISNULL(@load_dt, '19000101');

    IF @busy_start IS NOT NULL
        SET @busy_src = 1;
    ELSE
        SELECT TOP (1) @busy_start = a.start_execution_date, @busy_src = 2
        FROM msdb.dbo.sysjobactivity AS a WITH (NOLOCK)
        JOIN msdb.dbo.sysjobs        AS j WITH (NOLOCK) ON j.job_id = a.job_id
        WHERE j.name = @JOB_NAME
          AND a.session_id = (SELECT MAX(s.session_id) FROM msdb.dbo.syssessions AS s WITH (NOLOCK))
          AND a.start_execution_date > ISNULL(@load_dt, '19000101')
          AND a.stop_execution_date IS NULL
          AND ISNULL(a.last_executed_step_id, 1) = 1   -- идёт первый шаг или только что закончился шаг 1
        ORDER BY a.start_execution_date DESC;

    IF @busy_start IS NOT NULL
        SET @busy = 1;

    -- первый прогон (в журнале нет OK / MANUAL) берёт только сегодняшнюю загрузку: иначе задание, включённое
    -- до окончания сегодняшнего шага, прислало бы два письма — по вчерашней загрузке и по сегодняшней.
    -- После загрузки запускался шаг 1 MCI или идёт первый шаг нового запуска (@busy) — тоже «новой загрузки нет» (не попытка, в журнал не пишется до вечернего письма)
    IF @FORCE = 0 AND (@load_dt IS NULL OR @load_dt <= ISNULL(@last_ok, '19000101') OR @stuck = 1 OR @busy = 1
                       OR (@last_ok IS NULL AND CAST(@load_dt AS date) < @today))
    BEGIN
        -- новой загрузки нет; к вечеру без сегодняшней загрузки, с зависшей или с непроверенной из-за нового прохода
        -- MCI — одно письмо за день. Уже проверенная сегодняшняя загрузка (OK / MANUAL) письма не даёт
        -- первый прогон вчерашнюю загрузку не берёт (см. условие выше) — для письма она тоже не «непроверенная»
        DECLARE @unproc bit = CASE WHEN @load_dt > ISNULL(@last_ok, '19000101')
                                        AND NOT (@last_ok IS NULL AND CAST(@load_dt AS date) < @today) THEN 1 ELSE 0 END;

        IF DATEPART(hour, @now) >= @DEADLINE_HOUR
           AND (@stuck = 1 OR (@busy = 1 AND @unproc = 1) OR @load_dt IS NULL OR CAST(@load_dt AS date) < @today)
           AND NOT EXISTS (SELECT 1 FROM dbo.I_CHECK_COMPLETENESS_LOG WITH (NOLOCK) WHERE STATUS = 'NO_LOAD' AND STARTED >= @today)
        BEGIN
            -- только факты, без обещаний: что будет дальше, зависит от того, с какого шага начат проход
            SET @busy_msg = CASE @busy_src
                WHEN 1 THEN CONCAT(N' ', CASE WHEN @load_dt IS NULL THEN N'За 3 дня'
                                              ELSE CONCAT(N'После загрузки ', FORMAT(@load_dt, 'dd.MM.yyyy HH:mm')) END,
                                   N' запускался шаг «', @CLEAR_STEP_NAME, N'» задания «', @JOB_NAME, N'» (удаляет дни окна из I_CHECK) — ',
                                   FORMAT(@busy_start, 'dd.MM.yyyy HH:mm'), N'; успешного шага «', @STEP_NAME, N'» после него не было. ',
                                   N'Пока так, проверка полноты не выполняется и ручной запуск даёт ошибку 50006.')
                WHEN 2 THEN CONCAT(N' Задание «', @JOB_NAME, N'» запущено ', FORMAT(@busy_start, 'dd.MM.yyyy HH:mm'),
                                   N'; идёт первый шаг прохода или только что закончился шаг «', @CLEAR_STEP_NAME, N'». ',
                                   N'Пока так, проверка полноты не выполняется и ручной запуск даёт ошибку 50006.')
                ELSE N'' END;

            IF (@stuck = 1 OR (@busy = 1 AND @unproc = 1)) AND CAST(@load_dt AS date) = @today
            BEGIN
                -- чеки сегодня загружены, но проверка упала @MAX_TRIES раз или до неё начался новый проход MCI
                SET @subject = CONCAT(N'MCI чеки ', FORMAT(@today, 'dd.MM.yyyy'), N': проверка полноты не выполнена');
                SET @body = CONCAT(N'<p>Шаг «', @STEP_NAME, N'» задания «', @JOB_NAME, N'» завершён ',
                                   FORMAT(@load_dt, 'dd.MM.yyyy HH:mm'), N', но проверка полноты по этой загрузке не выполнена.',
                                   CASE WHEN @stuck = 1
                                        THEN CONCAT(N' Неудачных прогонов задания по этой загрузке: ', @tries,
                                                    N' (ошибка или прерванный прогон, см. dbo.I_CHECK_COMPLETENESS_LOG); ',
                                                    N'задание её больше не повторяет. Запустить вручную: ',
                                                    N'EXEC dbo.check_completeness_daily @RECIPIENTS = N''…'', @FORCE = 1;')
                                        ELSE N'' END,
                                   @busy_msg, N'</p>');
            END
            ELSE
            BEGIN
                SET @subject = CONCAT(N'MCI чеки ', FORMAT(@today, 'dd.MM.yyyy'), N': сегодня чеки не загружены');
                SET @body = CONCAT(N'<p>Шаг «', @STEP_NAME, N'» задания «', @JOB_NAME, N'» сегодня не завершился успешно. ',
                                   N'Последнее успешное окончание: ', ISNULL(FORMAT(@load_dt, 'dd.MM.yyyy HH:mm'), N'нет за 3 дня'),
                                   N'. Проверка полноты по сегодняшней загрузке не выполнялась.',
                                   -- прошлая загрузка зависла (третья неудача могла быть уже после 20:00 — тогда письма о ней не было)
                                   CASE WHEN @stuck = 1
                                        THEN CONCAT(N' Неудачных прогонов задания по загрузке ', FORMAT(@load_dt, 'dd.MM.yyyy HH:mm'), N': ',
                                                    @tries, N' (см. dbo.I_CHECK_COMPLETENESS_LOG); задание её больше не повторяет. ',
                                                    N'Запустить вручную: EXEC dbo.check_completeness_daily @RECIPIENTS = N''…'', @FORCE = 1;')
                                        ELSE N'' END,
                                   @busy_msg, N'</p>');
            END
            -- сначала журнал, потом письмо: если почта упадёт, NO_LOAD за сегодня уже записан и в 21 и 22 часа
            -- повторных попыток не будет
            INSERT INTO dbo.I_CHECK_COMPLETENESS_LOG (STARTED, FINISHED, LOAD_DT, MAIL_SENT, STATUS, IS_MANUAL)
            VALUES (@now, GETDATE(), @load_dt, 0, 'NO_LOAD', @FORCE);
            SET @run_id = SCOPE_IDENTITY();

            IF @RECIPIENTS IS NOT NULL
            BEGIN
                EXEC @mail_rc = msdb.dbo.sp_send_dbmail @profile_name = @PROFILE_NAME, @recipients = @RECIPIENTS,
                     @subject = @subject, @body = @body, @body_format = 'HTML';

                -- MAIL_SENT = 1 — письмо в очереди Database Mail; доставка — msdb.dbo.sysmail_allitems
                IF @mail_rc = 0
                BEGIN
                    UPDATE dbo.I_CHECK_COMPLETENESS_LOG SET MAIL_SENT = 1 WHERE RUN_ID = @run_id;
                END
                ELSE
                BEGIN
                    SET @mail_err = CONCAT(N'check_completeness_daily: sp_send_dbmail вернул ', @mail_rc,
                                           N', письмо «', @subject, N'» не поставлено в очередь (RUN_ID ', @run_id, N')');
                    THROW 50005, @mail_err, 1;
                END
            END
        END
        RETURN;
    END

    -- сюда при @busy доходит только ручной прогон — он не считает по возможно удалённым дням
    IF @busy = 1
    BEGIN
        SET @busy_msg = CASE @busy_src
            WHEN 1 THEN CONCAT(N'check_completeness_daily: ',
                               CASE WHEN @load_dt IS NULL THEN N'за 3 дня' ELSE CONCAT(N'после загрузки ', FORMAT(@load_dt, 'dd.MM.yyyy HH:mm')) END,
                               N' запускался шаг «', @CLEAR_STEP_NAME, N'» задания «', @JOB_NAME, N'» (', FORMAT(@busy_start, 'dd.MM.yyyy HH:mm'),
                               N'), успешного шага «', @STEP_NAME, N'» после него не было — запустить после следующего успешного шага «',
                               @STEP_NAME, N'».')
            ELSE CONCAT(N'check_completeness_daily: задание «', @JOB_NAME, N'» запущено ', FORMAT(@busy_start, 'dd.MM.yyyy HH:mm'),
                        N'; идёт первый шаг прохода или только что закончился шаг «', @CLEAR_STEP_NAME, N'». Если проход начат с шага «',
                        @CLEAR_STEP_NAME, N'» — запустить после следующего успешного шага «', @STEP_NAME,
                        N'», если с другого шага — после окончания первого шага.') END;
        THROW 50006, @busy_msg, 1;
    END

    -- журнал до расчёта окна: прерванный на расчёте окна прогон тоже остаётся строкой RUN (попыткой)
    INSERT INTO dbo.I_CHECK_COMPLETENESS_LOG (STARTED, LOAD_DT, D_FROM, D_TO, STATUS, MSG, IS_MANUAL)
    VALUES (@now, @load_dt, NULL, NULL, 'RUN', NULL, @FORCE);
    SET @run_id = SCOPE_IDENTITY();

    BEGIN TRY

        -- дни сегодняшней загрузки: что записал шаг 8 в service.loading_log после прошлого прогона.
        -- Первый прогон (в журнале ещё нет OK / MANUAL) — всё, что записано за сутки до окончания шага загрузки:
        -- сегодняшняя загрузка (в том числе трёхмесячная) записана в loading_log раньше этого шага
        SELECT @d_from = MIN(min_date), @d_to = MAX(max_date)
        FROM service.loading_log WITH (NOLOCK)
        WHERE table_name = N'I_CHECK_DELTA'
          AND min_date IS NOT NULL
          AND max_date IS NOT NULL
          AND dt_load > ISNULL(@last_ok, DATEADD(day, -1, ISNULL(@load_dt, @now)));

        -- перезалитые загрузкой дни (до расширения первым прогоном и обрезки); окно письма — их пересечение
        -- с пересчитанными, ниже
        SELECT @rel_from = @d_from, @rel_to = @d_to;

        SET @d_from = ISNULL(@d_from, DATEADD(day, -@NIGHT_WINDOW_DAYS, @today));
        SET @d_to   = ISNULL(@d_to,   @yesterday);

        -- дельта — не глубже нижней границы и не позже вчерашнего дня: битая старая или будущая дата не должна запускать
        -- пересчёт тысяч дней (будущий день ещё и сдвинул бы последний день оценки); неполный сегодняшний день в оценку
        -- не попадает
        IF @d_from < @floor_from SET @d_from = @floor_from;
        IF @d_to > @yesterday    SET @d_to = @yesterday;

        -- первый прогон: если история кончается раньше вчерашнего дня — пересчёт подряд по вчера от более раннего из двух
        -- дней — начала окна дельты или следующего за последним днём истории (раздел 3); окно дельты пустое (вся дельта вне
        -- границ) — от следующего за последним днём истории. История не кончается раньше вчерашнего дня — окно дельты.
        -- Без расширения дни между историей и дельтой остались бы пустыми и попали бы в оценку как «нет чеков за день».
        -- Граница дельты здесь не действует: @hist_next — из своей таблицы, не из loading_log.
        -- Последний день истории — без тестового канала (2), как в оценке
        DECLARE @first_ext bit = 0;
        IF @last_ok IS NULL
        BEGIN
            SELECT @hist_next = DATEADD(day, 1, MAX(DATA))
            FROM dbo.I_CHECK_COMPLETENESS WITH (NOLOCK)
            WHERE ID_COMPANY = @ID_COMPANY
              AND ID_ORGANIZATION <> 2;

            IF @hist_next IS NOT NULL AND @hist_next <= @yesterday
            BEGIN
                IF @hist_next < @d_from OR @d_from > @d_to
                    SELECT @d_from = @hist_next, @first_ext = 1;
                SET @d_to = @yesterday;
            END
        END

        -- пометка о границах — по итоговому окну пересчёта, а не по ходу расчёта
        SET @floor_msg = NULLIF(CONCAT_WS(N'; '
            , CASE WHEN @rel_from IS NOT NULL AND @d_from <= @d_to AND @rel_from < @d_from
                        AND @rel_to >= @d_from AND @rel_from <= @d_to   -- дельта пересекается с окном
                   THEN CONCAT(N'дельта с ', FORMAT(@rel_from, 'dd.MM.yyyy'), N' пересчитана с ', FORMAT(@d_from, 'dd.MM.yyyy'),
                               CASE WHEN @first_ext = 0 THEN N' (нижняя граница пересчёта дельты)' ELSE N'' END) END
            , CASE WHEN @rel_to IS NOT NULL AND @d_from <= @d_to AND @rel_to > @d_to
                        AND @rel_to >= @d_from AND @rel_from <= @d_to
                   THEN CONCAT(N'дельта по ', FORMAT(@rel_to, 'dd.MM.yyyy'), N' пересчитана по ', FORMAT(@d_to, 'dd.MM.yyyy'),
                               N' (сегодняшний и будущие дни не оцениваются)') END
            , CASE WHEN @first_ext = 1
                   THEN CONCAT(N'первый прогон: пересчёт с ', FORMAT(@hist_next, 'dd.MM.yyyy'), N' — со следующего за последним днём истории') END
            , CASE WHEN @rel_from IS NOT NULL AND @d_from > @d_to
                   THEN CONCAT(N'пересчёт не нужен: вся дельта вне диапазона с ', FORMAT(@floor_from, 'dd.MM.yyyy'),
                               N' по ', FORMAT(@yesterday, 'dd.MM.yyyy')) END
            ), N'');

        UPDATE dbo.I_CHECK_COMPLETENESS_LOG
        SET D_FROM = @d_from, D_TO = @d_to, REL_FROM = @rel_from, REL_TO = @rel_to, MSG = @floor_msg
        WHERE RUN_ID = @run_id;

        IF @d_from <= @d_to
            EXEC dbo.check_completeness_fill @ID_COMPANY = @ID_COMPANY, @D_FROM = @d_from, @D_TO = @d_to;
        EXEC dbo.check_completeness_eval @ID_COMPANY = @ID_COMPANY, @K = @K, @NIGHT_WINDOW_DAYS = @NIGHT_WINDOW_DAYS,
                                         @HOLIDAY_DAYTYPE = @HOLIDAY_DAYTYPE;

        -- чтение _DAY для письма — под разделяемой блокировкой: параллельный eval переписывает _DAY целиком
        BEGIN TRAN;
        EXEC @mail_rc = sp_getapplock @Resource = N'I_CHECK_COMPLETENESS_EVAL', @LockMode = N'Shared', @LockOwner = N'Transaction', @LockTimeout = -1;
        IF @mail_rc < 0
            THROW 50003, N'check_completeness_daily: не получена блокировка I_CHECK_COMPLETENESS_EVAL', 1;

        -- письмо: сколько дней перезатёрла загрузка (@rel_from..@rel_to из service.loading_log) — те и смотрим, в пределах
        -- пересчитанных этим прогоном: проблемный день среди них значит, что перезаливка его не исправила. Перезалитые,
        -- но не пересчитанные дни — если пересчёт был и они раньше его начала: пометкой ниже «раньше начала пересчёта»;
        -- позже вчерашнего при частичном пересчёте: в пометке о границах; если пересчёта не было или ни один перезалитый
        -- день не пересчитан (и это не случай «раньше начала») — «вне границ пересчёта». Записи в loading_log нет — окно
        -- письма = пересчитанные дни, и так и подписано. Остальная история и праздники — только числом (разбор — страница
        -- «Сверка данных» и раздел 4). Нет чеков за вчера ни в одном канале — первым в теме
        DECLARE @max_date date = (SELECT MAX(DATA) FROM dbo.I_CHECK_COMPLETENESS_DAY WITH (NOLOCK) WHERE ID_COMPANY = @ID_COMPANY);
        -- окно письма: перезалитые И пересчитанные этим прогоном дни — вердикт только по данным после перезаливки
        DECLARE @m_from date = CASE WHEN @d_from > @d_to THEN NULL
                                    WHEN @rel_from IS NULL THEN @d_from
                                    WHEN @rel_from > @d_from THEN @rel_from ELSE @d_from END;
        DECLARE @m_to   date = CASE WHEN @d_from > @d_to THEN NULL
                                    WHEN @rel_from IS NULL THEN @d_to
                                    WHEN @rel_to < @d_to THEN @rel_to ELSE @d_to END;
        IF @m_from > @m_to
            SELECT @m_from = NULL, @m_to = NULL;
        DECLARE @m_lbl  nvarchar(40) = CASE WHEN @rel_from IS NOT NULL THEN N'перезалитых' ELSE N'пересчитанных' END;
        DECLARE @nA int, @nOld int, @nHol int;
        DECLARE @rowsA nvarchar(max);

        SELECT    @nA   = SUM(CASE WHEN IS_HOLIDAY = 0 AND DATA BETWEEN @m_from AND @m_to THEN 1 ELSE 0 END)
                , @nHol = SUM(CASE WHEN IS_HOLIDAY = 1 AND DATA BETWEEN @m_from AND @m_to THEN 1 ELSE 0 END)
                , @nOld = SUM(CASE WHEN DATA BETWEEN @m_from AND @m_to THEN 0 ELSE 1 END)   -- NULL-окно — вся история
        FROM dbo.I_CHECK_COMPLETENESS_DAY WITH (NOLOCK)
        WHERE ID_COMPANY = @ID_COMPANY
          AND IS_PROBLEM = 1;

        SET @rowsA = CAST((
            SELECT    td = FORMAT(DATA, 'dd.MM.yyyy'), ''
                    , td = ISNULL(DOW, N''), ''   -- NULL выкинул бы ячейку и сдвинул столбцы
                    , td = ORGANIZATION, ''
                    , td = FORMAT(CHECKS, 'N0', 'ru-RU'), ''
                    , td = ISNULL(FORMAT(BASE_CHECKS, 'N0', 'ru-RU'), N'—'), ''
                    , td = ISNULL(FORMAT(DEV_CHECKS, '+0%;-0%;0%'), N'—'), ''
                    , td = CASE WHEN IN_NIGHT_WINDOW = 1 THEN N'да' ELSE N'нет' END, ''
                    , td = FLAG
            FROM dbo.I_CHECK_COMPLETENESS_DAY WITH (NOLOCK)
            WHERE ID_COMPANY = @ID_COMPANY AND IS_PROBLEM = 1 AND IS_HOLIDAY = 0 AND DATA BETWEEN @m_from AND @m_to
            ORDER BY DATA DESC, ID_ORGANIZATION
            FOR XML PATH('tr'), TYPE) AS nvarchar(max));

        DECLARE @th nvarchar(400) = N'<tr><th>Дата</th><th>День</th><th>Канал</th><th>Чеков</th><th>Норма</th><th>Откл.</th><th>В ночном окне</th><th>Признак</th></tr>';

        SET @subject = CONCAT(N'MCI чеки ', FORMAT(@today, 'dd.MM.yyyy'), N': ',
                              CASE WHEN @max_date IS NULL OR @max_date < @yesterday
                                   THEN CONCAT(N'нет чеков за ', FORMAT(@yesterday, 'dd.MM.yyyy'), N'; ') ELSE N'' END,
                              CASE WHEN @m_from IS NULL AND @rel_from IS NULL THEN N'нет записи о перезалитых днях, дни не пересчитывались'
                                   WHEN @m_from IS NULL THEN N'перезалитые дни не пересчитаны'
                                   WHEN ISNULL(@nA, 0) > 0 THEN CONCAT(N'проблемных день×канал среди ', @m_lbl, N' дней — ', @nA)
                                   WHEN @rel_from IS NOT NULL AND (@rel_from < @m_from OR @rel_to > @m_to)
                                   THEN N'пересчитанные перезалитые дни без замечаний, часть перезалитых не пересчитана'
                                   WHEN @rel_from IS NOT NULL THEN N'перезалитые дни без замечаний'
                                   ELSE N'пересчитанные дни без замечаний' END);

        SET @body = CONCAT(
              N'<style>table{border-collapse:collapse;font:13px Arial}td,th{border:1px solid #ccc;padding:3px 6px;text-align:left}</style>'
            , N'<p>Последний день чеков: <b>', ISNULL(FORMAT(@max_date, 'dd.MM.yyyy'), N'—'), N'</b>'
            , CASE WHEN @max_date IS NULL OR @max_date < @yesterday THEN CONCAT(N' (ожидалось ', FORMAT(@yesterday, 'dd.MM.yyyy'), N')') ELSE N'' END
            , N'. Загрузка: шаг «', @STEP_NAME, N'» завершён ', ISNULL(FORMAT(@load_dt, 'dd.MM.yyyy HH:mm'), N'—')
            , CASE WHEN @rel_from IS NOT NULL
                   THEN CONCAT(N'; перезалиты дни ', FORMAT(@rel_from, 'dd.MM.yyyy'), N'–', FORMAT(@rel_to, 'dd.MM.yyyy'),
                               N' (', DATEDIFF(day, @rel_from, @rel_to) + 1, N' дн., service.loading_log)')
                   WHEN @FORCE = 1 AND @load_dt <= @last_ok THEN N'; загрузка уже проверена, новых записей в service.loading_log нет'
                   ELSE N'; в service.loading_log нет записи о перезалитых днях' END
            , CASE WHEN @d_from <= @d_to
                   THEN CONCAT(N'; пересчитаны дни ', FORMAT(@d_from, 'dd.MM.yyyy'), N'–', FORMAT(@d_to, 'dd.MM.yyyy'))
                   ELSE N'; дни не пересчитывались' END
            , CASE WHEN @floor_msg IS NOT NULL THEN CONCAT(N' — ', @floor_msg) ELSE N'' END
            , N'.</p>'
            , CASE WHEN @m_from IS NOT NULL
                   THEN CONCAT(N'<h3>Проблемные дни среди ', @m_lbl, N' (', FORMAT(@m_from, 'dd.MM.yyyy'), N'–', FORMAT(@m_to, 'dd.MM.yyyy'), N')</h3>',
                               CASE WHEN @rowsA IS NULL THEN N'<p>Замечаний нет.</p>' ELSE CONCAT(N'<table>', @th, @rowsA, N'</table>') END,
                               N'<p style="color:#666;font-size:12px">«В ночном окне» — день входит в последние ', @NIGHT_WINDOW_DAYS,
                               N' дней, которые ночная перезаливка берёт каждую ночь.</p>')
                   ELSE N'' END
            , CASE WHEN @rel_from IS NOT NULL AND @rel_from < @d_from AND @d_from <= @d_to
                   THEN CONCAT(N'<p>Дни ', FORMAT(@rel_from, 'dd.MM.yyyy'), N'–',
                               FORMAT(CASE WHEN @rel_to < @d_from THEN @rel_to ELSE DATEADD(day, -1, @d_from) END, 'dd.MM.yyyy'),
                               N' перезалиты, но этим прогоном не пересчитаны (раньше начала пересчёта): признаки по ним — от прошлого ',
                               N'пересчёта, до перезаливки. Пересчитать: EXEC dbo.check_completeness_fill с этими датами, затем ',
                               N'EXEC dbo.check_completeness_eval.</p>')
                   WHEN @rel_from IS NOT NULL AND @m_from IS NULL
                   THEN CONCAT(N'<p>Дни ', FORMAT(@rel_from, 'dd.MM.yyyy'), N'–', FORMAT(@rel_to, 'dd.MM.yyyy'),
                               N' перезалиты, но этим прогоном не пересчитаны (вне границ пересчёта).</p>')
                   ELSE N'' END
            , CASE WHEN ISNULL(@nHol, 0) > 0 THEN CONCAT(N'<p>Праздничные дни (I_CALENDAR.DAYTYPE = ', @HOLIDAY_DAYTYPE, N') с признаками среди ',
                                                         @m_lbl, N': ', @nHol,
                                                         N' день×канал — в таблицу не выносятся, есть в dbo.I_CHECK_COMPLETENESS_DAY (IS_HOLIDAY = 1).</p>') ELSE N'' END
            , CASE WHEN ISNULL(@nOld, 0) > 0 THEN CONCAT(N'<p>В остальной истории проблемных день×канал: ', @nOld,
                                                         N' — разбор на странице «Сверка данных» портала и в dbo.I_CHECK_COMPLETENESS_DAY.</p>') ELSE N'' END
            , N'<p style="color:#666;font-size:12px">Норма — медиана того же дня недели за ±4 недели. Провал и всплеск — отклонение больше '
            , @K, N' робастных σ от обычного разброса канала за всю историю. Подробно: dbo.I_CHECK_COMPLETENESS_DAY; '
            , N'выпавшие магазины: dbo.I_CHECK_COMPLETENESS_SHOP; день × час × магазин: dbo.I_CHECK_COMPLETENESS.</p>'
        );

        COMMIT;   -- снимает разделяемую блокировку I_CHECK_COMPLETENESS_EVAL

        -- пересчёт и оценка выполнены — статус до письма: если почта упадёт, загрузка уже закрыта
        -- и пересчёт каждый час не повторяется
        UPDATE dbo.I_CHECK_COMPLETENESS_LOG
        SET FINISHED     = GETDATE()
          , DAYS_FLAGGED = ISNULL(@nA, 0)
          , MAIL_SENT    = 0
          , STATUS       = CASE WHEN @FORCE = 1 THEN 'MANUAL' ELSE 'OK' END
        WHERE RUN_ID = @run_id;
    END TRY
    BEGIN CATCH
        DECLARE @err nvarchar(4000) = ERROR_MESSAGE();

        IF @@TRANCOUNT > 0 ROLLBACK;

        UPDATE dbo.I_CHECK_COMPLETENESS_LOG
        SET FINISHED = GETDATE(), STATUS = 'ERROR', MSG = LEFT(CONCAT_WS(N'; ', MSG, @err), 4000)   -- пометку о границах окна пересчёта не затирать
        WHERE RUN_ID = @run_id;

        -- задание повторяет прогон той же загрузки каждый час — письмо об ошибке задания по одной загрузке одно;
        -- письма ручных прогонов (IS_MANUAL = 1) письмо задания не подавляют
        IF @RECIPIENTS IS NOT NULL
           AND (@FORCE = 1
                OR NOT EXISTS (SELECT 1 FROM dbo.I_CHECK_COMPLETENESS_LOG WITH (NOLOCK)
                               WHERE STATUS = 'ERROR' AND MAIL_SENT = 1 AND LOAD_DT = @load_dt AND IS_MANUAL = 0
                                 AND RUN_ID <> @run_id))
        BEGIN
            SET @subject = CONCAT(N'MCI чеки ', FORMAT(@today, 'dd.MM.yyyy'), N': ошибка проверки полноты');
            SET @body    = CONCAT(N'<p>check_completeness_daily', CASE WHEN @FORCE = 1 THEN N' (ручной прогон, @FORCE = 1)' ELSE N'' END,
                                  N': ', @err, N'</p>',
                                  CASE WHEN @FORCE = 1
                                       THEN N'<p>Ручной прогон попыток задания не расходует и статус загрузки не меняет. Повторить: EXEC dbo.check_completeness_daily @RECIPIENTS = N''…'', @FORCE = 1;</p>'
                                       ELSE CONCAT(N'<p>Неудачных прогонов задания по этой загрузке: ', @tries + 1, N' из ', @MAX_TRIES, N'. ',
                                                   N'После ', @MAX_TRIES, N' неудачных прогонов задание эту загрузку больше не берёт. ',
                                                   N'Вручную: EXEC dbo.check_completeness_daily @RECIPIENTS = N''…'', @FORCE = 1;</p>') END);
            EXEC @mail_rc = msdb.dbo.sp_send_dbmail @profile_name = @PROFILE_NAME, @recipients = @RECIPIENTS,
                 @subject = @subject, @body = @body, @body_format = 'HTML';

            -- MAIL_SENT = 1 только если письмо в очереди; иначе не ставить — исходную ошибку перебросит THROW ниже
            IF @mail_rc = 0
                UPDATE dbo.I_CHECK_COMPLETENESS_LOG SET MAIL_SENT = 1 WHERE RUN_ID = @run_id;
        END;

        THROW;
    END CATCH

    -- сюда доходит только успешный прогон (CATCH заканчивается THROW). Письмо вне TRY: ошибка почты уходит
    -- в задание, статус OK / MANUAL остаётся, MAIL_SENT = 0 показывает, что письма не было
    IF @RECIPIENTS IS NOT NULL
    BEGIN
        EXEC @mail_rc = msdb.dbo.sp_send_dbmail @profile_name = @PROFILE_NAME, @recipients = @RECIPIENTS,
             @subject = @subject, @body = @body, @body_format = 'HTML';

        -- MAIL_SENT = 1 — письмо в очереди Database Mail; доставка — msdb.dbo.sysmail_allitems
        IF @mail_rc = 0
        BEGIN
            UPDATE dbo.I_CHECK_COMPLETENESS_LOG SET MAIL_SENT = 1 WHERE RUN_ID = @run_id;
        END
        ELSE
        BEGIN
            SET @mail_err = CONCAT(N'check_completeness_daily: sp_send_dbmail вернул ', @mail_rc,
                                   N', письмо «', @subject, N'» не поставлено в очередь (RUN_ID ', @run_id, N')');
            THROW 50005, @mail_err, 1;
        END
    END
END
GO

-- ============================== 3. ИСТОРИЯ ==============================
-- Месяц за месяцем, печатает время каждого месяца. Перед заполнением, перед каждым месяцем и в конце проверяет MCI
-- (check_completeness_mci_busy): идёт задание MCI, оно стартовало во время заполнения или упало после удаления дней —
-- блок останавливается, разделы 6 и 7 тогда ничего не меняют. Повторный запуск файла продолжает историю.
DECLARE @from date;
DECLARE @to   date = DATEADD(day, -1, CAST(GETDATE() AS date));
DECLARE @m date, @m_end date, @t0 datetime, @msg nvarchar(600);
DECLARE @t_batch datetime = GETDATE(), @busy bit, @busy_msg nvarchar(400), @floor date;

EXEC sp_set_session_context @key = N'cc_history_ok', @value = 0;
EXEC sp_set_session_context @key = N'cc_history_filled', @value = 0;

EXEC dbo.check_completeness_mci_busy @SINCE = NULL, @BUSY = @busy OUTPUT, @MSG = @busy_msg OUTPUT;
IF @busy = 1
BEGIN
    SET @msg = CONCAT(N'История не заполнена: ', @busy_msg, N'. Старая сверка не заменена, задание не включено. Запустить файл после успешной загрузки MCI');
    THROW 50002, @msg, 1;
END

-- продолжение: с 1-го числа месяца последнего посчитанного дня, но не позже нижней границы перезаливки (1-е число
-- четыре месяца назад) — дни, перезалитые MCI между запусками, пересчитываются заново; истории нет — с первого дня чеков
SET @floor = DATEFROMPARTS(YEAR(DATEADD(month, -4, GETDATE())), MONTH(DATEADD(month, -4, GETDATE())), 1);
SET @from = (SELECT DATEFROMPARTS(YEAR(MAX(DATA)), MONTH(MAX(DATA)), 1) FROM dbo.I_CHECK_COMPLETENESS WITH (NOLOCK) WHERE ID_COMPANY = 1);
IF @from > @floor SET @from = @floor;
IF @from IS NULL
    SET @from = (SELECT MIN(DATA) FROM dbo.I_CHECKHEADER WITH (NOLOCK) WHERE ID_COMPANY = 1);

SET @m = @from;
WHILE @m <= @to
BEGIN
    EXEC dbo.check_completeness_mci_busy @SINCE = @t_batch, @BUSY = @busy OUTPUT, @MSG = @busy_msg OUTPUT;
    IF @busy = 1
    BEGIN
        SET @msg = CONCAT(N'История остановлена на ', FORMAT(@m, 'dd.MM.yyyy'), N': ', @busy_msg,
                          N'. Старая сверка не заменена, задание не включено. Запустить файл ещё раз после успешной загрузки MCI');
        THROW 50002, @msg, 1;
    END;

    SET @m_end = EOMONTH(@m);
    IF @m_end > @to SET @m_end = @to;
    SET @t0 = GETDATE();

    EXEC dbo.check_completeness_fill @ID_COMPANY = 1, @D_FROM = @m, @D_TO = @m_end;

    SET @msg = CONCAT(FORMAT(@m, 'dd.MM.yyyy'), N'–', FORMAT(@m_end, 'dd.MM.yyyy'), N': ', DATEDIFF(second, @t0, GETDATE()), N' с');
    RAISERROR(@msg, 0, 1) WITH NOWAIT;

    SET @m = DATEADD(day, 1, @m_end);
END

-- финальная проверка: за время заполнения MCI не стартовал и не удалял дни
EXEC dbo.check_completeness_mci_busy @SINCE = @t_batch, @BUSY = @busy OUTPUT, @MSG = @busy_msg OUTPUT;
IF @busy = 1
BEGIN
    SET @msg = CONCAT(N'История заполнена, но ', @busy_msg, N' — дни могли прочитаться пустыми. Старая сверка не заменена, задание не включено. Запустить файл ещё раз после успешной загрузки MCI');
    THROW 50002, @msg, 1;
END

-- история заполнена по вчера — флаг «заполнена»; «готово к включению» ставит раздел 4 после успешной оценки
EXEC sp_set_session_context @key = N'cc_history_filled', @value = 1;
RAISERROR(N'История заполнена по вчерашний день', 0, 1) WITH NOWAIT;
GO

-- ============================== 4. ОЦЕНКА ИСТОРИИ И ПРОСМОТР ==============================
DECLARE @filled int = ISNULL(CAST(SESSION_CONTEXT(N'cc_history_filled') AS int), 0);
EXEC sp_set_session_context @key = N'cc_history_filled', @value = 0;
EXEC dbo.check_completeness_eval @ID_COMPANY = 1;
-- оценка прошла без ошибки и история заполнена в этом запуске — разделы 6 и 7 могут заменить check_data и включить задание
IF @filled = 1
    EXEC sp_set_session_context @key = N'cc_history_ok', @value = 1;
GO

-- 4.1 Все проблемные дни по каналам
SELECT    DATA, DOW, IS_HOLIDAY, ORGANIZATION, CHECKS, BASE_CHECKS, DEV_CHECKS, Z_CHECKS
        , DEV_IDENT, DEV_LINES, SHOPS, SHOPS_MISSING, HOURS_LOW, DUP_CHECKS, MISMATCH_CHECKS, FLAG
FROM dbo.I_CHECK_COMPLETENESS_DAY WITH (NOLOCK)
WHERE ID_COMPANY = 1
  AND IS_PROBLEM = 1
ORDER BY ID_ORGANIZATION, DATA;

-- 4.2 По месяцам: сколько проблемных дней у каждого канала
SELECT    ORGANIZATION
        , MONTH_START  = DATEFROMPARTS(YEAR(DATA), MONTH(DATA), 1)
        , DAYS         = COUNT(*)
        , PROBLEM_DAYS = SUM(CAST(IS_PROBLEM AS int))
        , CHECKS       = SUM(CHECKS)
FROM dbo.I_CHECK_COMPLETENESS_DAY WITH (NOLOCK)
WHERE ID_COMPANY = 1
GROUP BY ORGANIZATION, ID_ORGANIZATION, DATEFROMPARTS(YEAR(DATA), MONTH(DATA), 1)
ORDER BY ID_ORGANIZATION, MONTH_START;

-- 4.3 Один день по часам против нормы (вписать дату)
DECLARE @day date = '2026-10-01';
SELECT    c.ID_ORGANIZATION, c.CLOCK
        , CHECKS    = SUM(CASE WHEN c.DATA = @day THEN c.CHECKS END)
        , WEEK_AGO  = SUM(CASE WHEN c.DATA = DATEADD(day, -7, @day) THEN c.CHECKS END)
        , LINES     = SUM(CASE WHEN c.DATA = @day THEN c.LINES END)
FROM dbo.I_CHECK_COMPLETENESS AS c WITH (NOLOCK)
WHERE c.ID_COMPANY = 1
  AND c.DATA IN (@day, DATEADD(day, -7, @day))
GROUP BY c.ID_ORGANIZATION, c.CLOCK
ORDER BY c.ID_ORGANIZATION, c.CLOCK;

-- 4.4 Выпавшие магазины за день (вписать дату)
SELECT m.DATA, m.ID_ORGANIZATION, m.ID_SHOP, s.SHOP_NAME, s.CITY, m.CHECKS_PREV_DAY, m.CONTROL_DATA, m.CHECKS_CONTROL
FROM dbo.I_CHECK_COMPLETENESS_SHOP AS m WITH (NOLOCK)
OUTER APPLY (SELECT TOP (1) s.SHOP_NAME, s.CITY FROM dbo.I_SHOP AS s WITH (NOLOCK)
             WHERE s.ID_SHOP = m.ID_SHOP AND s.ID_COMPANY = m.ID_COMPANY
             ORDER BY CASE WHEN s.ID_ORGANIZATION = m.ID_ORGANIZATION THEN 0 ELSE 1 END, s.SHOP_NAME) AS s
WHERE m.ID_COMPANY = 1
  AND m.DATA = @day
ORDER BY m.ID_ORGANIZATION, m.ID_SHOP;
GO

-- ============================== 5. ЗАДАНИЕ НА КАЖДЫЙ ДЕНЬ ==============================
-- Отдельное задание: каждый час 07:00–22:00 и сразу после шага 12 (его запускает новая dbo.check_data, раздел 6).
-- Создаётся выключенным, включает его раздел 7 — только если история заполнена и оценена в этом же запуске файла.
-- Владелец задания должен читать в msdb sysjobs, sysjobhistory, sysjobactivity и syssessions
-- и слать почту профилем «Mail» (как у заданий MCI); без прав на msdb шаг падает каждый час до записи в журнал.
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WITH (NOLOCK) WHERE name = N'CVM check completeness')
BEGIN
    EXEC msdb.dbo.sp_add_job
          @job_name    = N'CVM check completeness'
        , @enabled     = 0
        , @description = N'Полнота чеков и строк после загрузки из CDP: dbo.check_completeness_daily, письмо раз в день';

    EXEC msdb.dbo.sp_add_jobstep
          @job_name      = N'CVM check completeness'
        , @step_name     = N'check and mail'
        , @subsystem     = N'TSQL'
        , @database_name = N'mci_model'
        , @command       = N'EXEC dbo.check_completeness_daily @RECIPIENTS = N''elena.koryakova@gmail.com'';';

    EXEC msdb.dbo.sp_add_jobschedule
          @job_name             = N'CVM check completeness'
        , @name                 = N'hourly 07-22'
        , @freq_type            = 4      -- ежедневно
        , @freq_interval        = 1
        , @freq_subday_type     = 8      -- каждые N часов
        , @freq_subday_interval = 1
        , @active_start_time    = 70000
        , @active_end_time      = 220000;

    EXEC msdb.dbo.sp_add_jobserver @job_name = N'CVM check completeness', @server_name = N'(local)';
END
ELSE
    -- задание уже есть (повторный запуск файла) — обновить команду шага (адрес письма); включение не трогается
    EXEC msdb.dbo.sp_update_jobstep
          @job_name = N'CVM check completeness'
        , @step_id  = 1
        , @command  = N'EXEC dbo.check_completeness_daily @RECIPIENTS = N''elena.koryakova@gmail.com'';';
GO

-- ============================== 6. СТАРАЯ СВЕРКА dbo.check_data → НОВАЯ ПРОВЕРКА ==============================
-- dbo.check_data — шаг 13 «CHECKING_DATA» задания MCI Regular update, сразу после шага 12 «INSERT I_CHECK_DELTA».
-- Старая сверка (помесячные и дневные итоги в I_MCI_CHECK / I_MCI_CHECK_DATA, только при дельте за 2+ месяца)
-- неактуальна (решение 11.10.2026). Теперь шаг только запускает задание «CVM check completeness» — асинхронно:
-- MCI не ждёт проверку, ошибка проверки или запуска на шаг 13 не влияет. Таблицы I_MCI_CHECK* не трогаются.
-- Слепок текущего текста выводит sp_helptext (по строке текста на строку результата — переводы строк не теряются;
-- строки длиннее 255 символов он переносит): сохранить его до замены; откат — раздел 8.1.
-- Раздел закреплён за базой mci_model: в другой базе — ошибка 50007, ничего не меняется.
-- Владельцу задания MCI Regular update нужно право запускать чужое задание (sysadmin или SQLAgentOperatorRole);
-- без него шаг 13 пройдёт молча, проверку подхватит расписание задания по часам.
-- Замена выполняется, только если история заполнена и оценена в этом же запуске файла (разделы 3–4) и задание проверки
-- существует (раздел 5).
IF DB_NAME() <> N'mci_model'
    THROW 50007, N'Раздел 6: выберите базу mci_model (USE mci_model;) и запустите файл снова', 1;
EXEC sp_helptext N'dbo.check_data';
IF ISNULL(CAST(SESSION_CONTEXT(N'cc_history_ok') AS int), 0) = 0
   OR NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WITH (NOLOCK) WHERE name = N'CVM check completeness')
    RAISERROR(N'Раздел 6: старая сверка не заменена — история в этом запуске не заполнена или не оценена, либо задания нет (см. сообщения разделов 3–5)', 0, 1) WITH NOWAIT;
ELSE
    EXEC (N'CREATE OR ALTER PROCEDURE dbo.check_data
AS
SET NOCOUNT ON;
BEGIN TRY
    IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WITH (NOLOCK) WHERE name = N''CVM check completeness'' AND enabled = 1)
        EXEC msdb.dbo.sp_start_job @job_name = N''CVM check completeness'';
END TRY
BEGIN CATCH
    -- задание уже идёт, выключено или нет прав — проверку подхватит расписание по часам
    PRINT CONCAT(N''check_data: задание «CVM check completeness» не запущено: '', ERROR_MESSAGE());
END CATCH');
GO

-- ============================== 7. ВКЛЮЧЕНИЕ ЗАДАНИЯ ==============================
-- только если история заполнена и оценена в этом запуске файла и check_data уже новая; флаг читается и сразу сбрасывается
DECLARE @rc int, @ok int = ISNULL(CAST(SESSION_CONTEXT(N'cc_history_ok') AS int), 0);
EXEC sp_set_session_context @key = N'cc_history_ok', @value = 0;
IF @ok = 1
   AND OBJECT_DEFINITION(OBJECT_ID(N'dbo.check_data')) LIKE N'%CVM check completeness%'
   AND EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WITH (NOLOCK) WHERE name = N'CVM check completeness')
BEGIN
    EXEC @rc = msdb.dbo.sp_update_job @job_name = N'CVM check completeness', @enabled = 1;
    IF @rc <> 0
        THROW 50008, N'Раздел 7: задание «CVM check completeness» не включено (sp_update_job вернул ошибку)', 1;
    RAISERROR(N'Готово: история заполнена и оценена, старая сверка заменена, задание «CVM check completeness» включено — письма со следующей загрузки', 0, 1) WITH NOWAIT;
END
ELSE
    RAISERROR(N'Задание «CVM check completeness» не включено — история в этом запуске не заполнена или не оценена, check_data не заменена или задания нет (см. сообщения разделов 3–6)', 0, 1) WITH NOWAIT;
GO

-- ============================== 8. ОТКАТ (не запускать без решения) ==============================
-- Два независимых блока: раскомментировать и запускать каждый отдельно.
/*
USE mci_model;
IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WITH (NOLOCK) WHERE name = N'CVM check completeness')
    EXEC msdb.dbo.sp_delete_job @job_name = N'CVM check completeness';
DROP PROCEDURE IF EXISTS dbo.check_completeness_daily, dbo.check_completeness_eval, dbo.check_completeness_fill,
                         dbo.check_completeness_mci_busy;
DROP TABLE IF EXISTS dbo.I_CHECK_COMPLETENESS_LOG, dbo.I_CHECK_COMPLETENESS_SHOP, dbo.I_CHECK_COMPLETENESS_ORG,
                     dbo.I_CHECK_COMPLETENESS_DAY, dbo.I_CHECK_COMPLETENESS;
*/

-- 8.1 Откат dbo.check_data к старой сверке: текст из выгрузки структуры MCI 16.09.2026. Если слепок (sp_helptext)
-- из раздела 6 отличается от этого текста — откатывать слепком. Запускать в окне, где выбрана mci_model (сначала
-- отдельно USE mci_model;): CREATE OR ALTER должен быть первым в батче.
/*
CREATE OR ALTER PROCEDURE [dbo].[check_data]
as
set nocount on;
set quoted_identifier on;


drop table if exists #c

select ID_COMPANY
		, RN = ROW_NUMBER() over (order by ID_COMPANY)
into #c
from I_ORGANIZATION
group by  ID_COMPANY

DECLARE @c int = 1
DECLARE @idc int = (select ID_COMPANY from #c where RN=@c)

WHILE @c<= (select max(RN) from #c)

BEGIN

IF (select count(distinct YEAR_MONTH) from I_CHECK_DELTA (nolock) where ID_COMPANY=@idc)>=2

BEGIN

INSERT INTO I_MCI_CHECK
select	REFERENCE_DATE = getdate()
		, a.ID_COMPANY
		, COMPANY
		, a.ID_ORGANIZATION
		, ORGANIZATION
		, YEAR_MONTH
		, COST_DISCOUNT=sum(COST_DISCOUNT)
		, COUNT_CONTACT = count(distinct ID_CONTACT)
		, COUNT_CHECK = count(ID_CHECK)
		, CARD_DISCOUNT = sum(DISCOUNT)
--into I_MCI_CHECK
from I_CHECKHEADER as a(nolock)
		inner join I_ORGANIZATION as b (nolock)
		on a.ID_ORGANIZATION=b.ID_ORGANIZATION
		and a.ID_COMPANY=b.ID_COMPANY
where a.ID_COMPANY=@idc 
		and YEAR_MONTH in (select YEAR_MONTH from I_CHECK_DELTA (nolock) group by YEAR_MONTH)
group by  a.ID_COMPANY
		, COMPANY
		, a.ID_ORGANIZATION
		, ORGANIZATION
		, YEAR_MONTH

INSERT INTO I_MCI_CHECK_DATA
select	REFERENCE_DATE = getdate()
		, a.ID_COMPANY
		, COMPANY
		, a.ID_ORGANIZATION
		, ORGANIZATION
		, YEAR_MONTH
		, DATA
		, COST_DISCOUNT=sum(COST_DISCOUNT)
		, COUNT_CONTACT = count(distinct ID_CONTACT)
		, COUNT_CHECK = count(ID_CHECK)
		, CARD_DISCOUNT = sum(DISCOUNT)
--into I_MCI_CHECK_DATA
from I_CHECKHEADER as a(nolock)
		inner join I_ORGANIZATION as b (nolock)
		on a.ID_ORGANIZATION=b.ID_ORGANIZATION
		and a.ID_COMPANY=b.ID_COMPANY
where a.ID_COMPANY=@idc 
		and YEAR_MONTH in (select YEAR_MONTH from I_CHECK_DELTA (nolock) group by YEAR_MONTH)
group by  a.ID_COMPANY
		, COMPANY
		, a.ID_ORGANIZATION
		, ORGANIZATION
		, YEAR_MONTH
		, DATA
order by a.ID_COMPANY, a.ID_ORGANIZATION, DATA

END


set @c=@c+1
set @idc = (select ID_COMPANY from #c where RN=@c)

END
*/
