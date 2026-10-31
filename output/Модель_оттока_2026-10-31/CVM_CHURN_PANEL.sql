-- CVM · модель оттока, Т2: панель 10 % клиентов для обучения — список, журнал партий, процедура нарезки и первая партия
-- Елена 01.11.2026: «ок а только нужно выбрать правильно случайно», «как глобальную кг выделяем также 10%».
-- Бэклог: output/Модель_оттока_2026-10-31/бэклог_задач.txt, Т2 «ПОДВЫБОРКА 10 % — КАК ГЛОБАЛЬНАЯ КГ».
--
-- Что делает скрипт (по объектам):
--   I_CVM_CHURN_PANEL_BATCH   — новая таблица: журнал партий (одна строка на нарезку: диапазон первой покупки, клиентов,
--                               в панели, когорт, не сошедшихся когорт).
--   I_CVM_CHURN_PANEL_COHORT  — новая таблица: итог нарезки по когортам каждой партии (клиентов, в панели, попыток,
--                               расхождение средних бюджета и LTV панели и остальных).
--   I_CVM_CHURN_PANEL         — новая таблица: клиенты панели (≈ 10 % всех клиентов компании 1 с покупкой); клиент
--                               попадает в неё один раз и больше не меняется. Одна панель для всех таблиц и срезов модели
--                               оттока, в обеих организациях; ГКГ не исключается.
--   dbo.CVM_CHURN_PANEL_DRAW  — новая процедура: нарезка партии методом dbo.NEW_FOR_GLOBAL_CG (ГКГ):
--                               кто разыгрывается — клиенты компании 1 (ID_CONTACT <> 0, организации 1 и 3), у которых
--                               первая покупка попала в диапазон партии; профиль — за всю историю по последний день данных;
--                               когорты по месяцу последней покупки; внутри когорты сортировка LTV_100, BUDGET_10,
--                               MIN_WEEKNUMBER по убыванию, MAX_WEEKNUMBER; блоки по 10 подряд, из блока 1 случайный —
--                               у каждого клиента вероятность 10 %; лучший из 200 наборов по расхождению средних бюджета
--                               и LTV панели и остальных, остановка раньше при расхождении < 0,05 % по бюджету или
--                               < 0,07 % по LTV (как в ГКГ; бюджет и LTV здесь одна сумма — COST_DISCOUNT за всю историю).
--                               Первая партия — все клиенты с первой покупкой по «последний день данных − 10» (вне
--                               перезаливки последних 10 дней); дальше — донарезка
--                               новых клиентов раз в месяц (первая покупка со дня после прошлой партии по конец последнего
--                               полного месяца), её будет вызывать недельный шаг модели (Т2/Т4, после истории).
--                               Запись панели, партии и когорт — одной транзакцией.
--   Первая партия — в конце скрипта (если её ещё нет).
-- Ничего не удаляется и не меняется в существующих таблицах; I_CONTACT и I_GLOBAL_CG не трогаются.
-- Нагрузка: один проход по I_CHECKHEADER с группировкой по клиенту (колоночная, 1,77 млрд строк, ≈ 5–10 мин при двух потоках
-- batch mode) + нарезка когорт во временных таблицах (оценка 10–20 мин; крупнейшая когорта ≈ 7–8 млн — сортировка со сбросом в tempdb).
--
-- Скрипт для задания CVM_API_nightly (обменник; раннер — sqlcmd -b, ошибка уровня 16 = неуспех, повтор через 10 минут),
-- идемпотентный: таблицы и процедура создаются, только если их нет; первая партия — только если журнал пуст.
-- Стоп (ошибка 16, без изменений): нет нужных колонок в I_CHECKHEADER.

SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET NOCOUNT ON
SET XACT_ABORT ON
SET LOCK_TIMEOUT 60000
GO

USE mci_model;
GO

-- ============================== 0. СТОП ==============================
IF COL_LENGTH('dbo.I_CHECKHEADER', 'ID_CONTACT') IS NULL OR COL_LENGTH('dbo.I_CHECKHEADER', 'DATA') IS NULL
   OR COL_LENGTH('dbo.I_CHECKHEADER', 'COST_DISCOUNT') IS NULL OR COL_LENGTH('dbo.I_CHECKHEADER', 'WEEKNUMBER') IS NULL
   OR COL_LENGTH('dbo.I_CHECKHEADER', 'YEAR_MONTH') IS NULL OR COL_LENGTH('dbo.I_CHECKHEADER', 'ID_ORGANIZATION') IS NULL
   OR COL_LENGTH('dbo.I_CHECKHEADER', 'ID_COMPANY') IS NULL
BEGIN
    RAISERROR(N'СТОП: в I_CHECKHEADER нет нужных колонок — ничего не создаю.', 16, 1);
    SET NOEXEC ON;
END
GO

-- ============================== 1. ТАБЛИЦЫ ==============================
IF OBJECT_ID('dbo.I_CVM_CHURN_PANEL_BATCH') IS NULL
    CREATE TABLE dbo.I_CVM_CHURN_PANEL_BATCH (
          BATCH_ID       int            NOT NULL PRIMARY KEY
        , ID_COMPANY     int            NOT NULL
        , DRAW_DT        datetime2(0)   NOT NULL
        , FIRST_FROM     date           NULL          -- первая покупка с (NULL — с начала данных)
        , FIRST_TO       date           NOT NULL      -- первая покупка по
        , DATA_TO        date           NOT NULL      -- профиль клиентов — покупки по этот день
        , CLIENTS        int            NOT NULL      -- разыграно клиентов
        , PANEL_CLIENTS  int            NOT NULL      -- из них в панели
        , COHORTS        int            NOT NULL
        , NOT_CONVERGED  int            NOT NULL      -- когорт, где за предел попыток взят лучший набор
        , NOTE           nvarchar(200)  NULL
    );
GO

IF OBJECT_ID('dbo.I_CVM_CHURN_PANEL_COHORT') IS NULL
    CREATE TABLE dbo.I_CVM_CHURN_PANEL_COHORT (
          BATCH_ID       int            NOT NULL
        , COHORT_YM      int            NOT NULL      -- месяц последней покупки
        , CLIENTS        int            NOT NULL
        , PANEL_CLIENTS  int            NOT NULL
        , TRIES          int            NOT NULL
        , ERR_BUDGET     float          NULL          -- |среднее остальных − среднее панели| / среднее остальных
        , ERR_LTV        float          NULL
        , CONVERGED      bit            NOT NULL      -- 0 — за предел попыток взят лучший набор
        , PRIMARY KEY (BATCH_ID, COHORT_YM)
    );
GO

IF OBJECT_ID('dbo.I_CVM_CHURN_PANEL') IS NULL
BEGIN
    CREATE TABLE dbo.I_CVM_CHURN_PANEL (
          ID_COMPANY     int            NOT NULL
        , ID_CONTACT     int            NOT NULL
        , BATCH_ID       int            NOT NULL
        , COHORT_YM      int            NOT NULL
    );
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_CVM_CHURN_PANEL ON dbo.I_CVM_CHURN_PANEL;
END
GO

-- ============================== 2. ПРОЦЕДУРА НАРЕЗКИ ==============================
CREATE OR ALTER PROCEDURE dbo.CVM_CHURN_PANEL_DRAW
      @FIRST_TO   date = NULL   -- первая покупка по; NULL — первая партия: последний день данных, донарезка: конец последнего полного месяца
    , @max_tries  int  = 200    -- лучший из 200 наборов на когорту (как ГКГ, решение 13.10.2026)
    , @block      int  = 10     -- блок 10 → 10 % клиентов
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @idc int = 1;

    -- последний день данных (свежие группы строк; если за 40 дней чеков нет — полный максимум)
    DECLARE @last date = (SELECT max(DATA) FROM dbo.I_CHECKHEADER (nolock)
                          WHERE ID_COMPANY = @idc AND DATA >= dateadd(day, -40, cast(getdate() AS date)));
    IF @last IS NULL
        SET @last = (SELECT max(DATA) FROM dbo.I_CHECKHEADER (nolock) WHERE ID_COMPANY = @idc);

    DECLARE @prev_to date = (SELECT max(FIRST_TO) FROM dbo.I_CVM_CHURN_PANEL_BATCH (nolock) WHERE ID_COMPANY = @idc);
    DECLARE @first_from date = dateadd(day, 1, @prev_to);       -- NULL у первой партии — с начала данных
    SET @FIRST_TO = isnull(@FIRST_TO, IIF(@prev_to IS NULL, @last,
                                          IIF(@last = eomonth(@last), @last, eomonth(dateadd(month, -1, @last)))));

    IF @first_from > @FIRST_TO
    BEGIN
        PRINT N'CVM_CHURN_PANEL_DRAW: резать нечего — последняя партия покрывает первые покупки по ' + convert(nvarchar(10), @prev_to, 104);
        RETURN;
    END
    IF @FIRST_TO > @last
        THROW 50040, N'CVM_CHURN_PANEL_DRAW: конец диапазона первой покупки позже последнего дня данных', 1;

    -- 1. Клиенты диапазона: профиль за всю историю по последний день данных (как #tu_first в NEW_FOR_GLOBAL_CG)
    DROP TABLE IF EXISTS #pf;
    SELECT a.ID_CONTACT
         , MAX_YM         = max(a.YEAR_MONTH)
         , LTV            = sum(a.COST_DISCOUNT)
         , LTV_100        = round(sum(a.COST_DISCOUNT) / 100, 0) * 100
         , BUDGET_10      = round(sum(a.COST_DISCOUNT) / 10, 0) * 10
         , MIN_WEEKNUMBER = min(a.WEEKNUMBER)
         , MAX_WEEKNUMBER = max(a.WEEKNUMBER)
    INTO #pf
    FROM dbo.I_CHECKHEADER AS a (nolock)
    WHERE a.ID_COMPANY = @idc
      AND a.ID_ORGANIZATION IN (1, 3)
      AND a.ID_CONTACT <> 0
      AND a.DATA <= @last
    GROUP BY a.ID_CONTACT
    HAVING min(a.DATA) >= isnull(@first_from, '19000101')
       AND min(a.DATA) <= @FIRST_TO
    OPTION (RECOMPILE);
    CREATE CLUSTERED COLUMNSTORE INDEX cci_pf ON #pf;

    IF NOT EXISTS (SELECT 1 FROM #pf)
    BEGIN
        PRINT N'CVM_CHURN_PANEL_DRAW: новых клиентов в диапазоне нет — партия не заводится';
        RETURN;
    END

    -- клиент разыгрывается один раз: клиенты диапазона уже в панели = диапазон резали в обход процедуры
    DECLARE @in_panel int = (SELECT count(*) FROM #pf AS f
                             WHERE EXISTS (SELECT 1 FROM dbo.I_CVM_CHURN_PANEL AS p (nolock)
                                           WHERE p.ID_COMPANY = @idc AND p.ID_CONTACT = f.ID_CONTACT));
    IF @in_panel > 0
    BEGIN
        DECLARE @msg nvarchar(400) = concat(N'CVM_CHURN_PANEL_DRAW: ', @in_panel, N' клиентов диапазона уже в панели — нарезка не делается, нужно решение');
        THROW 50041, @msg, 1;
    END

    DROP TABLE IF EXISTS #new;
    CREATE TABLE #new (ID_CONTACT int NOT NULL, COHORT_YM int NOT NULL, INDEX cci_new CLUSTERED COLUMNSTORE);   -- до 1,5 млн строк
    DROP TABLE IF EXISTS #log;
    CREATE TABLE #log (COHORT_YM int NOT NULL, CLIENTS int NOT NULL, PANEL_CLIENTS int NOT NULL, TRIES int NOT NULL,
                       ERR_BUDGET float NULL, ERR_LTV float NULL, CONVERGED bit NOT NULL);

    -- 2. Нарезка по когортам (месяц последней покупки)
    DROP TABLE IF EXISTS #ym;
    SELECT MAX_YM, RN = row_number() OVER (ORDER BY MAX_YM DESC)
    INTO #ym
    FROM #pf
    GROUP BY MAX_YM;

    DECLARE @i int = 1, @ym int, @error float, @error1 float, @tries int, @best float, @maxerr float, @conv bit, @n int
          , @pc int, @s_all float, @s_cg float, @n_cg int, @m_o float, @m_c float;
    SET @ym = (SELECT MAX_YM FROM #ym WHERE RN = @i);

    WHILE @ym IS NOT NULL
    BEGIN
        -- порядок в когорте — как в ГКГ; блок = (RN − 1) / 10, то есть ровно 10 подряд (последний блок может быть короче)
        -- номер считается один раз: при равных ключах два отдельных row_number могли бы разойтись
        DROP TABLE IF EXISTS #iv;
        SELECT ID_CONTACT, LTV, BLK = (RN - 1) / @block, POS = (RN - 1) % @block
        INTO #iv
        FROM (SELECT ID_CONTACT, LTV
                   , RN = row_number() OVER (ORDER BY LTV_100, BUDGET_10, MIN_WEEKNUMBER DESC, MAX_WEEKNUMBER)
              FROM #pf
              WHERE MAX_YM = @ym) AS r;
        CREATE CLUSTERED COLUMNSTORE INDEX cci_iv ON #iv;
        SELECT @n = count(*), @s_all = sum(cast(LTV AS float)) FROM #iv;

        DROP TABLE IF EXISTS #bs;   -- размер блока (последний может быть короче)
        SELECT BLK, CNT = count(*) INTO #bs FROM #iv GROUP BY BLK;

        SET @error = 1.0; SET @error1 = 1.0; SET @tries = 0; SET @best = 999;
        DROP TABLE IF EXISTS #best;
        CREATE TABLE #best (ID_CONTACT int NOT NULL, INDEX cci_best CLUSTERED COLUMNSTORE);

        WHILE @error >= 0.0005
          AND @error1 >= 0.0007
          AND (@max_tries IS NULL OR @tries < @max_tries)
        BEGIN
            SET @tries += 1;

            -- 1 случайный из каждого блока: случайная позиция 0…9 (равновероятно), без сортировки всей когорты; в коротком
            -- последнем блоке позиция за его концом = никого — так у КАЖДОГО клиента ровно 10 % (Елена: «выбрать правильно случайно»)
            DROP TABLE IF EXISTS #pick;
            SELECT b.BLK, POS = (checksum(newid()) & 2147483647) % @block   -- не abs(): abs(−2147483648) — переполнение
            INTO #pick
            FROM #bs AS b;

            DROP TABLE IF EXISTS #cg;
            SELECT v.ID_CONTACT, v.LTV
            INTO #cg
            FROM #iv AS v
                 JOIN #pick AS k ON k.BLK = v.BLK AND k.POS = v.POS;

            -- средние панели и остальных: сумма всех известна, остальные = все − панель (один проход по #iv вместо двух)
            SELECT @s_cg = sum(cast(LTV AS float)), @n_cg = count(*) FROM #cg;

            -- когорта из одного короткого блока: в панели никого или все — сравнивать не с чем, набор принимается
            IF @n_cg = 0 OR @n_cg >= @n
            BEGIN
                SET @error = 0; SET @error1 = 0;
            END
            ELSE
            BEGIN
                SET @m_o = (@s_all - @s_cg) / (@n - @n_cg);
                SET @m_c = @s_cg / @n_cg;
                SET @error  = abs(@m_o - @m_c) / nullif(abs(@m_o), 0);
                SET @error1 = @error;               -- бюджет и LTV здесь одна величина
                SET @error  = isnull(@error, 0);    -- среднее остальных = 0: относительное расхождение не определено, набор принимается
                SET @error1 = isnull(@error1, 0);
            END

            SET @maxerr = IIF(@error > @error1, @error, @error1);
            IF @maxerr < @best
            BEGIN
                SET @best = @maxerr;
                TRUNCATE TABLE #best;
                INSERT INTO #best (ID_CONTACT) SELECT ID_CONTACT FROM #cg;
            END
        END

        SET @conv = IIF(@error >= 0.0005 AND @error1 >= 0.0007, 0, 1);

        IF @conv = 1
        BEGIN
            INSERT INTO #new (ID_CONTACT, COHORT_YM) SELECT ID_CONTACT, @ym FROM #cg;
            SET @pc = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            INSERT INTO #new (ID_CONTACT, COHORT_YM) SELECT ID_CONTACT, @ym FROM #best;
            SET @pc = @@ROWCOUNT;
            -- расхождение лучшего набора — в журнал
            SET @error = @best; SET @error1 = @best;
        END

        INSERT INTO #log (COHORT_YM, CLIENTS, PANEL_CLIENTS, TRIES, ERR_BUDGET, ERR_LTV, CONVERGED)
        SELECT @ym, @n, @pc, @tries, @error, @error1, @conv;

        RAISERROR(N'CVM_CHURN_PANEL_DRAW: когорта %d готова, попыток: %d', 0, 1, @ym, @tries) WITH NOWAIT;

        SET @i += 1;
        SET @ym = (SELECT MAX_YM FROM #ym WHERE RN = @i);
    END

    -- 3. Запись панели, партии и когорт — одной транзакцией
    DECLARE @batch int = isnull((SELECT max(BATCH_ID) FROM dbo.I_CVM_CHURN_PANEL_BATCH (nolock)), 0) + 1;
    DECLARE @note nvarchar(200) = IIF(@prev_to IS NULL, N'первая партия: все клиенты с покупкой по ', N'донарезка: первая покупка '
                                       + convert(nvarchar(10), @first_from, 104) + N'–')
                                  + convert(nvarchar(10), @FIRST_TO, 104);
    BEGIN TRANSACTION;

    INSERT INTO dbo.I_CVM_CHURN_PANEL WITH (TABLOCK) (ID_COMPANY, ID_CONTACT, BATCH_ID, COHORT_YM)
    SELECT @idc, ID_CONTACT, @batch, COHORT_YM
    FROM #new;

    INSERT INTO dbo.I_CVM_CHURN_PANEL_COHORT (BATCH_ID, COHORT_YM, CLIENTS, PANEL_CLIENTS, TRIES, ERR_BUDGET, ERR_LTV, CONVERGED)
    SELECT @batch, COHORT_YM, CLIENTS, PANEL_CLIENTS, TRIES, ERR_BUDGET, ERR_LTV, CONVERGED
    FROM #log;

    INSERT INTO dbo.I_CVM_CHURN_PANEL_BATCH (BATCH_ID, ID_COMPANY, DRAW_DT, FIRST_FROM, FIRST_TO, DATA_TO, CLIENTS, PANEL_CLIENTS,
                                             COHORTS, NOT_CONVERGED, NOTE)
    SELECT @batch, @idc, sysdatetime(), @first_from, @FIRST_TO, @last
         , (SELECT count(*) FROM #pf), (SELECT count(*) FROM #new)
         , (SELECT count(*) FROM #log), (SELECT count(*) FROM #log WHERE CONVERGED = 0), @note;

    COMMIT TRANSACTION;

    SELECT [Партия] = @batch, [Клиентов] = (SELECT count(*) FROM #pf), [В панели] = (SELECT count(*) FROM #new)
         , [Доля, %] = cast(100.0 * (SELECT count(*) FROM #new) / nullif((SELECT count(*) FROM #pf), 0) AS decimal(8, 3))
         , [Когорт] = (SELECT count(*) FROM #log), [Не сошлось] = (SELECT count(*) FROM #log WHERE CONVERGED = 0)
         , [Макс. расхождение, %] = cast(100.0 * (SELECT max(IIF(ERR_BUDGET > ERR_LTV, ERR_BUDGET, ERR_LTV)) FROM #log) AS decimal(18, 4));
END
GO

-- ============================== 3. ПЕРВАЯ ПАРТИЯ ==============================
-- замок на время нарезки: обменник повторяет скрипт каждые 10 минут, а нарезка идёт дольше — без замка второй запуск
-- увидел бы пустой журнал и записал панель второй раз (проверка «уже в панели» до записи не атомарна). Замок сессии
-- снимается при завершении sqlcmd, в том числе при ошибке.
DECLARE @rc int;
EXEC @rc = sp_getapplock @Resource = N'CVM_CHURN_PANEL_DRAW', @LockMode = N'Exclusive', @LockOwner = N'Session', @LockTimeout = 0;
IF @rc < 0
    THROW 50042, N'CVM_CHURN_PANEL: нарезка уже идёт в другой сессии — повтор позже', 1;
-- первая партия — клиенты с первой покупкой по «последний день данных − 10»: обменник может идти во время перезаливки
-- последних 10 дней чеков, и клиент с первой покупкой в эти дни, не попав в партию, больше никогда бы не разыгрывался;
-- их возьмёт месячная донарезка (с дня после этой партии)
DECLARE @ft date = (SELECT dateadd(day, -10, max(DATA)) FROM dbo.I_CHECKHEADER (nolock)
                    WHERE ID_COMPANY = 1 AND DATA >= dateadd(day, -40, cast(getdate() AS date)));
IF NOT EXISTS (SELECT 1 FROM dbo.I_CVM_CHURN_PANEL_BATCH (nolock) WHERE ID_COMPANY = 1)
    EXEC dbo.CVM_CHURN_PANEL_DRAW @FIRST_TO = @ft, @max_tries = 200;
EXEC sp_releaseapplock @Resource = N'CVM_CHURN_PANEL_DRAW', @LockOwner = N'Session';
GO

SELECT [Партия] = BATCH_ID, [Нарезка] = DRAW_DT, [Первая покупка с] = FIRST_FROM, [по] = FIRST_TO, [Данные по] = DATA_TO
     , [Клиентов] = CLIENTS, [В панели] = PANEL_CLIENTS, [Когорт] = COHORTS, [Не сошлось] = NOT_CONVERGED, [Примечание] = NOTE
FROM dbo.I_CVM_CHURN_PANEL_BATCH (nolock)
ORDER BY BATCH_ID;
GO
