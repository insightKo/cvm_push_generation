-- ============================================================================================
-- CVM_GCG_REPORT — эффект глобальной контрольной группы (ГКГ): процедуры + расчёт июля–октября.
-- Запустить целиком в SSMS (F5). Июль–октябрь записываются в витрину I_GCG_REPORT и выводятся из неё.
--
-- Что меняется (решения 13.10.2026):
--   1. «ДО» для коэффициента — фиксированное, до нарезки ГКГ. Считается ОДИН раз и хранится на клиента
--      в таблице dbo.I_GCG_PRE:
--        Активные          — оборот 20.05–30.06.2026 (6 недель до 01.07);
--        Отток со спящими  — оборот за всю историю до 01.07.2026.
--   2. В расчёт входят только клиенты, разыгранные в ГКГ. Июльская нарезка — клиенты с первой покупкой
--      по 01.07.2026; пришедшие позже в расчёт не входят, пока их не разыграют.
--   3. Партии жеребьёвки — в журнале dbo.I_GCG_BATCH. Каждая партия считается со своей даты и отдельно
--      (своя ГКГ, свой коэффициент, свои границы выбросов), Доп ТО и ошибка складываются по партиям.
--      Сейчас партия одна — июльская. Догонку (с 14.10) и ежемесячные донарезки заводит процедура
--      dbo.gcg_batch_add — сразу после того, как нарезка записана в I_GLOBAL_CG.
--   4. Сегмент фиксируется на дату нарезки партии и дальше не меняется: Активные — покупка в 6 недель до нарезки
--      (то же окно, что «ДО» активных: для июльской партии 20.05–30.06.2026), Отток со спящими — остальные.
--      Зафиксированные когорты ведутся во всех следующих месяцах; у новой партии — своя дата нарезки.
--      Партия считается целиком: отдельного исключения новых нет — не разыгранные клиенты в расчёт не входят
--      и так (июльская партия — первая покупка по 01.07.2026; пришедшие с 02.07 — только с догонки 14.10).
--   5. Доп ТО по слоям давности на дату нарезки — основной показатель (как в отчёте эффекта акций):
--      Активные — недели с последней покупки до нарезки (1–6), Отток со спящими — месяц последней покупки
--      (по этим когортам нарезали ГКГ). Соседние слои сливаются в ячейку, пока в ней нет обеих групп.
--      В каждой ячейке свой KOEF = ТО ГКГ / ТО ГКГ ДО и Доп ТО = ТО ЦГ − ТО ЦГ ДО × KOEF; Доп ТО ячеек складывается,
--      ошибки — квадратами. Без слоёв и простая разница — справочно (в выводе @debug).
--      В оценку входят только клиенты с «ДО» > 0; ТО — после скидки (для ГКГ — как было).
--   6. Выбросы — правило κ = ½, как в отчёте эффекта акций (13.10.2026), вместо границ Тьюки; клиент-выброс
--      исключается из расчёта целиком (и из «ДО», и из месяца), оценка считается на оставшихся.
--      Граница ячейки (партия × сегмент × слой): G = exp(m + z·s), m — медиана логарифма трат, s = (P75 логарифма − m) / 0,674
--      (разброс по правому хвосту), z — из условия n·(1 − Φ(z)) = ½: за границей в среднем меньше половины клиента.
--      «ДО» (до нарезки): по ЦГ и ГКГ вместе, среди трат > 0; клиент с «ДО» выше G или с отрицательным «ДО»
--      исключается из расчёта целиком.
--      Месяц расчёта («во время») — отдельная граница, считается после исключения выбросов «ДО»: m и s — по покупателям
--      ГКГ ячейки, n — все покупатели ячейки; клиент с |суммой за месяц| выше G исключается целиком, одинаково для ЦГ и ГКГ
--      (@Y_MODE = 'CAP' — вместо исключения обрезать сумму до границы). Если в ГКГ ячейки меньше 4 покупателей —
--      месяц в ячейке не обрабатывается.
--
-- Нагрузка: партия 1 (июльская) перезаводится с давностью — один проход по истории I_CHECKHEADER до 01.07.2026
-- включительно, один раз (повторный запуск скрипта её не трогает); затем июль, август и сентябрь — чеки каждого месяца.
-- Ночной шаг дальше читает только чеки месяца. Ночной шаг 49 (exec dbo.gcg_report;) не меняется.
-- ============================================================================================

USE mci_model
GO

SET NOCOUNT ON;

-- ============================== 1. КОЛОНКИ ВИТРИНЫ (идемпотентно) ==============================
IF COL_LENGTH(N'dbo.I_GCG_REPORT', N'KOEF')          IS NULL ALTER TABLE dbo.I_GCG_REPORT ADD [KOEF] float NULL;
IF COL_LENGTH(N'dbo.I_GCG_REPORT', N'BUDGET')        IS NULL ALTER TABLE dbo.I_GCG_REPORT ADD [BUDGET] decimal(18,6) NULL;
IF COL_LENGTH(N'dbo.I_GCG_REPORT', N'BUDGET_CG')     IS NULL ALTER TABLE dbo.I_GCG_REPORT ADD [BUDGET_CG] decimal(18,6) NULL;
IF COL_LENGTH(N'dbo.I_GCG_REPORT', N'BUDGET_bef')    IS NULL ALTER TABLE dbo.I_GCG_REPORT ADD [BUDGET_bef] decimal(18,6) NULL;
IF COL_LENGTH(N'dbo.I_GCG_REPORT', N'BUDGET_CG_bef') IS NULL ALTER TABLE dbo.I_GCG_REPORT ADD [BUDGET_CG_bef] decimal(18,6) NULL;
IF COL_LENGTH(N'dbo.I_GCG_REPORT', N'SE')            IS NULL ALTER TABLE dbo.I_GCG_REPORT ADD [SE] float NULL;
IF COL_LENGTH(N'dbo.I_GCG_REPORT', N'ДИ 95% низ')    IS NULL ALTER TABLE dbo.I_GCG_REPORT ADD [ДИ 95% низ] decimal(18,2) NULL;
IF COL_LENGTH(N'dbo.I_GCG_REPORT', N'ДИ 95% верх')   IS NULL ALTER TABLE dbo.I_GCG_REPORT ADD [ДИ 95% верх] decimal(18,2) NULL;
IF COL_LENGTH(N'dbo.I_GCG_REPORT', N't')             IS NULL ALTER TABLE dbo.I_GCG_REPORT ADD [t] float NULL;
IF COL_LENGTH(N'dbo.I_GCG_REPORT', N'Значимо')       IS NULL ALTER TABLE dbo.I_GCG_REPORT ADD [Значимо] nvarchar(3) NULL;
IF COL_LENGTH(N'dbo.I_GCG_REPORT', N'Данные с')      IS NULL ALTER TABLE dbo.I_GCG_REPORT ADD [Данные с] date NULL;
IF COL_LENGTH(N'dbo.I_GCG_REPORT', N'Данные по')     IS NULL ALTER TABLE dbo.I_GCG_REPORT ADD [Данные по] date NULL;
IF COL_LENGTH(N'dbo.I_GCG_REPORT', N'Полный месяц')  IS NULL ALTER TABLE dbo.I_GCG_REPORT ADD [Полный месяц] bit NULL;
IF COL_LENGTH(N'dbo.I_GCG_REPORT', N'Окно ДО оттока, мес') IS NULL ALTER TABLE dbo.I_GCG_REPORT ADD [Окно ДО оттока, мес] int NULL;
IF COL_LENGTH(N'dbo.I_GCG_REPORT', N'Дата расчёта')  IS NULL ALTER TABLE dbo.I_GCG_REPORT ADD [Дата расчёта] datetime2(0) NULL;
IF COL_LENGTH(N'dbo.I_GCG_REPORT', N'Источник')      IS NULL ALTER TABLE dbo.I_GCG_REPORT ADD [Источник] nvarchar(100) NULL;
GO

-- ============================== 2. ЖУРНАЛ ПАРТИЙ И ЗАФИКСИРОВАННОЕ «ДО» ==============================
-- Журнал партий жеребьёвки: одна строка = одна нарезка или донарезка ГКГ (служебная, несколько строк).
IF OBJECT_ID(N'dbo.I_GCG_BATCH', N'U') IS NULL
    CREATE TABLE dbo.I_GCG_BATCH
    (
          BATCH_ID    int           NOT NULL
        , DRAW_DATE   date          NOT NULL   -- с этой даты партия входит в расчёт эффекта
        , FIRST_FROM  date          NULL       -- первая покупка клиента с (NULL — без нижней границы)
        , FIRST_TO    date          NOT NULL   -- первая покупка клиента по, включительно
        , PRE_TO      date          NOT NULL   -- «ДО»: покупки строго до этой даты
        , CLIENTS     int           NULL       -- клиентов в партии
        , GCG_CLIENTS int           NULL       -- из них в ГКГ
        , NOTE        nvarchar(200) NULL
        , CREATED     datetime2(0)  NOT NULL CONSTRAINT DF_I_GCG_BATCH_CREATED DEFAULT (sysdatetime())
        , CONSTRAINT PK_I_GCG_BATCH PRIMARY KEY (BATCH_ID)
    );

-- Состав партий: клиент, группа и «ДО» — фиксируются один раз при заведении партии и дальше не пересчитываются.
IF OBJECT_ID(N'dbo.I_GCG_PRE', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.I_GCG_PRE
    (
          BATCH_ID   int     NOT NULL
        , ID_CONTACT int     NOT NULL
        , GCG        tinyint NOT NULL   -- 1 = в ГКГ, 0 = ЦГ
        , SEGMENT    tinyint NULL       -- сегмент на дату нарезки: 2 — Активные, 5 — Отток со спящими
        , FIRST_DATA date    NOT NULL   -- первая покупка
        , LAST_DATA  date    NULL       -- последняя покупка до PRE_TO (давность на дату нарезки — слой)
        , X_ACT      float   NULL       -- оборот за 6 недель до PRE_TO («ДО» активных)
        , X_OTT      float   NULL       -- оборот за всю историю до PRE_TO («ДО» оттока со спящими)
    );
    CREATE CLUSTERED COLUMNSTORE INDEX CCI_I_GCG_PRE ON dbo.I_GCG_PRE;
END
IF COL_LENGTH(N'dbo.I_GCG_PRE', N'SEGMENT') IS NULL ALTER TABLE dbo.I_GCG_PRE ADD SEGMENT tinyint NULL;
IF COL_LENGTH(N'dbo.I_GCG_PRE', N'LAST_DATA') IS NULL ALTER TABLE dbo.I_GCG_PRE ADD LAST_DATA date NULL;
GO

-- ============================== 3. ПРОЦЕДУРА ЗАВЕДЕНИЯ ПАРТИИ ==============================
-- Вызывать СРАЗУ после того, как нарезка записана в I_GLOBAL_CG. Пример для догонки:
--   EXEC dbo.gcg_batch_add @BATCH_ID = 2, @DRAW_DATE = '20261014', @FIRST_FROM = '20260702', @FIRST_TO = '<последний день первой покупки в догонке>';
CREATE OR ALTER PROCEDURE dbo.gcg_batch_add
      @BATCH_ID   int
    , @DRAW_DATE  date                  -- с этой даты партия входит в расчёт (дата нарезки)
    , @FIRST_FROM date = NULL           -- первая покупка с (NULL — без нижней границы, только первая партия)
    , @FIRST_TO   date                  -- первая покупка по, включительно
    , @PRE_TO     date = NULL           -- «ДО»: покупки строго до этой даты; NULL = @DRAW_DATE
    , @NOTE       nvarchar(200) = NULL
    , @REPLACE    bit = 0               -- 1 = перезавести партию: старые строки удаляются и пишутся заново в одной транзакции
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @idc int = 1;
    SET @PRE_TO = isnull(@PRE_TO, @DRAW_DATE);
    DECLARE @act_from  date = dateadd(week, -6, @PRE_TO);                              -- «ДО» активных: 6 недель до PRE_TO
    DECLARE @first_end date = dateadd(day, 1, @FIRST_TO);                              -- граница первой покупки, НЕ включается
    DECLARE @scan_to   date = IIF(@PRE_TO > @first_end, @PRE_TO, @first_end);

    IF @PRE_TO > @DRAW_DATE
        THROW 50011, N'gcg_batch_add: «ДО» не может заходить за дату нарезки', 1;
    IF @REPLACE = 0 AND EXISTS (SELECT 1 FROM dbo.I_GCG_BATCH (nolock) WHERE BATCH_ID = @BATCH_ID)
        THROW 50012, N'gcg_batch_add: партия с таким номером уже заведена', 1;
    IF EXISTS (SELECT 1 FROM dbo.I_GCG_BATCH (nolock)
               WHERE BATCH_ID <> @BATCH_ID
                 AND isnull(FIRST_FROM, '19000101') <= @FIRST_TO
                 AND isnull(@FIRST_FROM, '19000101') <= FIRST_TO)
        THROW 50013, N'gcg_batch_add: диапазон первой покупки пересекается с уже заведённой партией', 1;

    -- клиенты партии (первая покупка в диапазоне) и их «ДО»
    DROP TABLE IF EXISTS #gb_cl;
    SELECT a.ID_CONTACT
         , FIRST_DATA = min(a.DATA)
         , LAST_DATA  = max(IIF(a.DATA < @PRE_TO, a.DATA, NULL))                                -- последняя покупка до нарезки
         , ACT_CHECKS = count(IIF(a.DATA >= @act_from AND a.DATA < @PRE_TO, a.ID_CHECK, NULL))   -- покупки в 6 недель до нарезки
         , X_ACT      = sum(IIF(a.DATA >= @act_from AND a.DATA < @PRE_TO, cast(a.COST_DISCOUNT AS float), NULL))
         , X_OTT      = sum(IIF(a.DATA < @PRE_TO, cast(a.COST_DISCOUNT AS float), NULL))
    INTO #gb_cl
    FROM dbo.I_CHECKHEADER AS a (nolock)
    WHERE a.ID_COMPANY = @idc
      AND a.ID_CONTACT <> 0
      AND a.DATA < @scan_to
    GROUP BY a.ID_CONTACT
    HAVING min(a.DATA) >= isnull(@FIRST_FROM, '19000101')
       AND min(a.DATA) <  @first_end
    OPTION (RECOMPILE);
    CREATE CLUSTERED COLUMNSTORE INDEX cci_gb_cl ON #gb_cl;

    -- ГКГ на момент заведения партии
    DROP TABLE IF EXISTS #gb_cg;
    SELECT ID_CONTACT
    INTO #gb_cg
    FROM dbo.I_GLOBAL_CG (nolock)
    WHERE ID_COMPANY = @idc AND CONTROL_GROUP = 1
    GROUP BY ID_CONTACT;
    CREATE CLUSTERED COLUMNSTORE INDEX cci_gb_cg ON #gb_cg;

    DECLARE @clients int, @gcg int;
    SELECT @clients = count(*), @gcg = count(g.ID_CONTACT)
    FROM #gb_cl AS c
         LEFT JOIN #gb_cg AS g ON g.ID_CONTACT = c.ID_CONTACT;

    IF isnull(@gcg, 0) = 0
        THROW 50014, N'gcg_batch_add: в ГКГ нет ни одного клиента партии — сначала нарезка в I_GLOBAL_CG, потом заведение партии', 1;

    BEGIN TRANSACTION;

    IF @REPLACE = 1
    BEGIN
        DELETE FROM dbo.I_GCG_PRE   WHERE BATCH_ID = @BATCH_ID;
        DELETE FROM dbo.I_GCG_BATCH WHERE BATCH_ID = @BATCH_ID;
    END

    INSERT INTO dbo.I_GCG_PRE (BATCH_ID, ID_CONTACT, GCG, SEGMENT, FIRST_DATA, LAST_DATA, X_ACT, X_OTT)
    SELECT @BATCH_ID, c.ID_CONTACT, IIF(g.ID_CONTACT IS NULL, 0, 1), IIF(c.ACT_CHECKS > 0, 2, 5), c.FIRST_DATA, c.LAST_DATA, c.X_ACT, c.X_OTT
    FROM #gb_cl AS c
         LEFT JOIN #gb_cg AS g ON g.ID_CONTACT = c.ID_CONTACT;

    INSERT INTO dbo.I_GCG_BATCH (BATCH_ID, DRAW_DATE, FIRST_FROM, FIRST_TO, PRE_TO, CLIENTS, GCG_CLIENTS, NOTE)
    VALUES (@BATCH_ID, @DRAW_DATE, @FIRST_FROM, @FIRST_TO, @PRE_TO, @clients, @gcg, @NOTE);

    COMMIT TRANSACTION;

    SELECT [Партия]       = @BATCH_ID
         , [Клиентов]     = @clients
         , [Из них в ГКГ] = @gcg
         , [Доля ГКГ, %]  = cast(100.0 * @gcg / nullif(@clients, 0) AS decimal(6, 3));
END
GO

-- ============================== 4. ПРОЦЕДУРА РАСЧЁТА ЭФФЕКТА ==============================
CREATE OR ALTER PROCEDURE dbo.gcg_report
      @YM            int = NULL   -- месяц YYYYMM; NULL = ночной режим (текущий + недосчитанные прошлые)
    , @debug         bit = 0      -- 1 = показать выбросы и расчёт по партиям
    , @REPLACE_FILES bit = 0      -- 1 = разрешить заменить месяц, строки которого пришли из файлов / от Манзаны
    , @Y_MODE        varchar(4) = 'DROP'  -- выбросы месяца: DROP — исключить клиента целиком, CAP — обрезать сумму до границы
    , @WRITE         bit = 1      -- 0 = только показать результат месяца, витрину не менять
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @idc         int   = 1;
    DECLARE @kappa       float = 0.5;     -- выбросы: за границей в среднем меньше половины клиента слоя

    IF @Y_MODE IS NULL OR @Y_MODE NOT IN ('CAP', 'DROP')
        THROW 50007, N'gcg_report: @Y_MODE — только ''CAP'' или ''DROP''', 1;

    -- последний день, за который есть чеки. max(DATA) без нижней границы — полный скан I_CHECKHEADER (1,9 млрд строк);
    -- с границей 40 дней результат тот же, если за 40 дней чеки есть; иначе — прежний запрос
    DECLARE @last date = (SELECT max(DATA) FROM dbo.I_CHECKHEADER (nolock)
                          WHERE ID_COMPANY = @idc AND DATA >= dateadd(day, -40, cast(getdate() AS date)));
    IF @last IS NULL
        SET @last = (SELECT max(DATA) FROM dbo.I_CHECKHEADER (nolock) WHERE ID_COMPANY = @idc);
    DECLARE @ym_last int = year(@last) * 100 + month(@last);

    DECLARE @months TABLE (YM int PRIMARY KEY);
    IF @YM IS NOT NULL
    BEGIN
        -- история из файлов и строки Манзаны заменяются только явным флагом
        IF @REPLACE_FILES = 0 AND @WRITE = 1 AND EXISTS (SELECT 1 FROM dbo.I_GCG_REPORT (nolock)
                                          WHERE YEAR_MONTH = @YM AND isnull([Источник], N'') <> N'процедура gcg_report')
            THROW 50001, N'gcg_report: строки месяца пришли из файла или от Манзаны — для замены запустить с @REPLACE_FILES = 1', 1;
        INSERT INTO @months (YM) VALUES (@YM);
    END
    ELSE
    BEGIN
        INSERT INTO @months (YM) VALUES (@ym_last);
        INSERT INTO @months (YM)
        SELECT DISTINCT YEAR_MONTH
        FROM dbo.I_GCG_REPORT (nolock)
        WHERE [Полный месяц] = 0 AND YEAR_MONTH < @ym_last;
    END

    -- имя @cur_ym: на сервере имена переменных регистронезависимы, @ym совпало бы с параметром @YM.
    -- Временные таблицы — #gcg_*: процедура видит временные таблицы вызвавшей сессии SSMS.
    DECLARE @cur_ym int, @dt_from date, @dt_to date, @data_to date, @full bit, @scan_from date;
    DECLARE @lb int, @ls int, @lo int, @lnt int, @lnc int, @pb int, @ps int, @cell int, @at int, @ac int;

    WHILE EXISTS (SELECT 1 FROM @months)
    BEGIN
        SELECT TOP (1) @cur_ym = YM FROM @months ORDER BY YM;
        DELETE FROM @months WHERE YM = @cur_ym;

        SET @dt_from = datefromparts(@cur_ym / 100, @cur_ym % 100, 1);
        SET @data_to = IIF(@last < eomonth(@dt_from), @last, eomonth(@dt_from));   -- последний день данных в месяце
        IF @data_to < @dt_from CONTINUE;                                           -- данных за месяц ещё нет
        SET @dt_to   = dateadd(day, 1, @data_to);                                  -- граница НЕ включается
        SET @full    = IIF(@data_to = eomonth(@dt_from), 1, 0);

        -- 1. Партии, действующие в месяце. Период партии — с max(начало месяца, дата нарезки) по последний день данных.
        DROP TABLE IF EXISTS #gcg_batch;
        SELECT BATCH_ID, PRE_TO
             , P_FROM = IIF(DRAW_DATE > @dt_from, DRAW_DATE, @dt_from)
        INTO #gcg_batch
        FROM dbo.I_GCG_BATCH (nolock)
        WHERE IIF(DRAW_DATE > @dt_from, DRAW_DATE, @dt_from) <= @data_to;

        IF NOT EXISTS (SELECT 1 FROM #gcg_batch)
            THROW 50002, N'gcg_report: в месяце нет ни одной заведённой партии ГКГ (журнал I_GCG_BATCH) — месяц не посчитан', 1;

        -- чеки читаются с начала периода самой ранней партии по последний день данных
        SET @scan_from = (SELECT min(P_FROM) FROM #gcg_batch);

        -- партия должна быть заведена с сегментом и давностью на дату нарезки; иначе — перезавести (раздел 5 скрипта)
        IF EXISTS (SELECT 1 FROM dbo.I_GCG_PRE AS p (nolock) JOIN #gcg_batch AS b ON b.BATCH_ID = p.BATCH_ID
                   WHERE p.SEGMENT IS NULL OR (p.LAST_DATA IS NULL AND isnull(p.X_OTT, 0) <> 0))
            THROW 50006, N'gcg_report: у клиентов партии нет сегмента или давности на дату нарезки — партию нужно перезавести, месяц не посчитан', 1;

        -- 2. Клиенты партий: состав, группа, сегмент, «ДО» и слой давности — зафиксированные при заведении партии.
        --    В оценку — только клиенты с «ДО» > 0 (по окну своего сегмента).
        --    Слой: Активные — неделя с последней покупки до нарезки (1–6), Отток — месяц последней покупки (YYYYMM).
        DROP TABLE IF EXISTS #gcg_pop;
        SELECT p.BATCH_ID, p.ID_CONTACT, p.GCG, p.SEGMENT, p.X_ACT, p.X_OTT, b.P_FROM
             , LAYER = IIF(p.SEGMENT = 2, (datediff(day, p.LAST_DATA, b.PRE_TO) + 6) / 7, year(p.LAST_DATA) * 100 + month(p.LAST_DATA))
        INTO #gcg_pop
        FROM dbo.I_GCG_PRE AS p (nolock)
             JOIN #gcg_batch AS b ON b.BATCH_ID = p.BATCH_ID
        WHERE p.LAST_DATA IS NOT NULL
          AND IIF(p.SEGMENT = 5, p.X_OTT, p.X_ACT) > 0;
        CREATE CLUSTERED COLUMNSTORE INDEX cci_gcg_pop ON #gcg_pop;

        -- 2.1 Ячейки: слои по порядку давности (сначала недавние), соседние сливаются, пока в ячейке нет обеих групп;
        --     хвост без одной из групп присоединяется к предыдущей ячейке
        DROP TABLE IF EXISTS #gcg_lay;
        SELECT BATCH_ID, SEGMENT, LAYER
             , NT   = sum(IIF(GCG = 0, 1, 0))
             , NC   = sum(IIF(GCG = 1, 1, 0))
             , ORD  = row_number() OVER (PARTITION BY BATCH_ID, SEGMENT ORDER BY IIF(SEGMENT = 2, LAYER, -LAYER))
             , CELL = cast(NULL AS int)
        INTO #gcg_lay
        FROM #gcg_pop
        GROUP BY BATCH_ID, SEGMENT, LAYER;

        SELECT @pb = NULL, @ps = NULL, @cell = 0, @at = 0, @ac = 0;
        DECLARE gcg_lay_cur CURSOR LOCAL FAST_FORWARD FOR
            SELECT BATCH_ID, SEGMENT, ORD, NT, NC FROM #gcg_lay ORDER BY BATCH_ID, SEGMENT, ORD;
        OPEN gcg_lay_cur;
        FETCH NEXT FROM gcg_lay_cur INTO @lb, @ls, @lo, @lnt, @lnc;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF @pb IS NULL OR @lb <> @pb OR @ls <> @ps
            BEGIN
                IF @pb IS NOT NULL AND (@at = 0 OR @ac = 0) AND @cell > 1
                    UPDATE #gcg_lay SET CELL = @cell - 1 WHERE BATCH_ID = @pb AND SEGMENT = @ps AND CELL = @cell;
                SELECT @cell = 1, @at = 0, @ac = 0, @pb = @lb, @ps = @ls;
            END
            ELSE IF @at > 0 AND @ac > 0
                SELECT @cell += 1, @at = 0, @ac = 0;

            UPDATE #gcg_lay SET CELL = @cell WHERE BATCH_ID = @lb AND SEGMENT = @ls AND ORD = @lo;
            SELECT @at += @lnt, @ac += @lnc;
            FETCH NEXT FROM gcg_lay_cur INTO @lb, @ls, @lo, @lnt, @lnc;
        END
        IF @pb IS NOT NULL AND (@at = 0 OR @ac = 0) AND @cell > 1
            UPDATE #gcg_lay SET CELL = @cell - 1 WHERE BATCH_ID = @pb AND SEGMENT = @ps AND CELL = @cell;
        CLOSE gcg_lay_cur;
        DEALLOCATE gcg_lay_cur;

        -- 3. Оборот и чеки периода партии
        DROP TABLE IF EXISTS #gcg_cur;
        SELECT m.BATCH_ID
             , m.ID_CONTACT
             , Y      = sum(IIF(a.DATA >= m.P_FROM, cast(a.COST_DISCOUNT AS float), NULL))
             , CHECKS = count(IIF(a.DATA >= m.P_FROM, a.ID_CHECK, NULL))
        INTO #gcg_cur
        FROM dbo.I_CHECKHEADER AS a (nolock)
             JOIN #gcg_pop AS m ON m.ID_CONTACT = a.ID_CONTACT
        WHERE a.ID_COMPANY = @idc
          AND a.ID_CONTACT <> 0
          AND a.DATA >= @scan_from AND a.DATA < @dt_to
        GROUP BY m.BATCH_ID, m.ID_CONTACT
        OPTION (RECOMPILE);
        CREATE CLUSTERED COLUMNSTORE INDEX cci_gcg_cur ON #gcg_cur;

        -- 4. Клиентская таблица. Сегмент и слой — на дату нарезки партии; «ДО» — по сегменту.
        DROP TABLE IF EXISTS #gcg_cl;
        SELECT p.BATCH_ID
             , p.ID_CONTACT
             , p.SEGMENT
             , l.CELL
             , GRP    = IIF(p.GCG = 1, 'CG', 'TG')   -- TG = ЦГ
             , Y      = isnull(c.Y, 0)
             , CHECKS = isnull(c.CHECKS, 0)
             , BUYER  = IIF(isnull(c.CHECKS, 0) > 0, 1, 0)
             , X      = isnull(IIF(p.SEGMENT = 5, p.X_OTT, p.X_ACT), 0)
        INTO #gcg_cl
        FROM #gcg_pop AS p
             JOIN #gcg_lay AS l ON l.BATCH_ID = p.BATCH_ID AND l.SEGMENT = p.SEGMENT AND l.LAYER = p.LAYER
             LEFT JOIN #gcg_cur AS c ON c.BATCH_ID = p.BATCH_ID AND c.ID_CONTACT = p.ID_CONTACT;
        CREATE CLUSTERED COLUMNSTORE INDEX cci_gcg_cl ON #gcg_cl;

        -- 5. Выбросы — правило κ = ½ (как в отчёте эффекта акций). Граница ячейки (партия × сегмент × слой):
        --    G = exp(m + z·s); m — медиана ln трат; s = (P75 ln трат − m) / 0,674; z = Φ⁻¹(1 − κ / n).
        --    Φ⁻¹ — обратная функция нормального распределения, приближение Акклама (точность ~1e-9).

        -- 5.1 «ДО»: по ЦГ и ГКГ вместе, среди трат «ДО» > 0
        DROP TABLE IF EXISTS #gcg_qx;
        SELECT BATCH_ID, SEGMENT, CELL, M, P75, N
        INTO #gcg_qx
        FROM (SELECT BATCH_ID, SEGMENT, CELL
                   , M   = PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY V) OVER (PARTITION BY BATCH_ID, SEGMENT, CELL)
                   , P75 = PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY V) OVER (PARTITION BY BATCH_ID, SEGMENT, CELL)
                   , N   = count(*) OVER (PARTITION BY BATCH_ID, SEGMENT, CELL)
                   , RN  = row_number() OVER (PARTITION BY BATCH_ID, SEGMENT, CELL ORDER BY (SELECT NULL))
              FROM (SELECT BATCH_ID, SEGMENT, CELL, V = log(IIF(X > 0, X, NULL)) FROM #gcg_cl WHERE X > 0) AS t) AS q
        WHERE RN = 1;

        DROP TABLE IF EXISTS #gcg_gx;
        SELECT q.BATCH_ID, q.SEGMENT, q.CELL
             , G_X = IIF(q.P75 > q.M, exp(q.M + z.Z * (q.P75 - q.M) / 0.674), NULL)   -- P75 = медиане — граница не применяется
        INTO #gcg_gx
        FROM #gcg_qx AS q
             CROSS APPLY (SELECT P = 1.0 - @kappa / q.N) AS p
             CROSS APPLY (SELECT QC = p.P - 0.5, R = square(p.P - 0.5), QT = sqrt(-2.0 * log(1.0 - p.P))) AS w
             CROSS APPLY (SELECT Z = CASE WHEN p.P <= 0.97575
                  THEN (((((-3.969683028665376e+01 * w.R + 2.209460984245205e+02) * w.R - 2.759285104469687e+02) * w.R
                          + 1.383577518672690e+02) * w.R - 3.066479806614716e+01) * w.R + 2.506628277459239e+00) * w.QC
                     / (((((-5.447609879822406e+01 * w.R + 1.615858368580409e+02) * w.R - 1.556989798598866e+02) * w.R
                          + 6.680131188771972e+01) * w.R - 1.328068155288572e+01) * w.R + 1.0)
                  ELSE -(((((-7.784894002430293e-03 * w.QT - 3.223964580411365e-01) * w.QT - 2.400758277161838e+00) * w.QT
                          - 2.549732539343734e+00) * w.QT + 4.374664141464968e+00) * w.QT + 2.938163982698783e+00)
                     / ((((7.784695709041462e-03 * w.QT + 3.224671290700398e-01) * w.QT + 2.445134137142996e+00) * w.QT
                          + 3.754408661907416e+00) * w.QT + 1.0)
                  END) AS z;

        IF @debug = 1
            SELECT [Выбросы «ДО»: месяц]       = @cur_ym
                 , [Партия]                    = b.BATCH_ID
                 , [Сегмент]                   = IIF(b.SEGMENT = 2, N'Активные', N'Отток')
                 , [Группа]                    = IIF(b.GRP = 'TG', N'ЦГ', N'ГКГ')
                 , [Клиентов]                  = count(*)
                 , [Исключено]                 = sum(o.OUT_X)
                 , [Доля исключённых, %]       = cast(100.0 * sum(o.OUT_X) / count(*) AS decimal(18, 4))
                 , [Из них «ДО» < 0]           = sum(IIF(b.X < 0, 1, 0))
                 , [Их «ДО», руб]              = sum(IIF(o.OUT_X = 1, b.X, 0))
                 , [Доля их «ДО», %]           = cast(100.0 * sum(IIF(o.OUT_X = 1, b.X, 0)) / nullif(sum(b.X), 0) AS decimal(18, 4))
                 , [Граница «ДО» по слоям: мин, руб]  = min(g.G_X)
                 , [Граница «ДО» по слоям: макс, руб] = max(g.G_X)
            FROM #gcg_cl AS b
                 LEFT JOIN #gcg_gx AS g ON g.BATCH_ID = b.BATCH_ID AND g.SEGMENT = b.SEGMENT AND g.CELL = b.CELL
                 CROSS APPLY (SELECT OUT_X = IIF(b.X < 0 OR b.X > g.G_X, 1, 0)) AS o
            GROUP BY b.BATCH_ID, b.SEGMENT, b.GRP
            ORDER BY b.BATCH_ID, b.SEGMENT, b.GRP;

        -- клиент с «ДО» выше границы или с отрицательным «ДО» исключается из расчёта целиком
        DELETE b
        FROM #gcg_cl AS b
             LEFT JOIN #gcg_gx AS g ON g.BATCH_ID = b.BATCH_ID AND g.SEGMENT = b.SEGMENT AND g.CELL = b.CELL
        WHERE b.X < 0 OR b.X > g.G_X;

        -- 5.1а ячейка, оставшаяся после исключения выбросов «ДО» без одной из групп, присоединяется к предыдущей
        --      ячейке с обеими группами (как хвост при слиянии слоёв), если предыдущей нет — к следующей
        DROP TABLE IF EXISTS #gcg_cellx;
        SELECT BATCH_ID, SEGMENT, CELL, OK = IIF(min(GRP) <> max(GRP), 1, 0)
        INTO #gcg_cellx
        FROM #gcg_cl
        GROUP BY BATCH_ID, SEGMENT, CELL;

        UPDATE b
        SET CELL = isnull(p.CELL, n.CELL)
        FROM #gcg_cl AS b
             JOIN #gcg_cellx AS x ON x.BATCH_ID = b.BATCH_ID AND x.SEGMENT = b.SEGMENT AND x.CELL = b.CELL AND x.OK = 0
             OUTER APPLY (SELECT CELL = max(y.CELL) FROM #gcg_cellx AS y
                          WHERE y.BATCH_ID = b.BATCH_ID AND y.SEGMENT = b.SEGMENT AND y.OK = 1 AND y.CELL < b.CELL) AS p
             OUTER APPLY (SELECT CELL = min(y.CELL) FROM #gcg_cellx AS y
                          WHERE y.BATCH_ID = b.BATCH_ID AND y.SEGMENT = b.SEGMENT AND y.OK = 1 AND y.CELL > b.CELL) AS n
        WHERE isnull(p.CELL, n.CELL) IS NOT NULL;

        -- 5.2 Месяц расчёта («во время»): m и s — по покупателям ГКГ, n — все покупатели слоя (ЦГ и ГКГ)
        DROP TABLE IF EXISTS #gcg_qy;
        SELECT BATCH_ID, SEGMENT, CELL, M, P75, NC
        INTO #gcg_qy
        FROM (SELECT BATCH_ID, SEGMENT, CELL
                   , M   = PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY V) OVER (PARTITION BY BATCH_ID, SEGMENT, CELL)
                   , P75 = PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY V) OVER (PARTITION BY BATCH_ID, SEGMENT, CELL)
                   , NC  = count(*) OVER (PARTITION BY BATCH_ID, SEGMENT, CELL)
                   , RN  = row_number() OVER (PARTITION BY BATCH_ID, SEGMENT, CELL ORDER BY (SELECT NULL))
              FROM (SELECT BATCH_ID, SEGMENT, CELL, V = log(IIF(Y > 0, Y, NULL)) FROM #gcg_cl WHERE GRP = 'CG' AND Y > 0) AS t) AS q
        WHERE RN = 1;

        DROP TABLE IF EXISTS #gcg_gy;
        SELECT n.BATCH_ID, n.SEGMENT, n.CELL
             , G_Y = IIF(q.NC >= 4 AND q.P75 > q.M, exp(q.M + z.Z * (q.P75 - q.M) / 0.674), NULL)   -- меньше 4 покупателей ГКГ или P75 = медиане — не обрабатывается
        INTO #gcg_gy
        FROM (SELECT BATCH_ID, SEGMENT, CELL, N = sum(BUYER) FROM #gcg_cl GROUP BY BATCH_ID, SEGMENT, CELL) AS n
             LEFT JOIN #gcg_qy AS q ON q.BATCH_ID = n.BATCH_ID AND q.SEGMENT = n.SEGMENT AND q.CELL = n.CELL
             CROSS APPLY (SELECT P = 1.0 - @kappa / nullif(n.N, 0)) AS p
             CROSS APPLY (SELECT QC = p.P - 0.5, R = square(p.P - 0.5), QT = sqrt(-2.0 * log(1.0 - p.P))) AS w
             CROSS APPLY (SELECT Z = CASE WHEN p.P <= 0.97575
                  THEN (((((-3.969683028665376e+01 * w.R + 2.209460984245205e+02) * w.R - 2.759285104469687e+02) * w.R
                          + 1.383577518672690e+02) * w.R - 3.066479806614716e+01) * w.R + 2.506628277459239e+00) * w.QC
                     / (((((-5.447609879822406e+01 * w.R + 1.615858368580409e+02) * w.R - 1.556989798598866e+02) * w.R
                          + 6.680131188771972e+01) * w.R - 1.328068155288572e+01) * w.R + 1.0)
                  ELSE -(((((-7.784894002430293e-03 * w.QT - 3.223964580411365e-01) * w.QT - 2.400758277161838e+00) * w.QT
                          - 2.549732539343734e+00) * w.QT + 4.374664141464968e+00) * w.QT + 2.938163982698783e+00)
                     / ((((7.784695709041462e-03 * w.QT + 3.224671290700398e-01) * w.QT + 2.445134137142996e+00) * w.QT
                          + 3.754408661907416e+00) * w.QT + 1.0)
                  END) AS z;

        IF @debug = 1
            SELECT [Выбросы месяца: месяц]     = @cur_ym
                 , [Партия]                    = b.BATCH_ID
                 , [Сегмент]                   = IIF(b.SEGMENT = 2, N'Активные', N'Отток')
                 , [Группа]                    = IIF(b.GRP = 'TG', N'ЦГ', N'ГКГ')
                 , [Покупателей]               = sum(b.BUYER)
                 , [Выше границы]              = sum(o.OVER_Y)
                 , [Доля выше границы, %]      = cast(100.0 * sum(o.OVER_Y) / nullif(sum(b.BUYER), 0) AS decimal(18, 4))
                 , [Срезано, руб]              = sum(IIF(o.OVER_Y = 1, abs(b.Y) - g.G_Y, 0))
                 , [Доля срезанного ТО, %]     = cast(100.0 * sum(IIF(o.OVER_Y = 1, abs(b.Y) - g.G_Y, 0)) / nullif(sum(b.Y), 0) AS decimal(18, 4))
                 , [Граница месяца по слоям: мин, руб]  = min(g.G_Y)
                 , [Граница месяца по слоям: макс, руб] = max(g.G_Y)
                 , [Как обрабатывается]        = max(IIF(@Y_MODE = 'DROP', N'клиент исключается целиком', N'сумма обрезается до границы'))
            FROM #gcg_cl AS b
                 LEFT JOIN #gcg_gy AS g ON g.BATCH_ID = b.BATCH_ID AND g.SEGMENT = b.SEGMENT AND g.CELL = b.CELL
                 CROSS APPLY (SELECT OVER_Y = IIF(abs(b.Y) > g.G_Y, 1, 0)) AS o
            GROUP BY b.BATCH_ID, b.SEGMENT, b.GRP
            ORDER BY b.BATCH_ID, b.SEGMENT, b.GRP;

        IF @Y_MODE = 'DROP'
            DELETE b
            FROM #gcg_cl AS b
                 JOIN #gcg_gy AS g ON g.BATCH_ID = b.BATCH_ID AND g.SEGMENT = b.SEGMENT AND g.CELL = b.CELL
            WHERE abs(b.Y) > g.G_Y;
        ELSE
            UPDATE b
            SET Y = IIF(b.Y > 0, g.G_Y, -g.G_Y)   -- сумма за месяц обрезается до границы, отрицательная — до −G
            FROM #gcg_cl AS b
                 JOIN #gcg_gy AS g ON g.BATCH_ID = b.BATCH_ID AND g.SEGMENT = b.SEGMENT AND g.CELL = b.CELL
            WHERE abs(b.Y) > g.G_Y;

        -- 6. Агрегаты по ячейке (партия × сегмент × слой) и группе
        DROP TABLE IF EXISTS #gcg_stat;
        SELECT BATCH_ID, SEGMENT, CELL, GRP
             , CLIENTS   = count(*) * 1.0
             , BUYERS    = sum(BUYER * 1.0)
             , TO_PERIOD = sum(Y)
             , TO_BEFORE = sum(X)
             , CHK       = sum(CHECKS * 1.0)
             , MX        = avg(X)
        INTO #gcg_stat
        FROM #gcg_cl
        GROUP BY BATCH_ID, SEGMENT, CELL, GRP;

        IF EXISTS (SELECT 1 FROM #gcg_stat GROUP BY BATCH_ID, SEGMENT, CELL HAVING count(*) < 2)
            THROW 50003, N'gcg_report: в ячейке слоя нет одной из групп (ЦГ или ГКГ) — месяц не перезаписан', 1;

        -- 7. Доп ТО и ошибка по ячейке: дельта-метод для отношения, D = Y − KOEF·X в каждой группе
        DROP TABLE IF EXISTS #gcg_rep;
        SELECT t.BATCH_ID, t.SEGMENT, t.CELL
             , KOEF = c.TO_PERIOD / nullif(c.TO_BEFORE, 0)
             , NT = t.CLIENTS, NC = c.CLIENTS, MXT = t.MX, MXC = c.MX
             , T_CLIENTS = t.CLIENTS, T_BUYERS = t.BUYERS, T_TO = t.TO_PERIOD, T_BEF = t.TO_BEFORE, T_CHK = t.CHK
             , C_CLIENTS = c.CLIENTS, C_BUYERS = c.BUYERS, C_TO = c.TO_PERIOD, C_BEF = c.TO_BEFORE, C_CHK = c.CHK
        INTO #gcg_rep
        FROM #gcg_stat AS t
             JOIN #gcg_stat AS c ON t.BATCH_ID = c.BATCH_ID AND t.SEGMENT = c.SEGMENT AND t.CELL = c.CELL AND t.GRP = 'TG' AND c.GRP = 'CG';

        IF EXISTS (SELECT 1 FROM #gcg_rep WHERE KOEF IS NULL)
            THROW 50004, N'gcg_report: у ГКГ ячейки слоя нулевой оборот «ДО» — коэффициент не считается, месяц не перезаписан', 1;

        DROP TABLE IF EXISTS #gcg_se;
        SELECT b.BATCH_ID, b.SEGMENT, b.CELL
             , SE = r.NT * sqrt(  var(IIF(b.GRP = 'TG', b.Y - r.KOEF * b.X, NULL)) / r.NT
                                + square(r.MXT / nullif(r.MXC, 0))
                                  * var(IIF(b.GRP = 'CG', b.Y - r.KOEF * b.X, NULL)) / r.NC)
        INTO #gcg_se
        FROM #gcg_cl AS b
             JOIN #gcg_rep AS r ON r.BATCH_ID = b.BATCH_ID AND r.SEGMENT = b.SEGMENT AND r.CELL = b.CELL
        GROUP BY b.BATCH_ID, b.SEGMENT, b.CELL, r.NT, r.NC, r.KOEF, r.MXT, r.MXC;

        IF @debug = 1
            SELECT [Месяц]                              = @cur_ym
                 , [Партия]                             = x.BATCH_ID
                 , [Период с]                           = b.P_FROM
                 , [Сегмент]                            = IIF(x.SEGMENT = 2, N'Активные', N'Отток')
                 , [Ячеек слоёв]                        = x.CELLS
                 , [Доп ТО по слоям, руб (основной)]    = x.ADD_TO
                 , [SE по слоям]                        = x.SE
                 , [Без слоёв, руб (справочно)]         = x.T_TO - x.T_BEF * x.C_TO / nullif(x.C_BEF, 0)
                 , [Простая разница, руб (справочно)]   = (x.T_TO / nullif(x.NT, 0) - x.C_TO / nullif(x.NC, 0)) * x.NT
                 , [Клиентов ЦГ]                        = cast(x.NT AS int)
                 , [Клиентов ГКГ]                       = cast(x.NC AS int)
            FROM (SELECT r.BATCH_ID, r.SEGMENT, CELLS = count(*)
                       , ADD_TO = sum(r.T_TO - r.T_BEF * r.KOEF), SE = sqrt(sum(square(e.SE)))
                       , T_TO = sum(r.T_TO), T_BEF = sum(r.T_BEF), C_TO = sum(r.C_TO), C_BEF = sum(r.C_BEF)
                       , NT = sum(r.T_CLIENTS), NC = sum(r.C_CLIENTS)
                  FROM #gcg_rep AS r
                       JOIN #gcg_se AS e ON e.BATCH_ID = r.BATCH_ID AND e.SEGMENT = r.SEGMENT AND e.CELL = r.CELL
                  GROUP BY r.BATCH_ID, r.SEGMENT) AS x
                 JOIN #gcg_batch AS b ON b.BATCH_ID = x.BATCH_ID
            ORDER BY x.BATCH_ID, x.SEGMENT;

        -- 8. Строка сегмента = сумма ячеек всех партий; ячейки независимы — ошибки складываются квадратами
        DROP TABLE IF EXISTS #gcg_seg;
        SELECT r.SEGMENT
             , ADD_TO    = sum(r.T_TO - r.T_BEF * r.KOEF)
             , SE        = sqrt(sum(square(e.SE)))
             , T_CLIENTS = sum(r.T_CLIENTS), T_BUYERS = sum(r.T_BUYERS), T_TO = sum(r.T_TO), T_BEF = sum(r.T_BEF), T_CHK = sum(r.T_CHK)
             , C_CLIENTS = sum(r.C_CLIENTS), C_BUYERS = sum(r.C_BUYERS), C_TO = sum(r.C_TO), C_BEF = sum(r.C_BEF), C_CHK = sum(r.C_CHK)
        INTO #gcg_seg
        FROM #gcg_rep AS r
             JOIN #gcg_se AS e ON e.BATCH_ID = r.BATCH_ID AND e.SEGMENT = r.SEGMENT AND e.CELL = r.CELL
        GROUP BY r.SEGMENT;

        -- пустой расчёт не должен стереть месяц: шаг падает
        IF (SELECT count(*) FROM #gcg_seg) < 2
            THROW 50001, N'gcg_report: расчёт не дал строк обоих сегментов — месяц не перезаписан', 1;

        -- только показать результат месяца (@WRITE = 0): витрина не меняется
        IF @WRITE = 0
        BEGIN
            SELECT [Месяц], [Сегмент], [Доп ТО р], [ДИ 95% низ], [ДИ 95% верх], [Значимо]
                 , [Клиентов ЦГ], [Клиентов ГКГ], [ТО ЦГ], [ТО ГКГ], [ТО ЦГ ДО], [ТО ГКГ ДО], [KOEF]
                 , [BUDGET], [BUDGET_CG], [Данные с], [Данные по]
            FROM (SELECT ORD = s.SEGMENT, [Месяц] = @cur_ym, [Сегмент] = IIF(s.SEGMENT = 2, N'Активные', N'Отток')
                       , [Доп ТО р] = s.ADD_TO, [ДИ 95% низ] = s.ADD_TO - 1.96 * s.SE, [ДИ 95% верх] = s.ADD_TO + 1.96 * s.SE
                       , [Значимо] = IIF(abs(s.ADD_TO / nullif(s.SE, 0)) >= 1.96, N'ДА', N'НЕТ')
                       , [Клиентов ЦГ] = cast(s.T_CLIENTS AS int), [Клиентов ГКГ] = cast(s.C_CLIENTS AS int)
                       , [ТО ЦГ] = s.T_TO, [ТО ГКГ] = s.C_TO, [ТО ЦГ ДО] = s.T_BEF, [ТО ГКГ ДО] = s.C_BEF
                       , [KOEF] = (s.T_TO - s.ADD_TO) / nullif(s.T_BEF, 0)
                       , [BUDGET] = s.T_TO / nullif(s.T_CLIENTS, 0), [BUDGET_CG] = s.C_TO / nullif(s.C_CLIENTS, 0)
                       , [Данные с] = @dt_from, [Данные по] = @data_to
                  FROM #gcg_seg AS s
                  UNION ALL
                  SELECT 9, @cur_ym, N'ИТОГО'
                       , sum(ADD_TO), sum(ADD_TO) - 1.96 * sqrt(sum(square(SE))), sum(ADD_TO) + 1.96 * sqrt(sum(square(SE)))
                       , IIF(abs(sum(ADD_TO)) >= 1.96 * sqrt(sum(square(SE))), N'ДА', N'НЕТ')
                       , cast(sum(T_CLIENTS) AS int), cast(sum(C_CLIENTS) AS int), sum(T_TO), sum(C_TO), sum(T_BEF), sum(C_BEF)
                       , NULL, NULL, NULL, @dt_from, @data_to
                  FROM #gcg_seg) AS v
            ORDER BY ORD;
            CONTINUE;
        END

        -- 9. Запись месяца: строки месяца перезаписываются целиком
        BEGIN TRANSACTION;

        -- один запуск за раз: ночной шаг и ручной EXEC не задвоят строки месяца
        DECLARE @lock int;
        EXEC @lock = sp_getapplock @Resource = N'dbo.gcg_report', @LockMode = 'Exclusive', @LockOwner = 'Transaction';
        IF @lock < 0
            THROW 50001, N'gcg_report: не удалось взять блокировку записи витрины', 1;

        DELETE FROM dbo.I_GCG_REPORT WHERE YEAR_MONTH = @cur_ym;

        -- KOEF строки — средний по ячейкам с весом ТО ЦГ ДО: Доп ТО = ТО ЦГ − ТО ЦГ ДО × KOEF выполняется и для суммы
        INSERT INTO dbo.I_GCG_REPORT
              (ID_ORGANIZATION, [Канал продаж], YEAR_MONTH, [Год], [Месяц], [Сегмент канала], SEGMENT, [Сегмент]
             , [Доп ТО р], [Клиентов ЦГ], [Клиентов ГКГ], [ТО ЦГ], [ТО ГКГ], [ТО ЦГ ДО], [ТО ГКГ ДО]
             , [Бюджет ЦГ], [Бюджет ГКГ], [Ср чек ЦГ], [Ср чек ГКГ]
             , [KOEF], [BUDGET], [BUDGET_CG], [BUDGET_bef], [BUDGET_CG_bef]
             , [SE], [ДИ 95% низ], [ДИ 95% верх], [t], [Значимо]
             , [Данные с], [Данные по], [Полный месяц], [Окно ДО оттока, мес], [Дата расчёта], [Источник])
        SELECT NULL, NULL, @cur_ym, @cur_ym / 100, @cur_ym % 100, NULL, s.SEGMENT, IIF(s.SEGMENT = 2, N'Активные', N'Отток')
             , s.ADD_TO, cast(s.T_CLIENTS AS int), cast(s.C_CLIENTS AS int), s.T_TO, s.C_TO, s.T_BEF, s.C_BEF
             , s.T_TO / nullif(s.T_BUYERS, 0), s.C_TO / nullif(s.C_BUYERS, 0)
             , s.T_TO / nullif(s.T_CHK, 0),    s.C_TO / nullif(s.C_CHK, 0)
             , (s.T_TO - s.ADD_TO) / nullif(s.T_BEF, 0)
             , s.T_TO / nullif(s.T_CLIENTS, 0),  s.C_TO / nullif(s.C_CLIENTS, 0)
             , s.T_BEF / nullif(s.T_CLIENTS, 0), s.C_BEF / nullif(s.C_CLIENTS, 0)
             , s.SE, s.ADD_TO - 1.96 * s.SE, s.ADD_TO + 1.96 * s.SE
             , s.ADD_TO / nullif(s.SE, 0)
             , IIF(abs(s.ADD_TO / nullif(s.SE, 0)) >= 1.96, N'ДА', N'НЕТ')
             , @dt_from, @data_to, @full, NULL, sysdatetime(), N'процедура gcg_report'
        FROM #gcg_seg AS s;

        COMMIT TRANSACTION;
    END
END
GO

-- ============================== 5. ИЮЛЬСКАЯ ПАРТИЯ, ИЮЛЬ–ОКТЯБРЬ ==============================
-- Один пакет до конца файла: при ошибке дальше ничего не выполняется.
SET NOCOUNT ON;
SET XACT_ABORT ON;

-- июльская нарезка ГКГ: клиенты с первой покупкой по 01.07.2026, «ДО» — до 01.07.2026; заводится один раз
IF NOT EXISTS (SELECT 1 FROM dbo.I_GCG_BATCH (nolock) WHERE BATCH_ID = 1)
    EXEC dbo.gcg_batch_add @BATCH_ID = 1, @DRAW_DATE = '20260701', @FIRST_FROM = NULL, @FIRST_TO = '20260701'
                         , @PRE_TO = '20260701', @NOTE = N'июльская нарезка ГКГ';

-- партии, заведённые до слоёв (нет давности на дату нарезки), перезаводятся с теми же параметрами:
-- состав, группа, сегмент и «ДО» считаются заново, плюс давность; удаление старых строк и запись — одной транзакцией
-- внутри gcg_batch_add (после прохода по истории)
DECLARE @rb int, @rd date, @rff date, @rft date, @rpt date, @rn nvarchar(200);
WHILE EXISTS (SELECT 1 FROM dbo.I_GCG_PRE (nolock) WHERE LAST_DATA IS NULL AND isnull(X_OTT, 0) <> 0)
BEGIN
    SELECT TOP (1) @rb = b.BATCH_ID, @rd = b.DRAW_DATE, @rff = b.FIRST_FROM, @rft = b.FIRST_TO, @rpt = b.PRE_TO, @rn = b.NOTE
    FROM dbo.I_GCG_BATCH AS b (nolock)
    WHERE EXISTS (SELECT 1 FROM dbo.I_GCG_PRE AS p (nolock)
                  WHERE p.BATCH_ID = b.BATCH_ID AND p.LAST_DATA IS NULL AND isnull(p.X_OTT, 0) <> 0)
    ORDER BY b.BATCH_ID;

    RAISERROR(N'перезаводится партия %d', 0, 1, @rb) WITH NOWAIT;
    EXEC dbo.gcg_batch_add @BATCH_ID = @rb, @DRAW_DATE = @rd, @FIRST_FROM = @rff, @FIRST_TO = @rft, @PRE_TO = @rpt, @NOTE = @rn, @REPLACE = 1;
END

-- ИЮЛЬ, АВГУСТ, СЕНТЯБРЬ и ОКТЯБРЬ (по последний день данных): считаются и записываются в витрину I_GCG_REPORT
-- (июль — вместо строк Манзаны, август — вместо строк из файла); в выводе — выбросы и расчёт по партиям
EXEC dbo.gcg_report @YM = 202607, @debug = 1, @REPLACE_FILES = 1;
EXEC dbo.gcg_report @YM = 202608, @debug = 1, @REPLACE_FILES = 1;
EXEC dbo.gcg_report @YM = 202609, @debug = 1;
EXEC dbo.gcg_report @YM = 202610, @debug = 1;   -- октябрь — по последний день данных

-- ЭФФЕКТ ИЮЛЯ–ОКТЯБРЯ — из витрины
SELECT YEAR_MONTH, [Сегмент], [Доп ТО р], [ДИ 95% низ], [ДИ 95% верх], [Значимо]
     , [Клиентов ЦГ], [Клиентов ГКГ], [ТО ЦГ], [ТО ГКГ], [ТО ЦГ ДО], [ТО ГКГ ДО], [KOEF]
     , [BUDGET], [BUDGET_CG], [Данные с], [Данные по], [Дата расчёта]
FROM dbo.I_GCG_REPORT (nolock)
WHERE YEAR_MONTH IN (202607, 202608, 202609, 202610)
ORDER BY YEAR_MONTH, SEGMENT;

SELECT YEAR_MONTH
     , [Сегмент]      = N'ИТОГО'
     , [Доп ТО р]     = sum([Доп ТО р])
     , [ДИ 95% низ]   = sum([Доп ТО р]) - 1.96 * sqrt(sum(square([SE])))
     , [ДИ 95% верх]  = sum([Доп ТО р]) + 1.96 * sqrt(sum(square([SE])))
     , [Значимо]      = IIF(abs(sum([Доп ТО р])) >= 1.96 * sqrt(sum(square([SE]))), N'ДА', N'НЕТ')
     , [Клиентов ЦГ]  = sum([Клиентов ЦГ])
     , [Клиентов ГКГ] = sum([Клиентов ГКГ])
     , [ТО ЦГ]        = sum([ТО ЦГ])
     , [ТО ГКГ]       = sum([ТО ГКГ])
FROM dbo.I_GCG_REPORT (nolock)
WHERE YEAR_MONTH IN (202607, 202608, 202609, 202610)
GROUP BY YEAR_MONTH
ORDER BY YEAR_MONTH;
