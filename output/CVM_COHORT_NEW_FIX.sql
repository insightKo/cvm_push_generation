/* =====================================================================================
   CVM_COHORT_NEW_FIX.sql — исправленная dbo.cohort_new (шаг 52 «cohort_new» задания «MCI Regular update»)
   Таблица dbo.I_COHORT_NEW — прежние 13 колонок те же (типы, YEAR_MONTH-метки периодов) + 2 новые:
   TURNOVER_GROSS (ТО до скидки, SUM(COST)) и DISCOUNT (скидка, SUM(DISCOUNT)); TURNOVER — как было, после скидки.

   Методика: новый клиент = первый чек в организациях ID_SALE_TYPE = 1 (ID_CONTACT <> 0),
   поколение = месяц первого чека (MIN(YEAR_MONTH)), период 0 = ВСЕ чеки в день первой покупки (аудит 23.10.2026:
   раньше — только чеки в минуту первой покупки, а остальные чеки этого дня не попадали ни в один период),
   периоды 1..5 = 28-дневные окна от даты первой покупки, CLIENTS/TURNOVER/CHECKS/
   индекс цены — по чекам, у которых есть строка I_CONT_ASSORT_W (как INNER JOIN раньше),
   VISITS — по всем чекам окна (как t1 раньше), RETENTION = CLIENTS / CLIENTS периода 0.
   ТО до скидки — по решениям Елены 12.10 и 16.10.2026 (ТО после скидки в оценках некорректно); портал показывает его.

   Что исправлено (ревью sql-senior 23.10.2026):
   1) Период считается только когда окно закрыто данными для ВСЕХ клиентов поколения:
      конец окна последнего дня месяца (последний день месяца + 28·k) <= последний день чеков − @settle_days.
      @settle_days = 10: ежедневная перезаливка «MCI Reload every day» переписывает getdate()-10 … getdate()-1,
      а I_CONT_ASSORT_W строится только по закрытым неделям (до 6 дней задержки). Раньше период вставлялся
      со 2-го числа следующего месяца по незакрытым окнам и замораживался (NOT IN по LIFETIME_MONTH);
      исправлялся только чисткой clear_data_update при глубокой перезаливке (первая суббота месяца).
   2) Окна ровно по 28 дней: период k = дни (k−1)·28+1 … k·28 от даты первой покупки (1–28, 29–56, … 113–140).
      Раньше DATA (date) сравнивалась с datetime MIN_DATA + сдвиг: окна по 27 дней, дни 28/56/84/112 терялись,
      у первой покупки ровно в 00:00 в период 1 попадал день 0 (с самим первым чеком).
   3) Один проход по I_CHECKHEADER на поколение (все нужные периоды разом, фильтр по DATA — отсечение групп строк),
      вместо 1 + 2 × (число периодов) полных сканов на поколение; первые покупки — один проход за запуск
      и только когда есть что считать; считаются только недостающие пары (поколение, период).
   4) Первые покупки — по клиенту (без ID_ORGANIZATION в группировке): при второй организации с ID_SALE_TYPE = 1
      строки не размножаются. Сейчас такая организация одна (1) — результат тот же.
   5) NOT IN по nullable LIFETIME_MONTH заменён на NOT EXISTS; DECLARE вынесены из циклов; запись — одной транзакцией.

   Параметры (шаг задания не меняется: exec dbo.cohort_new):
     @settle_days  — запас дней после закрытия окна (по умолчанию 10, см. п.1);
     @rebuild = 1  — пересчитать все закрытые периоды и заменить строки компании одной транзакцией;
     @first_cohort — первое поколение; NULL = MIN(COHORT) из I_COHORT_NEW (сейчас 202302),
                     если таблица пуста — месяц после первого месяца чеков (как было).

   Порядок запуска: файл целиком (F5) или обменником (CVM_API_nightly) не во время «MCI Reload every day» /
   «MCI Regular update» (блок 2 сам проверяет и откажется ошибкой 16 — раннер обменника повторит через 10 минут).
   Блок 0 — новые колонки, блок 1 — процедура, блок 2 — одноразовый пересчёт истории и сравнение.
   Повторный запуск безопасен: колонки добавляются, только если их нет; процедура — CREATE OR ALTER;
   пересчёт истории идёт, только пока в таблице есть строки без ТО до скидки (то есть посчитанные старой процедурой).
   ===================================================================================== */

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

/* ---------- Блок 0. Колонки ТО до скидки и скидки ---------- */
IF COL_LENGTH('dbo.I_COHORT_NEW', 'TURNOVER_GROSS') IS NULL
    ALTER TABLE dbo.I_COHORT_NEW ADD TURNOVER_GROSS decimal(38,2) NULL;
IF COL_LENGTH('dbo.I_COHORT_NEW', 'DISCOUNT') IS NULL
    ALTER TABLE dbo.I_COHORT_NEW ADD DISCOUNT decimal(38,2) NULL;
GO

/* ---------- Блок 1. Процедура ---------- */
CREATE OR ALTER PROCEDURE dbo.cohort_new
    @settle_days  int = 10,
    @rebuild      bit = 0,
    @first_cohort int = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @lifetime  int  = 5,          -- сколько периодов каждого поколения рассчитывать (как было)
            @idc       int,
            @last_date date,
            @closed_to date,
            @fc        int,
            @min_ym    int,
            @YM        int,
            @kmin      int,
            @kmax      int,
            @c_first   date,
            @c_last    date,
            @d_from    date,
            @d_to      date,
            @w_from    int,
            @w_to      int;

    /* месяцы календаря с порядковым номером — для меток YEAR_MONTH (как #yearmonth раньше) */
    DROP TABLE IF EXISTS #ym;
    SELECT YEAR_MONTH,
           FIRST_DAY = MIN([DATE]),
           LAST_DAY  = MAX([DATE]),
           RN        = ROW_NUMBER() OVER (ORDER BY YEAR_MONTH)
    INTO #ym
    FROM dbo.I_CALENDAR (nolock)
    GROUP BY YEAR_MONTH;

    DROP TABLE IF EXISTS #org, #need, #fp, #s, #res;
    CREATE TABLE #org  (ID_ORGANIZATION int NOT NULL);
    CREATE TABLE #need (COHORT int NOT NULL, LIFETIME_MONTH int NOT NULL, YEAR_MONTH int NOT NULL,
                        C_FIRST date NOT NULL, C_LAST date NOT NULL);
    /* первые покупки клиентов нужных поколений: при пересчёте истории — миллионы строк, поэтому колоночный */
    CREATE TABLE #fp   (ID_CONTACT int NOT NULL, COHORT int NOT NULL, FIRST_DATE date NOT NULL,
                        INDEX cci_fp CLUSTERED COLUMNSTORE);
    CREATE TABLE #s    (COHORT int NOT NULL, LIFETIME_MONTH int NOT NULL, CLIENTS int, TURNOVER decimal(38,2),
                        TURNOVER_GROSS decimal(38,2), DISCOUNT decimal(38,2),
                        CHECKS int, IDX_SUM decimal(38,6), IDX_N int, VIS_DAYS int, VIS_CLIENTS int);
    CREATE TABLE #res  (COHORT int, LIFETIME_MONTH int, YEAR_MONTH int, CLIENTS int, TURNOVER decimal(38,2),
                        CHECKS int, BUDGET decimal(38,6), AVG_CHEQUE decimal(38,6), CHEQUE_PER_CLIENT numeric(24,12),
                        AVG_PICE_INDEX decimal(38,6), VISITS float, RETENTION numeric(24,12), ID_COMPANY int,
                        TURNOVER_GROSS decimal(38,2), DISCOUNT decimal(38,2));

    SELECT @idc = MIN(ID_COMPANY) FROM dbo.I_ORGANIZATION (nolock);

    WHILE @idc IS NOT NULL
    BEGIN
        TRUNCATE TABLE #org;
        TRUNCATE TABLE #need;
        TRUNCATE TABLE #fp;
        TRUNCATE TABLE #s;
        TRUNCATE TABLE #res;

        /* офлайн-организации (как join I_ORGANIZATION c on … c.ID_SALE_TYPE = 1) */
        INSERT INTO #org (ID_ORGANIZATION)
        SELECT DISTINCT ID_ORGANIZATION
        FROM dbo.I_ORGANIZATION (nolock)
        WHERE ID_SALE_TYPE = 1;

        /* последний день чеков: сначала узко по свежим датам (отсечение групп строк), иначе — без окна */
        SELECT @last_date = MAX(a.DATA)
        FROM dbo.I_CHECKHEADER a (nolock)
        WHERE a.ID_COMPANY = @idc
          AND a.ID_ORGANIZATION IN (SELECT o.ID_ORGANIZATION FROM #org o)
          AND a.DATA >= DATEADD(day, -62, CAST(GETDATE() AS date));

        IF @last_date IS NULL
            SELECT @last_date = MAX(a.DATA)
            FROM dbo.I_CHECKHEADER a (nolock)
            WHERE a.ID_COMPANY = @idc
              AND a.ID_ORGANIZATION IN (SELECT o.ID_ORGANIZATION FROM #org o);

        IF @last_date IS NOT NULL
        BEGIN
            SET @closed_to = DATEADD(day, -@settle_days, @last_date);

            /* первое поколение */
            SET @fc = @first_cohort;
            IF @fc IS NULL
                SELECT @fc = MIN(COHORT) FROM dbo.I_COHORT_NEW (nolock) WHERE ID_COMPANY = @idc;
            IF @fc IS NULL
            BEGIN
                SELECT @min_ym = MIN(YEAR_MONTH) FROM dbo.I_CHECKHEADER (nolock) WHERE ID_COMPANY = @idc;
                SELECT @fc = n.YEAR_MONTH
                FROM #ym m
                JOIN #ym n ON n.RN = m.RN + 1
                WHERE m.YEAR_MONTH = @min_ym;
            END;

            /* пары (поколение, период), окна которых закрыты и которых нет в таблице (или все — при @rebuild = 1).
               Метка YEAR_MONTH как раньше: период 0 и 1 — месяц поколения, период k — месяц поколения + (k − 1). */
            INSERT INTO #need (COHORT, LIFETIME_MONTH, YEAR_MONTH, C_FIRST, C_LAST)
            SELECT c.YEAR_MONTH, k.n, lab.YEAR_MONTH, c.FIRST_DAY, c.LAST_DAY
            FROM #ym c
            CROSS JOIN (VALUES (0), (1), (2), (3), (4), (5)) k(n)
            JOIN #ym lab
              ON lab.RN = c.RN + CASE WHEN k.n = 0 THEN 0 ELSE k.n - 1 END
            WHERE c.YEAR_MONTH >= @fc
              AND k.n <= @lifetime
              AND DATEADD(day, 28 * k.n, c.LAST_DAY) <= @closed_to
              AND (@rebuild = 1
                   OR NOT EXISTS (SELECT 1
                                  FROM dbo.I_COHORT_NEW e (nolock)
                                  WHERE e.ID_COMPANY = @idc
                                    AND e.COHORT = c.YEAR_MONTH
                                    AND e.LIFETIME_MONTH = k.n));

            IF EXISTS (SELECT 1 FROM #need)
            BEGIN
                /* первые покупки: один проход по истории офлайн-чеков, в #fp — только клиенты нужных поколений.
                   Период 0 — весь день первой покупки, поэтому нужна только дата (час и минута не нужны). */
                INSERT INTO #fp (ID_CONTACT, COHORT, FIRST_DATE)
                SELECT f.ID_CONTACT,
                       f.MIN_YM,
                       f.MIN_DATE
                FROM (
                    SELECT a.ID_CONTACT,
                           MIN_DATE = MIN(a.DATA),
                           MIN_YM   = MIN(a.YEAR_MONTH)
                    FROM dbo.I_CHECKHEADER a (nolock)
                    WHERE a.ID_COMPANY = @idc
                      AND a.ID_CONTACT <> 0
                      AND a.ID_ORGANIZATION IN (SELECT o.ID_ORGANIZATION FROM #org o)
                    GROUP BY a.ID_CONTACT
                ) f
                WHERE f.MIN_DATE IS NOT NULL
                  AND f.MIN_YM IN (SELECT n.COHORT FROM #need n);

                /* один проход по чекам на поколение: все недостающие периоды разом */
                SET @YM = (SELECT MIN(COHORT) FROM #need);
                WHILE @YM IS NOT NULL
                BEGIN
                    SELECT @kmin    = MIN(LIFETIME_MONTH),
                           @kmax    = MAX(LIFETIME_MONTH),
                           @c_first = MIN(C_FIRST),
                           @c_last  = MAX(C_LAST)
                    FROM #need
                    WHERE COHORT = @YM;

                    SET @d_from = DATEADD(day, CASE WHEN @kmin = 0 THEN 0 ELSE 28 * (@kmin - 1) + 1 END, @c_first);
                    SET @d_to   = DATEADD(day, 28 * @kmax, @c_last);

                    SELECT @w_from = MIN(WEEKNUMBER), @w_to = MAX(WEEKNUMBER)
                    FROM dbo.I_CALENDAR (nolock)
                    WHERE [DATE] BETWEEN @d_from AND @d_to;

                    INSERT INTO #s (COHORT, LIFETIME_MONTH, CLIENTS, TURNOVER, TURNOVER_GROSS, DISCOUNT, CHECKS,
                                    IDX_SUM, IDX_N, VIS_DAYS, VIS_CLIENTS)
                    SELECT @YM,
                           c.LIFETIME_MONTH,
                           SUM(CASE WHEN c.CHECKS_I > 0 THEN 1 ELSE 0 END),   -- клиенты с чеками, у которых есть строка индекса
                           SUM(c.TO_I),
                           SUM(c.TOG_I),
                           SUM(c.DISC_I),
                           SUM(c.CHECKS_I),
                           SUM(c.IDX_SUM),
                           SUM(c.IDX_N),
                           SUM(c.DAYS_ALL),                                    -- дни с покупками по всем чекам окна (VISITS)
                           COUNT(*)                                            -- клиенты с любым чеком окна (VISITS)
                    FROM (
                        SELECT dd.LIFETIME_MONTH,
                               dd.ID_CONTACT,
                               DAYS_ALL = COUNT(*),
                               CHECKS_I = SUM(dd.CHECKS_I),
                               TO_I     = SUM(dd.TO_I),
                               TOG_I    = SUM(dd.TOG_I),
                               DISC_I   = SUM(dd.DISC_I),
                               IDX_SUM  = SUM(dd.IDX_SUM),
                               IDX_N    = SUM(dd.IDX_N)
                        FROM (
                            SELECT p.LIFETIME_MONTH,
                                   a.ID_CONTACT,
                                   a.DATA,
                                   CHECKS_I = SUM(CASE WHEN d.ID_ORGANIZATION IS NOT NULL THEN 1 ELSE 0 END),
                                   TO_I     = SUM(CASE WHEN d.ID_ORGANIZATION IS NOT NULL THEN a.COST_DISCOUNT END),
                                   TOG_I    = SUM(CASE WHEN d.ID_ORGANIZATION IS NOT NULL THEN a.COST END),
                                   DISC_I   = SUM(CASE WHEN d.ID_ORGANIZATION IS NOT NULL THEN a.DISCOUNT END),
                                   IDX_SUM  = SUM(d.AVG_PRICE_INDEX),
                                   IDX_N    = COUNT(d.AVG_PRICE_INDEX)
                            FROM dbo.I_CHECKHEADER a (nolock)
                            JOIN #fp b
                              ON b.ID_CONTACT = a.ID_CONTACT
                             AND b.COHORT = @YM
                            CROSS APPLY (SELECT DD = DATEDIFF(day, b.FIRST_DATE, a.DATA)) x
                            CROSS APPLY (SELECT LIFETIME_MONTH =
                                             CASE WHEN x.DD = 0 THEN 0                                                     -- день первой покупки
                                                  WHEN x.DD BETWEEN 1 AND 28 * @lifetime THEN (x.DD - 1) / 28 + 1           -- окна по 28 дней
                                             END) p
                            JOIN #need n
                              ON n.COHORT = @YM
                             AND n.LIFETIME_MONTH = p.LIFETIME_MONTH
                            LEFT JOIN dbo.I_CONT_ASSORT_W d (nolock)
                              ON d.ID_CONTACT = a.ID_CONTACT
                             AND d.WEEKNUMBER = a.WEEKNUMBER
                             AND d.ID_COMPANY = a.ID_COMPANY
                             AND d.ID_ORGANIZATION = a.ID_ORGANIZATION
                             AND d.WEEKNUMBER BETWEEN @w_from AND @w_to
                            WHERE a.ID_COMPANY = @idc
                              AND a.ID_ORGANIZATION IN (SELECT o.ID_ORGANIZATION FROM #org o)
                              AND a.DATA BETWEEN @d_from AND @d_to
                            GROUP BY p.LIFETIME_MONTH, a.ID_CONTACT, a.DATA
                        ) dd
                        GROUP BY dd.LIFETIME_MONTH, dd.ID_CONTACT
                    ) c
                    GROUP BY c.LIFETIME_MONTH;

                    SET @YM = (SELECT MIN(COHORT) FROM #need WHERE COHORT > @YM);
                END;

                /* показатели и удержание; ALL_CLIENTS — период 0 из этого прогона, иначе уже записанный */
                INSERT INTO #res (COHORT, LIFETIME_MONTH, YEAR_MONTH, CLIENTS, TURNOVER, CHECKS, BUDGET, AVG_CHEQUE,
                                  CHEQUE_PER_CLIENT, AVG_PICE_INDEX, VISITS, RETENTION, ID_COMPANY, TURNOVER_GROSS, DISCOUNT)
                SELECT n.COHORT,
                       n.LIFETIME_MONTH,
                       n.YEAR_MONTH,
                       s.CLIENTS,
                       s.TURNOVER,
                       s.CHECKS,
                       s.TURNOVER / s.CLIENTS,
                       s.TURNOVER / s.CHECKS,
                       s.CHECKS * 1.0 / s.CLIENTS,
                       s.IDX_SUM / NULLIF(s.IDX_N, 0),
                       CASE WHEN n.LIFETIME_MONTH = 0 THEN CAST(1 AS float)
                            ELSE s.VIS_DAYS * 1.0 / NULLIF(s.VIS_CLIENTS, 0) END,
                       s.CLIENTS * 1.0 / ac.ALL_CLIENTS,
                       @idc,
                       s.TURNOVER_GROSS,
                       s.DISCOUNT
                FROM #need n
                JOIN #s s
                  ON s.COHORT = n.COHORT
                 AND s.LIFETIME_MONTH = n.LIFETIME_MONTH
                CROSS APPLY (SELECT ALL_CLIENTS = COALESCE(
                                 (SELECT s0.CLIENTS FROM #s s0
                                  WHERE s0.COHORT = n.COHORT AND s0.LIFETIME_MONTH = 0 AND s0.CLIENTS > 0),
                                 (SELECT MAX(e.CLIENTS) FROM dbo.I_COHORT_NEW e (nolock)
                                  WHERE e.ID_COMPANY = @idc AND e.COHORT = n.COHORT AND e.LIFETIME_MONTH = 0))) ac
                WHERE s.CLIENTS > 0
                  AND ac.ALL_CLIENTS > 0;

                /* запись одной транзакцией: при @rebuild — замена всех строк компании, иначе — только посчитанных пар */
                BEGIN TRAN;

                IF @rebuild = 1
                    DELETE FROM dbo.I_COHORT_NEW
                    WHERE ID_COMPANY = @idc;
                ELSE
                    DELETE t
                    FROM dbo.I_COHORT_NEW t
                    WHERE t.ID_COMPANY = @idc
                      AND EXISTS (SELECT 1 FROM #res r
                                  WHERE r.COHORT = t.COHORT AND r.LIFETIME_MONTH = t.LIFETIME_MONTH);

                INSERT INTO dbo.I_COHORT_NEW (COHORT, LIFETIME_MONTH, YEAR_MONTH, CLIENTS, TURNOVER, CHECKS, BUDGET,
                                              AVG_CHEQUE, CHEQUE_PER_CLIENT, AVG_PICE_INDEX, VISITS, RETENTION, ID_COMPANY,
                                              TURNOVER_GROSS, DISCOUNT)
                SELECT COHORT, LIFETIME_MONTH, YEAR_MONTH, CLIENTS, TURNOVER, CHECKS, BUDGET,
                       AVG_CHEQUE, CHEQUE_PER_CLIENT, AVG_PICE_INDEX, VISITS, RETENTION, ID_COMPANY,
                       TURNOVER_GROSS, DISCOUNT
                FROM #res;

                COMMIT;
            END;
        END;

        SET @idc = (SELECT MIN(ID_COMPANY) FROM dbo.I_ORGANIZATION (nolock) WHERE ID_COMPANY > @idc);
    END;
END;
GO

/* ---------- Блок 2. Одноразовый пересчёт истории (все строки — по новым окнам) и сравнение «было / стало» ---------- */
SET NOCOUNT ON;

IF EXISTS (SELECT 1
           FROM msdb.dbo.sysjobactivity ja (nolock)
           JOIN msdb.dbo.sysjobs j (nolock) ON j.job_id = ja.job_id
           WHERE j.name IN (N'MCI Regular update', N'MCI Reload every day')
             AND ja.session_id = (SELECT MAX(ss.session_id) FROM msdb.dbo.syssessions ss (nolock))
             AND ja.start_execution_date IS NOT NULL
             AND ja.stop_execution_date IS NULL)
   OR EXISTS (SELECT 1
              FROM integration.import_schedules (nolock)
              WHERE finished = 0
                AND target_table IN ('dbo.chequeitems_retro', 'dbo.chequeitems_rules'))
BEGIN
    RAISERROR(N'Идёт перезаливка чеков или «MCI Regular update» — пересчёт не запущен. Запустить блок 2 после окончания задания.', 16, 1);
END
ELSE IF EXISTS (SELECT 1 FROM dbo.I_COHORT_NEW (nolock))
    AND NOT EXISTS (SELECT 1 FROM dbo.I_COHORT_NEW (nolock) WHERE TURNOVER_GROSS IS NULL)
BEGIN
    /* повторный запуск (обменник повторяет файл): история уже пересчитана новой процедурой;
       пустая таблица сюда не попадает — для неё пересчёт всей истории идёт сразу */
    SELECT result = N'История уже пересчитана, пересчёт не нужен',
           rows_now = (SELECT COUNT(*) FROM dbo.I_COHORT_NEW (nolock));
END
ELSE
BEGIN
    DROP TABLE IF EXISTS #old;
    SELECT * INTO #old FROM dbo.I_COHORT_NEW (nolock);

    EXEC dbo.cohort_new @rebuild = 1;

    /* итог: строк было / стало и удержание по последним поколениям */
    SELECT rows_old = (SELECT COUNT(*) FROM #old),
           rows_new = (SELECT COUNT(*) FROM dbo.I_COHORT_NEW (nolock)),
           min_cohort = (SELECT MIN(COHORT) FROM dbo.I_COHORT_NEW (nolock)),
           max_cohort = (SELECT MAX(COHORT) FROM dbo.I_COHORT_NEW (nolock));

    SELECT COHORT         = COALESCE(n.COHORT, o.COHORT),
           LIFETIME_MONTH = COALESCE(n.LIFETIME_MONTH, o.LIFETIME_MONTH),
           YEAR_MONTH     = COALESCE(n.YEAR_MONTH, o.YEAR_MONTH),
           CLIENTS_OLD    = o.CLIENTS,
           CLIENTS_NEW    = n.CLIENTS,
           RETENTION_OLD  = o.RETENTION,
           RETENTION_NEW  = n.RETENTION,
           VISITS_OLD     = o.VISITS,
           VISITS_NEW     = n.VISITS
    FROM (SELECT * FROM dbo.I_COHORT_NEW (nolock) WHERE ID_COMPANY = 1) n
    FULL JOIN (SELECT * FROM #old WHERE ID_COMPANY = 1) o
      ON o.COHORT = n.COHORT
     AND o.LIFETIME_MONTH = n.LIFETIME_MONTH
    WHERE COALESCE(n.COHORT, o.COHORT) >= 202601
    ORDER BY 1, 2;
END;
GO
