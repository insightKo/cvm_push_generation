-- ============================================================================================
-- CVM_PRODUCT_SCORE_REGION — скоры товаров ПО ФЕДЕРАЛЬНЫМ ОКРУГАМ, ежедневный пересчёт (шаг «MCI Regular update»).
-- Для витрины ДЦО «Согласованные»: трафик-скор (FREQ_SCORE) и чек-скор (CHECK_SCORE) рядом с товаром в округе формы.
-- Формулы — скрипт Елены data/freq_score.sql (выгрузка data/I_PRODUCT_SCORE.xlsx от 05.08.2026), добавлен округ (19.10.2026).
-- ОДИН СКРИПТ: запустить целиком в SSMS (F5). Создаёт таблицу и процедуру, сразу считает (EXEC) и показывает итог по округам,
-- затем добавляет шаг в задание (секция 4).
--
-- ПЕРЕЧЕНЬ ДЕЙСТВИЙ ПО ОБЪЕКТАМ
--   dbo.I_PRODUCT_SCORE_REGION     создаётся (если нет): строка = организация × округ × товар; маленькая (≈ 7 округов × 12 тыс.
--                                  товаров), обычный кластерный ключ (ID_COMPANY, ID_ORGANIZATION, ID_DISTRICT, ID_PRODUCT).
--                                  Пересчёт целиком каждый день: в одной транзакции очистка и вставка; пустой расчёт таблицу
--                                  не очищает (ошибка, остаются вчерашние скоры).
--   dbo.product_score_region       создаётся/заменяется: расчёт за 6 недель до даты I_CVM_CONTACT.
--   msdb: задание «MCI Regular update» — шаг PRODUCT_SCORE_REGION вставляется сразу после CVM_CONTACT (берёт клиентские
--         FREQ_SCORE и BUDGET_SCORE этой ночи); следующие шаги сдвигаются на 1, переходы msdb пересчитывает сам;
--         при успехе и при ошибке — следующий шаг.
--   НЕ ТРОГАЕТСЯ И НЕ УДАЛЯЕТСЯ: dbo.I_PRODUCT_SCORE (августовская, из неё читают разделы портала «Товары», каталог, миссии),
--         I_CHECK, I_CHECKHEADER, I_CVM_CONTACT, I_SHOP, I_CITY, I_CITY_GUIDE (только чтение, NOLOCK), остальные шаги задания.
--
-- ЗНАЧЕНИЯ (как в freq_score.sql, на округ)
--   Окно: 6 недель до REFERENCE_DATE в I_CVM_CONTACT (та же дата и то же окно, что у клиентских FREQ_SCORE/BUDGET_SCORE в
--         CVM_CONTACT: с @act_date − 6 недель + 1 день по @act_date). ID_COMPANY = 1, ID_ORGANIZATION = 1 (офлайн; ДЦО по карте
--         в магазинах; у e-com магазинов округа нет).
--   Клиенты: I_CVM_CONTACT SEGMENT 1–4, как в freq_score.sql (без фрода 0, Оттока 5, Спящих 6).
--   Округ покупки — по магазину строки чека: I_SHOP.ID_CITY → I_CITY.CITY (область) → I_CITY_GUIDE.REGION → DISTRICT.
--         В справочнике нет городов федерального значения и «КАРЕЛИЯ» под своим именем: МОСКВА → ЦФО, САНКТ-ПЕТЕРБУРГ → СЗФО,
--         КАРЕЛИЯ (Республика Карелия) → СЗФО. Магазины без округа (на 19.10.2026 — 2 магазина) в расчёт не входят.
--   Сокращения округа — как в формах ДЦО: ЦФО, СЗФО, ЮФО, ПФО, УФО, СФО, ДФО (СКФО в справочнике нет — его республики там в ЮФО).
--   C_ALL — клиенты SEGMENT 1–4 округа: округ любимого магазина клиента (I_CVM_CONTACT.LOVE_STORE).
--   FREQ_SCORE    = сумма FREQ_SCORE клиента по строкам покупок товара в магазинах округа ÷ C_ALL округа.
--   LOYALTY_SCORE = сумма BUDGET_SCORE клиента по тем же строкам ÷ C_ALL округа.
--   CHECK_SCORE   = среднее по тем же строкам (сумма чека ÷ 10000); сумма чека — COST_DISCOUNT I_CHECKHEADER по ID_CHECK.
--         В freq_score.sql сумма бралась по ID_CONTACT_DATA (клиент × день); с 19.10.2026 — по ID_CHECK: у части чеков с картой
--         ID_CONTACT_DATA равен голой дате (напр. 20261010), и все такие чеки дня склеивались в одну «сумму» 44,6 млн.
--   CONTACTS      = разных клиентов SEGMENT 1–4, купивших товар в магазинах округа за окно; CHECKS — строк покупок (count ID_CHECK).
--   Отличие от freq_score.sql (результат тот же по смыслу): категория товара — одна на строку (MAX ID_CATEGORY_5), чтобы
--   у товара в округе была одна строка; дата окна — из I_CVM_CONTACT вместо MAX(DATA) по I_CHECKHEADER (полный скан);
--   клиент I_CVM_CONTACT берётся одной строкой (при дубле клиента строки чеков не умножаются); строки I_CHECK без товара не берутся.
-- ============================================================================================

USE mci_model
GO

-- ============================== 1. ТАБЛИЦА ==============================
IF OBJECT_ID(N'dbo.I_PRODUCT_SCORE_REGION', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.I_PRODUCT_SCORE_REGION (
          ID_COMPANY        int           NOT NULL
        , ID_ORGANIZATION   int           NOT NULL
        , ID_DISTRICT       int           NOT NULL     -- ID_DISTRICT из I_CITY_GUIDE
        , DISTRICT          nvarchar(8)   NOT NULL     -- сокращение, как в формах ДЦО: ЦФО, СЗФО, ...
        , ID_PRODUCT        bigint        NOT NULL
        , ID_CATEGORY_5     int           NULL
        , FREQ_SCORE        float         NULL         -- трафик-скор
        , LOYALTY_SCORE     float         NULL
        , CHECK_SCORE       float         NULL         -- чек-скор
        , CONTACTS          int           NULL
        , CHECKS            int           NULL
        , C_ALL             int           NULL         -- клиентов SEGMENT 1–4 округа (знаменатель FREQ/LOYALTY)
        , DATE_FROM         date          NOT NULL
        , DATE_TO           date          NOT NULL
        , CALC_DT           datetime2(0)  NOT NULL
        , CONSTRAINT PK_I_PRODUCT_SCORE_REGION PRIMARY KEY CLUSTERED (ID_COMPANY, ID_ORGANIZATION, ID_DISTRICT, ID_PRODUCT)
    );
END
GO

-- ============================== 2. РАСЧЁТ ==============================
CREATE OR ALTER PROCEDURE dbo.product_score_region
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @idc int = 1, @org int = 1;
    DECLARE @act_date date, @d1 date;

    -- 2.1 Округ магазина
    DROP TABLE IF EXISTS #reg, #shop, #cl, #c_all, #check, #res;

    SELECT REGION, ID_DISTRICT = MIN(ID_DISTRICT), DISTRICT = MIN(DISTRICT)
    INTO #reg
    FROM dbo.I_CITY_GUIDE WITH (NOLOCK)
    WHERE REGION IS NOT NULL
    GROUP BY REGION;

    SELECT x.ID_SHOP, x.ID_DISTRICT,
           DISTRICT = CASE x.ID_DISTRICT WHEN 1 THEN N'ЦФО' WHEN 2 THEN N'СЗФО' WHEN 3 THEN N'ЮФО' WHEN 4 THEN N'ПФО'
                                         WHEN 5 THEN N'УФО' WHEN 6 THEN N'СФО' WHEN 7 THEN N'ДФО' END
    INTO #shop
    FROM (
        SELECT s.ID_SHOP,
               ID_DISTRICT = COALESCE(r.ID_DISTRICT,
                                      CASE UPPER(LTRIM(RTRIM(c.CITY)))
                                           WHEN N'МОСКВА' THEN 1
                                           WHEN N'САНКТ-ПЕТЕРБУРГ' THEN 2
                                           WHEN N'КАРЕЛИЯ' THEN 2 END),
               rn = ROW_NUMBER() OVER (PARTITION BY s.ID_SHOP ORDER BY s.revision_date DESC, s.ID_CITY DESC)
        FROM dbo.I_SHOP AS s WITH (NOLOCK)
        LEFT JOIN dbo.I_CITY AS c WITH (NOLOCK) ON c.ID_CITY = s.ID_CITY
        LEFT JOIN #reg AS r ON r.REGION = c.CITY
        WHERE s.ID_COMPANY = @idc AND s.ID_ORGANIZATION = @org
    ) AS x
    WHERE x.rn = 1 AND x.ID_DISTRICT BETWEEN 1 AND 7;

    CREATE UNIQUE CLUSTERED INDEX ix ON #shop (ID_SHOP);

    -- 2.2 Клиенты SEGMENT 1–4 со скорами CVM_CONTACT и округом любимого магазина (клиент — одной строкой; дата окна — отсюда же:
    --     CVM_CONTACT пишет одну REFERENCE_DATE на всю таблицу)
    CREATE TABLE #cl (ID_CONTACT int NOT NULL, FREQ_SCORE float NULL, BUDGET_SCORE float NULL, REFERENCE_DATE date NULL,
                      ID_DISTRICT int NULL, INDEX cci CLUSTERED COLUMNSTORE);
    INSERT INTO #cl WITH (TABLOCK) (ID_CONTACT, FREQ_SCORE, BUDGET_SCORE, REFERENCE_DATE, ID_DISTRICT)
    SELECT a.ID_CONTACT, MAX(CAST(a.FREQ_SCORE AS float)), MAX(CAST(a.BUDGET_SCORE AS float)), MAX(a.REFERENCE_DATE), MAX(sh.ID_DISTRICT)
    FROM dbo.I_CVM_CONTACT AS a WITH (NOLOCK)
    LEFT JOIN #shop AS sh ON sh.ID_SHOP = a.LOVE_STORE
    WHERE a.ID_COMPANY = @idc AND a.ID_ORGANIZATION = @org AND a.SEGMENT IN (1, 2, 3, 4)
    GROUP BY a.ID_CONTACT;

    SET @act_date = (SELECT MAX(REFERENCE_DATE) FROM #cl);
    IF @act_date IS NULL
    BEGIN
        RAISERROR(N'product_score_region: в I_CVM_CONTACT нет клиентов SEGMENT 1–4 — скоры не пересчитаны, остаются прошлые.', 16, 1);
        RETURN;
    END
    SET @d1 = DATEADD(day, 1, DATEADD(week, -6, @act_date));

    SELECT ID_DISTRICT, C_ALL = COUNT(*)
    INTO #c_all
    FROM #cl
    WHERE ID_DISTRICT IS NOT NULL
    GROUP BY ID_DISTRICT;

    -- 2.3 Суммы чеков за окно — по ID_CHECK (не по ID_CONTACT_DATA: у части чеков с картой он равен голой дате, 19.10.2026)
    CREATE TABLE #check (ID_CHECK bigint NOT NULL, COST_DISCOUNT decimal(38, 4) NULL, INDEX cci CLUSTERED COLUMNSTORE);
    INSERT INTO #check WITH (TABLOCK) (ID_CHECK, COST_DISCOUNT)
    SELECT h.ID_CHECK, SUM(h.COST_DISCOUNT)
    FROM dbo.I_CHECKHEADER AS h WITH (NOLOCK)
    WHERE h.ID_COMPANY = @idc AND h.ID_ORGANIZATION = @org AND h.DATA BETWEEN @d1 AND @act_date AND h.ID_CONTACT <> 0
    GROUP BY h.ID_CHECK
    OPTION (RECOMPILE);

    -- 2.4 Скоры товара в округе
    SELECT sh.ID_DISTRICT,
           a.ID_PRODUCT,
           ID_CATEGORY_5 = MAX(a.ID_CATEGORY_5),
           FREQ_SUM      = SUM(CAST(b.FREQ_SCORE AS float)),
           BUDGET_SUM    = SUM(CAST(b.BUDGET_SCORE AS float)),
           CHECK_SCORE   = AVG(CAST(c.COST_DISCOUNT AS float) / 10000),
           CONTACTS      = COUNT(DISTINCT a.ID_CONTACT),
           CHECKS        = COUNT(a.ID_CHECK)
    INTO #res
    FROM dbo.I_CHECK AS a WITH (NOLOCK)
    JOIN #shop  AS sh ON sh.ID_SHOP = a.ID_SHOP
    JOIN #cl    AS b  ON b.ID_CONTACT = a.ID_CONTACT
    JOIN #check AS c  ON c.ID_CHECK = a.ID_CHECK
    WHERE a.ID_COMPANY = @idc AND a.ID_ORGANIZATION = @org AND a.DATA BETWEEN @d1 AND @act_date AND a.ID_PRODUCT IS NOT NULL
      AND a.ID_CONTACT <> 0
    GROUP BY sh.ID_DISTRICT, a.ID_PRODUCT
    OPTION (RECOMPILE);

    IF NOT EXISTS (SELECT 1 FROM #res)
    BEGIN
        RAISERROR(N'product_score_region: расчёт пустой — таблица не очищена, остаются прошлые скоры.', 16, 1);
        RETURN;
    END

    -- 2.5 Замена целиком одной транзакцией
    BEGIN TRAN;
        DELETE FROM dbo.I_PRODUCT_SCORE_REGION WHERE ID_COMPANY = @idc AND ID_ORGANIZATION = @org;

        INSERT dbo.I_PRODUCT_SCORE_REGION
              (ID_COMPANY, ID_ORGANIZATION, ID_DISTRICT, DISTRICT, ID_PRODUCT, ID_CATEGORY_5,
               FREQ_SCORE, LOYALTY_SCORE, CHECK_SCORE, CONTACTS, CHECKS, C_ALL, DATE_FROM, DATE_TO, CALC_DT)
        SELECT @idc, @org, r.ID_DISTRICT, d.DISTRICT, r.ID_PRODUCT, r.ID_CATEGORY_5,
               r.FREQ_SUM / NULLIF(ca.C_ALL, 0),
               r.BUDGET_SUM / NULLIF(ca.C_ALL, 0),
               r.CHECK_SCORE, r.CONTACTS, r.CHECKS, ca.C_ALL, @d1, @act_date, SYSDATETIME()
        FROM #res AS r
        JOIN (SELECT DISTINCT ID_DISTRICT, DISTRICT FROM #shop) AS d ON d.ID_DISTRICT = r.ID_DISTRICT
        LEFT JOIN #c_all AS ca ON ca.ID_DISTRICT = r.ID_DISTRICT;
    COMMIT;
END
GO

-- ============================== 3. ПЕРВЫЙ РАСЧЁТ И ИТОГ ==============================
-- I_CVM_CONTACT перезаливается в задании (TRUNCATE + INSERT) — пока задание идёт, не считаем
IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobactivity AS a WITH (NOLOCK)
           JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = a.job_id
           WHERE j.name = N'MCI Regular update'
             AND a.session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions WITH (NOLOCK))
             AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL)
    RAISERROR(N'СТОП: идёт «MCI Regular update» (I_CVM_CONTACT может перезаливаться) — первый расчёт не запущен, запусти скрипт после окончания.', 16, 1);
ELSE
    EXEC dbo.product_score_region;
GO

SELECT DISTRICT, C_ALL, products = COUNT(*), contacts_max = MAX(CONTACTS), checks = SUM(CHECKS),
       DATE_FROM = MIN(DATE_FROM), DATE_TO = MAX(DATE_TO), CALC_DT = MAX(CALC_DT)
FROM dbo.I_PRODUCT_SCORE_REGION WITH (NOLOCK)
GROUP BY DISTRICT, C_ALL
ORDER BY checks DESC;
GO

-- ============================== 4. ШАГ ЗАДАНИЯ ==============================
-- Шаг в «MCI Regular update» сразу после CVM_CONTACT; при успехе и при ошибке — к следующему шагу. sp_add_jobstep с
-- @step_id = (CVM_CONTACT + 1) сдвигает следующие шаги на 1 и сам пересчитывает их переходы. Повторный запуск шаг не задвоит.
USE msdb
GO
SET NOCOUNT ON;
DECLARE @job sysname = N'MCI Regular update';
DECLARE @cc int, @cc_ok int, @rc int = 0;

SELECT @cc = s.step_id, @cc_ok = s.on_success_action
FROM msdb.dbo.sysjobsteps AS s WITH (NOLOCK)
JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = s.job_id
WHERE j.name = @job AND s.step_name = N'CVM_CONTACT';

IF OBJECT_ID(N'mci_model.dbo.product_score_region', N'P') IS NULL OR OBJECT_ID(N'mci_model.dbo.I_PRODUCT_SCORE_REGION', N'U') IS NULL
BEGIN
    RAISERROR(N'СТОП: процедуры dbo.product_score_region или таблицы I_PRODUCT_SCORE_REGION в mci_model нет (верх скрипта не выполнился) — задание не изменено.', 16, 1);
END
ELSE IF NOT EXISTS (SELECT 1 FROM mci_model.dbo.I_PRODUCT_SCORE_REGION WITH (NOLOCK))
BEGIN
    RAISERROR(N'СТОП: первый расчёт не дал строк (секция 3 не выполнилась или упала) — задание не изменено. Исправь причину и запусти скрипт ещё раз.', 16, 1);
END
ELSE IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobsteps AS s WITH (NOLOCK) JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = s.job_id
                WHERE j.name = @job AND s.step_name = N'PRODUCT_SCORE_REGION')
BEGIN
    RAISERROR(N'Шаг PRODUCT_SCORE_REGION уже стоит в задании — задание не изменено.', 10, 1);
END
ELSE IF @cc IS NULL OR @cc_ok <> 3
BEGIN
    RAISERROR(N'СТОП: шаг CVM_CONTACT не найден или при успехе идёт не на следующий шаг — задание не изменено.', 16, 1);
END
ELSE IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobactivity AS a WITH (NOLOCK)
                JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = a.job_id
                WHERE j.name = @job
                  AND a.session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions WITH (NOLOCK))
                  AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL)
BEGIN
    RAISERROR(N'СТОП: задание «MCI Regular update» сейчас идёт — номера шагов не сдвигаю, задание не изменено. Запусти блок после окончания.', 16, 1);
END
ELSE
BEGIN
    SET @cc += 1;
    EXEC @rc = msdb.dbo.sp_add_jobstep @job_name = @job, @step_id = @cc, @step_name = N'PRODUCT_SCORE_REGION',
         @subsystem = N'TSQL', @database_name = N'mci_model', @command = N'exec dbo.product_score_region;',
         @on_success_action = 3, @on_fail_action = 3;
    IF (@@ERROR <> 0 OR @rc <> 0)
        RAISERROR(N'СТОП: шаг не добавлен (ошибка sp_add_jobstep) — задание не изменено.', 16, 1);
END
GO

SELECT s.step_id, s.step_name, s.command, s.on_success_action, s.on_success_step_id, s.on_fail_action, s.on_fail_step_id
FROM msdb.dbo.sysjobsteps AS s WITH (NOLOCK)
JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = s.job_id
WHERE j.name = N'MCI Regular update'
ORDER BY s.step_id;
GO
