-- ============================================================================================
-- CVM_CLUSTER_SCORE — трафик-скор и чек-скор КАТЕГОРИЙ и ТОВАРОВ по кластерам и их отклонение от всей базы,
--   ежедневный пересчёт (шаг «MCI Regular update»). Для выбора категорий и товаров под кластерные акции (19.10.2026).
-- ОДИН СКРИПТ: запустить целиком в SSMS (F5). Создаёт таблицы и процедуру, сразу считает (EXEC) и показывает итог,
--   затем добавляет шаг в задание (секция 4).
--
-- ПЕРЕЧЕНЬ ДЕЙСТВИЙ ПО ОБЪЕКТАМ
--   dbo.I_CLUSTER_CATEGORY_SCORE   создаётся (если нет): строка = организация × кластер × НАША категория (справочник CVM).
--                                  ≈ 11 × 650 строк; обычный кластерный ключ (ID_COMPANY, ID_ORGANIZATION, CLUSTER, ID_CVM_CATEGORY).
--   dbo.I_CLUSTER_PRODUCT_SCORE    создаётся (если нет): строка = организация × кластер × товар. ≈ 11 × 15 тыс. строк;
--                                  кластерный ключ (ID_COMPANY, ID_ORGANIZATION, CLUSTER, ID_PRODUCT).
--                                  Обе таблицы пересчитываются целиком каждый день одной транзакцией (очистка + вставка);
--                                  пустой расчёт таблицы не очищает (ошибка, остаются вчерашние скоры).
--                                  Уже есть без колонок FREQ_INDEX_LO / CHECK_INDEX_LO — колонки добавляются (ALTER TABLE ADD),
--                                  строки не трогаются; следующий расчёт их заполнит.
--   dbo.cluster_score              создаётся/заменяется: расчёт за 6 недель до даты I_CVM_CONTACT.
--   msdb: задание «MCI Regular update» — шаг CLUSTER_SCORE вставляется сразу после PRODUCT_SCORE_REGION; следующие шаги
--         сдвигаются на 1, переходы msdb пересчитывает сам; при успехе и при ошибке — следующий шаг.
--   НЕ ТРОГАЕТСЯ И НЕ УДАЛЯЕТСЯ: I_PRODUCT_SCORE, I_PRODUCT_SCORE_REGION, I_CLUSTER_CONTACT, I_CLUSTER_DICT,
--         I_CLUSTER_ASSIGN_LOG, I_CVM_PRODUCT_CATEGORY, I_CVM_CATEGORY, I_CVM_CATEGORY_MAP, I_PRODUCT, I_CHECK,
--         I_CHECKHEADER, I_CVM_CONTACT (только чтение, NOLOCK), остальные шаги задания.
--
-- ЗНАЧЕНИЯ
--   Окно покупок: 6 недель до REFERENCE_DATE в I_CVM_CONTACT (как у FREQ_SCORE клиента и у скоров товаров).
--   ID_COMPANY = 1, ID_ORGANIZATION = 1 (офлайн). ID_CONTACT = 0 не берётся. Сумма чека — COST_DISCOUNT I_CHECKHEADER по ID_CHECK.
--   Клиенты: I_CVM_CONTACT SEGMENT 1–4 (как в freq_score.sql и скорах товаров).
--   Кластер клиента: детальный уровень из 10 типов (I_CLUSTER_CONTACT.CLUSTER_DETAIL) последнего готового присвоения
--         офлайна — строка журнала I_CLUSTER_ASSIGN_LOG со STATUS = OK, после которой по тем же организации, окну и версии
--         нет строки RUN (перезапись). Номер типа — как на портале «Сегменты → Кластеры», название — из I_CLUSTER_DICT.
--         Прочитанных клиентов окна должно быть столько же, сколько в журнале (ROWS_), и у окна должен быть детальный
--         уровень — иначе ошибка, остаются вчерашние скоры.
--   CLUSTER = 0 — вся база: все клиенты SEGMENT 1–4, с кластером и без. Клиенты без кластера в строки кластеров 1–10 не входят.
--   C_ALL — клиентов SEGMENT 1–4 в кластере (в базе — всего).
--   Товары:
--     FREQ_SCORE (трафик-скор) = сумма FREQ_SCORE клиента (CVM_CONTACT) по строкам покупок товара клиентами кластера ÷ C_ALL;
--     CHECK_SCORE (чек-скор)   = среднее по тем же строкам (сумма чека ÷ 10000) — формулы freq_score.sql;
--     CONTACTS — разных клиентов кластера, купивших товар; CHECKS — чеков с товаром.
--   Категории (наш справочник CVM, I_CVM_PRODUCT_CATEGORY; «Несопоставленные» HOW = 0 не входят):
--     те же формулы, но чек с категорией считается ОДИН раз (5 йогуртов в чеке = один чек «Йогурты»), иначе категории
--     с большим ассортиментом росли бы сами по себе. FREQ_SCORE = сумма FREQ_SCORE клиента по чекам с категорией ÷ C_ALL;
--     CHECK_SCORE = средняя сумма чека с категорией ÷ 10000.
--   Отклонение от базы: FREQ_INDEX = FREQ_SCORE кластера ÷ FREQ_SCORE базы (CLUSTER = 0) той же категории/товара;
--     CHECK_INDEX — так же по CHECK_SCORE. 1,35 — на 35 % выше базы, 0,80 — на 20 % ниже. У строк базы индекс = 1.
--     Общий уровень кластера входит во все его индексы (у частых кластеров трафик выше по всему) — категории и товары
--     сравнивать по индексу ВНУТРИ кластера.
--   FREQ_INDEX_LO / CHECK_INDEX_LO — нижняя граница 95 % доверительного интервала индекса (решение Елены 20.10.2026,
--     по ней сортируются топы портрета): категория с горсткой покупателей даёт широкий интервал и уходит вниз сама,
--     без порога по числу покупателей. Трафик: скор кластера — среднее по клиентам кластера (у не покупавших 0) величины
--     «FREQ_SCORE клиента × его строк (у категорий — чеков) с товаром»; граница = среднее − 1,96 × стандартная ошибка
--     среднего, делённая на скор базы. Чек: средняя сумма чека с товаром − 1,96 × стандартная ошибка ПО ПОКУПАТЕЛЯМ
--     (чеки одного клиента — одна единица: десять похожих чеков одного человека не сужают интервал), делённая на чек-скор
--     базы; один покупатель — граница NULL (разброс не определён), в топе по убыванию такая строка внизу. Разброс базы
--     не учитывается — база в 8–14 раз больше кластера; у строк самой базы (CLUSTER = 0) нижняя граница = 1.
--   FREQ_RANK / CHECK_RANK — место в кластере 0–100 по скору (100 — лучшая категория/товар кластера), PERCENT_RANK.
--   EXT_CODES — коды ДИКСИ, которые целиком в категории; EXT_CODES_PART — коды, товары которых делятся между нашими
--     категориями по словам или поштучно (для условий акции — проверять состав).
--   Табак не входит (скидки запрещены): коды ДИКСИ 610901, 610902, 610904, 610905, 610906*, 70030101 — строки покупок табака
--     не считаются; в сумме чека они остаются (чек — целиком).
-- ============================================================================================

USE mci_model
GO

-- ============================== 1. ТАБЛИЦЫ ==============================
IF OBJECT_ID(N'dbo.I_CLUSTER_CATEGORY_SCORE', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.I_CLUSTER_CATEGORY_SCORE (
          ID_COMPANY        int            NOT NULL
        , ID_ORGANIZATION   int            NOT NULL
        , CLUSTER           tinyint        NOT NULL     -- 0 — вся база; 1–10 — тип детального уровня (I_CLUSTER_CONTACT.CLUSTER_DETAIL)
        , CLUSTER_NAME      nvarchar(100)  NULL         -- I_CLUSTER_DICT, уровень 2
        , ID_CVM_CATEGORY   int            NOT NULL
        , ID_CVM_GROUP      int            NULL
        , CATEGORY_NAME     nvarchar(200)  NULL
        , FREQ_SCORE        float          NULL         -- трафик-скор
        , CHECK_SCORE       float          NULL         -- чек-скор
        , FREQ_INDEX        float          NULL         -- трафик-скор кластера ÷ трафик-скор базы
        , CHECK_INDEX       float          NULL         -- чек-скор кластера ÷ чек-скор базы
        , FREQ_INDEX_LO     float          NULL         -- нижняя граница 95 % интервала FREQ_INDEX
        , CHECK_INDEX_LO    float          NULL         -- нижняя граница 95 % интервала CHECK_INDEX
        , FREQ_RANK         tinyint        NULL         -- место в кластере по трафик-скору, 0–100
        , CHECK_RANK        tinyint        NULL         -- место в кластере по чек-скору, 0–100
        , CONTACTS          int            NULL
        , CHECKS            int            NULL
        , C_ALL             int            NULL         -- клиентов SEGMENT 1–4 в кластере (знаменатель FREQ_SCORE)
        , EXT_CODES         nvarchar(max)  NULL
        , EXT_CODES_PART    nvarchar(max)  NULL
        , MODEL_VERSION     varchar(32)    NOT NULL     -- версия модели кластеров
        , CLUSTER_DATE      date           NOT NULL     -- конец окна присвоения кластеров
        , DATE_FROM         date           NOT NULL
        , DATE_TO           date           NOT NULL
        , CALC_DT           datetime2(0)   NOT NULL
        , CONSTRAINT PK_I_CLUSTER_CATEGORY_SCORE PRIMARY KEY CLUSTERED (ID_COMPANY, ID_ORGANIZATION, CLUSTER, ID_CVM_CATEGORY)
    );
END
GO

IF OBJECT_ID(N'dbo.I_CLUSTER_PRODUCT_SCORE', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.I_CLUSTER_PRODUCT_SCORE (
          ID_COMPANY        int            NOT NULL
        , ID_ORGANIZATION   int            NOT NULL
        , CLUSTER           tinyint        NOT NULL     -- 0 — вся база; 1–10 — тип детального уровня
        , CLUSTER_NAME      nvarchar(100)  NULL
        , ID_PRODUCT        bigint         NOT NULL
        , ID_CVM_CATEGORY   int            NULL         -- NULL — товара нет в справочнике CVM
        , FREQ_SCORE        float          NULL
        , CHECK_SCORE       float          NULL
        , FREQ_INDEX        float          NULL
        , CHECK_INDEX       float          NULL
        , FREQ_INDEX_LO     float          NULL
        , CHECK_INDEX_LO    float          NULL
        , FREQ_RANK         tinyint        NULL
        , CHECK_RANK        tinyint        NULL
        , CONTACTS          int            NULL
        , CHECKS            int            NULL
        , C_ALL             int            NULL
        , MODEL_VERSION     varchar(32)    NOT NULL
        , CLUSTER_DATE      date           NOT NULL
        , DATE_FROM         date           NOT NULL
        , DATE_TO           date           NOT NULL
        , CALC_DT           datetime2(0)   NOT NULL
        , CONSTRAINT PK_I_CLUSTER_PRODUCT_SCORE PRIMARY KEY CLUSTERED (ID_COMPANY, ID_ORGANIZATION, CLUSTER, ID_PRODUCT)
    );
END
GO

-- Таблицы первой редакции (19.10.2026) — без нижних границ индекса: добавить колонки
IF COL_LENGTH(N'dbo.I_CLUSTER_CATEGORY_SCORE', N'FREQ_INDEX_LO') IS NULL
    ALTER TABLE dbo.I_CLUSTER_CATEGORY_SCORE ADD FREQ_INDEX_LO float NULL, CHECK_INDEX_LO float NULL;
IF COL_LENGTH(N'dbo.I_CLUSTER_PRODUCT_SCORE', N'FREQ_INDEX_LO') IS NULL
    ALTER TABLE dbo.I_CLUSTER_PRODUCT_SCORE ADD FREQ_INDEX_LO float NULL, CHECK_INDEX_LO float NULL;
GO

-- ============================== 2. РАСЧЁТ ==============================
CREATE OR ALTER PROCEDURE dbo.cluster_score
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @idc int = 1, @org int = 1;
    DECLARE @act_date date, @d1 date, @cl_date date, @ver varchar(32), @n_log bigint, @n_read bigint, @n_det bigint;

    IF OBJECT_ID(N'dbo.I_CVM_PRODUCT_CATEGORY', N'U') IS NULL OR OBJECT_ID(N'dbo.I_CVM_CATEGORY', N'U') IS NULL
       OR OBJECT_ID(N'dbo.I_CVM_CATEGORY_MAP', N'U') IS NULL
    BEGIN
        RAISERROR(N'cluster_score: нет справочника категорий CVM (сначала CVM_CATEGORY_REFERENCE.sql) — скоры не пересчитаны.', 16, 1);
        RETURN;
    END

    -- 2.1 Последнее готовое присвоение кластеров офлайна
    SELECT TOP (1) @cl_date = l.REFERENCE_DATE, @ver = l.MODEL_VERSION, @n_log = l.ROWS_
    FROM dbo.I_CLUSTER_ASSIGN_LOG AS l WITH (NOLOCK)
    WHERE l.ID_ORGANIZATION = @org AND l.STATUS = 'OK'
      AND NOT EXISTS (SELECT 1 FROM dbo.I_CLUSTER_ASSIGN_LOG AS r WITH (NOLOCK)
                      WHERE r.ID_ORGANIZATION = l.ID_ORGANIZATION AND r.REFERENCE_DATE = l.REFERENCE_DATE
                        AND r.MODEL_VERSION = l.MODEL_VERSION AND r.STATUS = 'RUN' AND r.ID > l.ID)
    ORDER BY l.REFERENCE_DATE DESC, l.ID DESC;

    IF @cl_date IS NULL
    BEGIN
        RAISERROR(N'cluster_score: нет готового присвоения кластеров офлайна (I_CLUSTER_ASSIGN_LOG) — скоры не пересчитаны.', 16, 1);
        RETURN;
    END

    DROP TABLE IF EXISTS #ccl, #cl, #c_all, #prod, #check, #line, #pg, #pres, #cg, #cres, #codes;

    -- 2.2 Кластер клиента (одной строкой) и клиенты SEGMENT 1–4 со скором CVM_CONTACT.
    --     Всё окно присвоения читается целиком и сверяется по числу клиентов с журналом (ROWS_): чтение NOLOCK не должно
    --     попасть на окно, которое cluster_assign в этот момент переписывает (как сверка окна витрины в cluster_assign, 50306).
    CREATE TABLE #ccl (ID_CONTACT int NOT NULL, CLUSTER tinyint NULL, INDEX cci CLUSTERED COLUMNSTORE);
    INSERT INTO #ccl WITH (TABLOCK) (ID_CONTACT, CLUSTER)
    SELECT k.ID_CONTACT, MAX(k.CLUSTER_DETAIL)
    FROM dbo.I_CLUSTER_CONTACT AS k WITH (NOLOCK)
    WHERE k.ID_ORGANIZATION = @org AND k.REFERENCE_DATE = @cl_date AND k.MODEL_VERSION = @ver
    GROUP BY k.ID_CONTACT;

    SELECT @n_read = COUNT_BIG(*), @n_det = COUNT_BIG(CLUSTER) FROM #ccl;
    IF @n_log IS NULL OR @n_read <> @n_log
    BEGIN
        RAISERROR(N'cluster_score: окно кластеров прочиталось не полностью (клиентов не столько, сколько в журнале I_CLUSTER_ASSIGN_LOG) — скоры не пересчитаны, остаются прошлые.', 16, 1);
        RETURN;
    END
    IF @n_det = 0
    BEGIN
        RAISERROR(N'cluster_score: в последнем присвоении офлайна нет детального уровня (CLUSTER_DETAIL пуст) — скоры не пересчитаны, остаются прошлые.', 16, 1);
        RETURN;
    END

    CREATE TABLE #cl (ID_CONTACT int NOT NULL, FREQ_SCORE float NULL, REFERENCE_DATE date NULL, CLUSTER tinyint NULL,
                      INDEX cci CLUSTERED COLUMNSTORE);
    INSERT INTO #cl WITH (TABLOCK) (ID_CONTACT, FREQ_SCORE, REFERENCE_DATE, CLUSTER)
    SELECT a.ID_CONTACT, a.FREQ_SCORE, a.REFERENCE_DATE, k.CLUSTER
    FROM (SELECT ID_CONTACT, FREQ_SCORE = MAX(CAST(FREQ_SCORE AS float)), REFERENCE_DATE = MAX(REFERENCE_DATE)
          FROM dbo.I_CVM_CONTACT WITH (NOLOCK)
          WHERE ID_COMPANY = @idc AND ID_ORGANIZATION = @org AND SEGMENT IN (1, 2, 3, 4) AND ID_CONTACT <> 0
          GROUP BY ID_CONTACT) AS a
    LEFT JOIN #ccl AS k ON k.ID_CONTACT = a.ID_CONTACT;

    SET @act_date = (SELECT MAX(REFERENCE_DATE) FROM #cl);
    IF @act_date IS NULL
    BEGIN
        RAISERROR(N'cluster_score: в I_CVM_CONTACT нет клиентов SEGMENT 1–4 — скоры не пересчитаны, остаются прошлые.', 16, 1);
        RETURN;
    END
    SET @d1 = DATEADD(day, 1, DATEADD(week, -6, @act_date));

    SELECT CLUSTER = ISNULL(CAST(CLUSTER AS int), -1), C_ALL = COUNT(*)
    INTO #c_all
    FROM #cl
    GROUP BY CLUSTER
    UNION ALL
    SELECT 0, COUNT(*) FROM #cl;

    -- 2.3 Товары: наша категория, табак
    SELECT p.ID_PRODUCT,
           ID_CVM_CATEGORY = CASE WHEN pc.HOW <> 0 THEN pc.ID_CVM_CATEGORY END,
           IS_TOBACCO      = CAST(CASE WHEN LEFT(p.ID_CATEGORY_5_ext, 6) IN (N'610901', N'610902', N'610904', N'610905', N'610906')
                                        OR p.ID_CATEGORY_5_ext = N'70030101' THEN 1 ELSE 0 END AS bit)
    INTO #prod
    FROM (SELECT ID_PRODUCT, ID_CATEGORY_5_ext = MAX(ID_CATEGORY_5_ext)
          FROM dbo.I_PRODUCT WITH (NOLOCK)
          WHERE ID_COMPANY = @idc
          GROUP BY ID_PRODUCT) AS p
    LEFT JOIN dbo.I_CVM_PRODUCT_CATEGORY AS pc WITH (NOLOCK) ON pc.ID_PRODUCT = p.ID_PRODUCT;

    CREATE UNIQUE CLUSTERED INDEX ix ON #prod (ID_PRODUCT);

    -- 2.4 Суммы чеков за окно — по ID_CHECK
    CREATE TABLE #check (ID_CHECK bigint NOT NULL, COST_DISCOUNT float NULL, INDEX cci CLUSTERED COLUMNSTORE);
    INSERT INTO #check WITH (TABLOCK) (ID_CHECK, COST_DISCOUNT)
    SELECT h.ID_CHECK, SUM(CAST(h.COST_DISCOUNT AS float))
    FROM dbo.I_CHECKHEADER AS h WITH (NOLOCK)
    WHERE h.ID_COMPANY = @idc AND h.ID_ORGANIZATION = @org AND h.DATA BETWEEN @d1 AND @act_date AND h.ID_CONTACT <> 0
    GROUP BY h.ID_CHECK
    OPTION (RECOMPILE);

    -- 2.5 Чек × товар: строк покупки, клиент, кластер, скор клиента, сумма чека — один проход по I_CHECK
    CREATE TABLE #line (ID_CHECK bigint NOT NULL, ID_CONTACT int NOT NULL, CLUSTER tinyint NULL, ID_PRODUCT bigint NOT NULL,
                        ID_CVM_CATEGORY int NULL, N_LINES int NOT NULL, FREQ_SCORE float NULL, CHECK_SUM float NULL,
                        INDEX cci CLUSTERED COLUMNSTORE);
    INSERT INTO #line WITH (TABLOCK) (ID_CHECK, ID_CONTACT, CLUSTER, ID_PRODUCT, ID_CVM_CATEGORY, N_LINES, FREQ_SCORE, CHECK_SUM)
    SELECT a.ID_CHECK, a.ID_CONTACT, MAX(b.CLUSTER), a.ID_PRODUCT, MAX(p.ID_CVM_CATEGORY), COUNT(*),
           MAX(b.FREQ_SCORE), MAX(c.COST_DISCOUNT)
    FROM dbo.I_CHECK AS a WITH (NOLOCK)
    JOIN #cl    AS b ON b.ID_CONTACT = a.ID_CONTACT
    JOIN #check AS c ON c.ID_CHECK = a.ID_CHECK
    LEFT JOIN #prod AS p ON p.ID_PRODUCT = a.ID_PRODUCT
    WHERE a.ID_COMPANY = @idc AND a.ID_ORGANIZATION = @org AND a.DATA BETWEEN @d1 AND @act_date AND a.ID_PRODUCT IS NOT NULL
      AND a.ID_CONTACT <> 0 AND ISNULL(p.IS_TOBACCO, 0) = 0
    GROUP BY a.ID_CHECK, a.ID_CONTACT, a.ID_PRODUCT
    OPTION (RECOMPILE);

    -- 2.6 Товары: сначала клиент × товар (Y = FREQ_SCORE клиента × его строк товара — слагаемое трафик-скора; суммы и
    --     квадраты сумм чеков — для разброса чек-скора), потом кластер × товар; клиенты без кластера — группа NULL. Вся база
    --     (CLUSTER = 0) — сумма групп: клиент ровно в одной группе, суммы и квадраты складываются точно. Средний чек — как AVG:
    --     строки без суммы чека не считаются.
    SELECT c.CLUSTER, c.ID_PRODUCT,
           ID_CVM_CATEGORY = MAX(c.ID_CVM_CATEGORY),
           FREQ_SUM = SUM(c.Y), FREQ_SQ = SUM(c.Y * c.Y),
           CHK_SUM  = SUM(c.CHK_SUM), CHK_N = SUM(c.CHK_N),
           CHK_S2   = SUM(c.CHK_SUM * c.CHK_SUM), CHK_SN = SUM(c.CHK_SUM * c.CHK_N), CHK_N2 = SUM(c.CHK_N * c.CHK_N),
           CHK_K    = COUNT(c.CHK_SUM),
           CONTACTS = COUNT(*), CHECKS = SUM(c.CHECKS)
    INTO #pg
    FROM (SELECT l.CLUSTER, l.ID_PRODUCT, l.ID_CONTACT,
                 ID_CVM_CATEGORY = MAX(l.ID_CVM_CATEGORY),
                 Y       = SUM(l.N_LINES * l.FREQ_SCORE),
                 CHK_SUM = SUM(l.N_LINES * l.CHECK_SUM),
                 CHK_N   = CAST(SUM(CASE WHEN l.CHECK_SUM IS NOT NULL THEN l.N_LINES END) AS float),
                 CHECKS  = COUNT(*)
          FROM #line AS l
          GROUP BY l.CLUSTER, l.ID_PRODUCT, l.ID_CONTACT) AS c
    GROUP BY c.CLUSTER, c.ID_PRODUCT;

    SELECT g.CLUSTER, g.ID_PRODUCT, g.ID_CVM_CATEGORY, g.FREQ_SUM, g.FREQ_SQ, g.CHK_SUM, g.CHK_N,
           g.CHK_S2, g.CHK_SN, g.CHK_N2, g.CHK_K,
           CHECK_SCORE = g.CHK_SUM / NULLIF(g.CHK_N, 0) / 10000, g.CONTACTS, g.CHECKS
    INTO #pres
    FROM #pg AS g
    WHERE g.CLUSTER IS NOT NULL
    UNION ALL
    SELECT CAST(0 AS tinyint), g.ID_PRODUCT, MAX(g.ID_CVM_CATEGORY), SUM(g.FREQ_SUM), SUM(g.FREQ_SQ), SUM(g.CHK_SUM),
           SUM(g.CHK_N), SUM(g.CHK_S2), SUM(g.CHK_SN), SUM(g.CHK_N2), SUM(g.CHK_K),
           SUM(g.CHK_SUM) / NULLIF(SUM(g.CHK_N), 0) / 10000, SUM(g.CONTACTS), SUM(g.CHECKS)
    FROM #pg AS g
    GROUP BY g.ID_PRODUCT;

    -- 2.7 Категории: чек × категория один раз (подзапросом, без промежуточной таблицы на сотню миллионов строк), потом
    --     клиент × категория (Y = FREQ_SCORE клиента × его чеков с категорией), потом кластер × категория; база — сумма групп.
    SELECT c.CLUSTER, c.ID_CVM_CATEGORY,
           FREQ_SUM = SUM(c.Y), FREQ_SQ = SUM(c.Y * c.Y),
           CHK_SUM  = SUM(c.CHK_SUM), CHK_N = SUM(c.CHK_N),
           CHK_S2   = SUM(c.CHK_SUM * c.CHK_SUM), CHK_SN = SUM(c.CHK_SUM * c.CHK_N), CHK_N2 = SUM(c.CHK_N * c.CHK_N),
           CHK_K    = COUNT(c.CHK_SUM),
           CONTACTS = COUNT(*), CHECKS = SUM(c.CHECKS)
    INTO #cg
    FROM (SELECT x.CLUSTER, x.ID_CVM_CATEGORY, x.ID_CONTACT,
                 Y       = SUM(x.FREQ_SCORE),
                 CHK_SUM = SUM(x.CHECK_SUM),
                 CHK_N   = CAST(COUNT(x.CHECK_SUM) AS float),
                 CHECKS  = COUNT(*)
          FROM (SELECT l.ID_CHECK, l.ID_CONTACT, l.ID_CVM_CATEGORY, CLUSTER = MAX(l.CLUSTER),
                       FREQ_SCORE = MAX(l.FREQ_SCORE), CHECK_SUM = MAX(l.CHECK_SUM)
                FROM #line AS l
                WHERE l.ID_CVM_CATEGORY IS NOT NULL
                GROUP BY l.ID_CHECK, l.ID_CONTACT, l.ID_CVM_CATEGORY) AS x
          GROUP BY x.CLUSTER, x.ID_CVM_CATEGORY, x.ID_CONTACT) AS c
    GROUP BY c.CLUSTER, c.ID_CVM_CATEGORY;

    SELECT g.CLUSTER, g.ID_CVM_CATEGORY, g.FREQ_SUM, g.FREQ_SQ, g.CHK_SUM, g.CHK_N,
           g.CHK_S2, g.CHK_SN, g.CHK_N2, g.CHK_K,
           CHECK_SCORE = g.CHK_SUM / NULLIF(g.CHK_N, 0) / 10000, g.CONTACTS, g.CHECKS
    INTO #cres
    FROM #cg AS g
    WHERE g.CLUSTER IS NOT NULL
    UNION ALL
    SELECT CAST(0 AS tinyint), g.ID_CVM_CATEGORY, SUM(g.FREQ_SUM), SUM(g.FREQ_SQ), SUM(g.CHK_SUM), SUM(g.CHK_N),
           SUM(g.CHK_S2), SUM(g.CHK_SN), SUM(g.CHK_N2), SUM(g.CHK_K),
           SUM(g.CHK_SUM) / NULLIF(SUM(g.CHK_N), 0) / 10000, SUM(g.CONTACTS), SUM(g.CHECKS)
    FROM #cg AS g
    GROUP BY g.ID_CVM_CATEGORY;

    IF NOT EXISTS (SELECT 1 FROM #pres) OR NOT EXISTS (SELECT 1 FROM #cres)
    BEGIN
        RAISERROR(N'cluster_score: расчёт пустой (нет покупок или справочник CVM пуст) — таблицы не очищены, остаются прошлые скоры.', 16, 1);
        RETURN;
    END

    -- 2.8 Коды ДИКСИ категории: целиком — все товары кода в справочнике в этой одной категории; делящиеся — товары кода
    --     есть и в других категориях (по словам, поштучно или в «Несопоставленных»). По MAP_METHOD = 1 нельзя: товар кода
    --     «целиком», перенесённый поштучно (HOW = 3) в другую категорию, приносил бы туда весь код как «целиком».
    SELECT g.ID_CVM_CATEGORY,
           EXT_CODES      = STRING_AGG(CASE WHEN g.N_CAT = 1 THEN CAST(g.ID_CATEGORY_5_ext AS nvarchar(max)) END, N', ')
                                WITHIN GROUP (ORDER BY g.ID_CATEGORY_5_ext),
           EXT_CODES_PART = STRING_AGG(CASE WHEN g.N_CAT > 1 THEN CAST(g.ID_CATEGORY_5_ext AS nvarchar(max)) END, N', ')
                                WITHIN GROUP (ORDER BY g.ID_CATEGORY_5_ext)
    INTO #codes
    FROM (SELECT ID_CVM_CATEGORY, ID_CATEGORY_5_ext, HOW = MAX(HOW),
                 N_CAT = COUNT(*) OVER (PARTITION BY ID_CATEGORY_5_ext)
          FROM dbo.I_CVM_PRODUCT_CATEGORY WITH (NOLOCK)
          WHERE ID_CATEGORY_5_ext IS NOT NULL
          GROUP BY ID_CVM_CATEGORY, ID_CATEGORY_5_ext) AS g
    WHERE g.HOW <> 0
    GROUP BY g.ID_CVM_CATEGORY;

    -- 2.9 Замена целиком одной транзакцией
    BEGIN TRAN;
        DELETE FROM dbo.I_CLUSTER_CATEGORY_SCORE WHERE ID_COMPANY = @idc AND ID_ORGANIZATION = @org;

        INSERT dbo.I_CLUSTER_CATEGORY_SCORE
              (ID_COMPANY, ID_ORGANIZATION, CLUSTER, CLUSTER_NAME, ID_CVM_CATEGORY, ID_CVM_GROUP, CATEGORY_NAME,
               FREQ_SCORE, CHECK_SCORE, FREQ_INDEX, CHECK_INDEX, FREQ_INDEX_LO, CHECK_INDEX_LO, FREQ_RANK, CHECK_RANK,
               CONTACTS, CHECKS, C_ALL, EXT_CODES, EXT_CODES_PART, MODEL_VERSION, CLUSTER_DATE, DATE_FROM, DATE_TO, CALC_DT)
        SELECT @idc, @org, s.CLUSTER, d.CLUSTER_NAME, s.ID_CVM_CATEGORY, cat.ID_PARENT, cat.NAME,
               s.FREQ_SCORE, s.CHECK_SCORE,
               s.FREQ_SCORE / NULLIF(b.FREQ_SCORE, 0),
               s.CHECK_SCORE / NULLIF(b.CHECK_SCORE, 0),
               CASE WHEN s.CLUSTER = 0 THEN 1 ELSE s.FREQ_LO / NULLIF(b.FREQ_SCORE, 0) END,
               CASE WHEN s.CLUSTER = 0 THEN 1 ELSE s.CHECK_LO / NULLIF(b.CHECK_SCORE, 0) END,
               CAST(ROUND(100 * PERCENT_RANK() OVER (PARTITION BY s.CLUSTER ORDER BY s.FREQ_SCORE), 0) AS tinyint),
               CAST(ROUND(100 * PERCENT_RANK() OVER (PARTITION BY s.CLUSTER ORDER BY s.CHECK_SCORE), 0) AS tinyint),
               s.CONTACTS, s.CHECKS, s.C_ALL, cd.EXT_CODES, cd.EXT_CODES_PART, @ver, @cl_date, @d1, @act_date, SYSDATETIME()
        FROM (SELECT r.CLUSTER, r.ID_CVM_CATEGORY, FREQ_SCORE = r.FREQ_SUM / NULLIF(ca.C_ALL, 0), r.CHECK_SCORE,
                     FREQ_LO = r.FREQ_SUM / ca.C_ALL - 1.96 * SQRT(CASE WHEN r.FREQ_SQ / ca.C_ALL - SQUARE(r.FREQ_SUM / ca.C_ALL) > 0
                                                                         THEN (r.FREQ_SQ / ca.C_ALL - SQUARE(r.FREQ_SUM / ca.C_ALL)) / ca.C_ALL
                                                                         ELSE 0 END),
                     -- средний чек m = Σ S / Σ n по клиентам (S — суммы чеков клиента, n — его чеки/строки); дисперсия m
                     -- по клиентам: Σ (S − m·n)² / (Σ n)² · K/(K−1) = (Σ S² − 2m Σ S·n + m² Σ n²) / (Σ n)² · K/(K−1)
                     CHECK_LO = CASE WHEN r.CHK_K > 1 AND r.CHK_N > 0 THEN
                                  (r.CHK_SUM / r.CHK_N
                                   - 1.96 * SQRT(CASE WHEN (r.CHK_S2 - 2 * (r.CHK_SUM / r.CHK_N) * r.CHK_SN
                                                            + SQUARE(r.CHK_SUM / r.CHK_N) * r.CHK_N2) > 0
                                                      THEN (r.CHK_S2 - 2 * (r.CHK_SUM / r.CHK_N) * r.CHK_SN
                                                            + SQUARE(r.CHK_SUM / r.CHK_N) * r.CHK_N2)
                                                           / SQUARE(r.CHK_N) * r.CHK_K / (r.CHK_K - 1)
                                                      ELSE 0 END)) / 10000
                                END,
                     r.CONTACTS, r.CHECKS, ca.C_ALL
              FROM #cres AS r
              JOIN #c_all AS ca ON ca.CLUSTER = r.CLUSTER) AS s
        JOIN (SELECT r.ID_CVM_CATEGORY, FREQ_SCORE = r.FREQ_SUM / NULLIF(ca.C_ALL, 0), r.CHECK_SCORE
              FROM #cres AS r
              JOIN #c_all AS ca ON ca.CLUSTER = 0
              WHERE r.CLUSTER = 0) AS b ON b.ID_CVM_CATEGORY = s.ID_CVM_CATEGORY
        LEFT JOIN dbo.I_CVM_CATEGORY AS cat WITH (NOLOCK) ON cat.ID_CVM_CATEGORY = s.ID_CVM_CATEGORY
        LEFT JOIN dbo.I_CLUSTER_DICT AS d WITH (NOLOCK)
               ON d.MODEL_VERSION = @ver AND d.ID_ORGANIZATION = @org AND d.LEVEL_NO = 2 AND d.CLUSTER_ID = s.CLUSTER
        LEFT JOIN #codes AS cd ON cd.ID_CVM_CATEGORY = s.ID_CVM_CATEGORY;

        DELETE FROM dbo.I_CLUSTER_PRODUCT_SCORE WHERE ID_COMPANY = @idc AND ID_ORGANIZATION = @org;

        INSERT dbo.I_CLUSTER_PRODUCT_SCORE
              (ID_COMPANY, ID_ORGANIZATION, CLUSTER, CLUSTER_NAME, ID_PRODUCT, ID_CVM_CATEGORY,
               FREQ_SCORE, CHECK_SCORE, FREQ_INDEX, CHECK_INDEX, FREQ_INDEX_LO, CHECK_INDEX_LO, FREQ_RANK, CHECK_RANK,
               CONTACTS, CHECKS, C_ALL, MODEL_VERSION, CLUSTER_DATE, DATE_FROM, DATE_TO, CALC_DT)
        SELECT @idc, @org, s.CLUSTER, d.CLUSTER_NAME, s.ID_PRODUCT, s.ID_CVM_CATEGORY,
               s.FREQ_SCORE, s.CHECK_SCORE,
               s.FREQ_SCORE / NULLIF(b.FREQ_SCORE, 0),
               s.CHECK_SCORE / NULLIF(b.CHECK_SCORE, 0),
               CASE WHEN s.CLUSTER = 0 THEN 1 ELSE s.FREQ_LO / NULLIF(b.FREQ_SCORE, 0) END,
               CASE WHEN s.CLUSTER = 0 THEN 1 ELSE s.CHECK_LO / NULLIF(b.CHECK_SCORE, 0) END,
               CAST(ROUND(100 * PERCENT_RANK() OVER (PARTITION BY s.CLUSTER ORDER BY s.FREQ_SCORE), 0) AS tinyint),
               CAST(ROUND(100 * PERCENT_RANK() OVER (PARTITION BY s.CLUSTER ORDER BY s.CHECK_SCORE), 0) AS tinyint),
               s.CONTACTS, s.CHECKS, s.C_ALL, @ver, @cl_date, @d1, @act_date, SYSDATETIME()
        FROM (SELECT r.CLUSTER, r.ID_PRODUCT, r.ID_CVM_CATEGORY, FREQ_SCORE = r.FREQ_SUM / NULLIF(ca.C_ALL, 0), r.CHECK_SCORE,
                     FREQ_LO = r.FREQ_SUM / ca.C_ALL - 1.96 * SQRT(CASE WHEN r.FREQ_SQ / ca.C_ALL - SQUARE(r.FREQ_SUM / ca.C_ALL) > 0
                                                                         THEN (r.FREQ_SQ / ca.C_ALL - SQUARE(r.FREQ_SUM / ca.C_ALL)) / ca.C_ALL
                                                                         ELSE 0 END),
                     -- средний чек m = Σ S / Σ n по клиентам (S — суммы чеков клиента, n — его чеки/строки); дисперсия m
                     -- по клиентам: Σ (S − m·n)² / (Σ n)² · K/(K−1) = (Σ S² − 2m Σ S·n + m² Σ n²) / (Σ n)² · K/(K−1)
                     CHECK_LO = CASE WHEN r.CHK_K > 1 AND r.CHK_N > 0 THEN
                                  (r.CHK_SUM / r.CHK_N
                                   - 1.96 * SQRT(CASE WHEN (r.CHK_S2 - 2 * (r.CHK_SUM / r.CHK_N) * r.CHK_SN
                                                            + SQUARE(r.CHK_SUM / r.CHK_N) * r.CHK_N2) > 0
                                                      THEN (r.CHK_S2 - 2 * (r.CHK_SUM / r.CHK_N) * r.CHK_SN
                                                            + SQUARE(r.CHK_SUM / r.CHK_N) * r.CHK_N2)
                                                           / SQUARE(r.CHK_N) * r.CHK_K / (r.CHK_K - 1)
                                                      ELSE 0 END)) / 10000
                                END,
                     r.CONTACTS, r.CHECKS, ca.C_ALL
              FROM #pres AS r
              JOIN #c_all AS ca ON ca.CLUSTER = r.CLUSTER) AS s
        JOIN (SELECT r.ID_PRODUCT, FREQ_SCORE = r.FREQ_SUM / NULLIF(ca.C_ALL, 0), r.CHECK_SCORE
              FROM #pres AS r
              JOIN #c_all AS ca ON ca.CLUSTER = 0
              WHERE r.CLUSTER = 0) AS b ON b.ID_PRODUCT = s.ID_PRODUCT
        LEFT JOIN dbo.I_CLUSTER_DICT AS d WITH (NOLOCK)
               ON d.MODEL_VERSION = @ver AND d.ID_ORGANIZATION = @org AND d.LEVEL_NO = 2 AND d.CLUSTER_ID = s.CLUSTER;
    COMMIT;
END
GO

-- ============================== 3. ПЕРВЫЙ РАСЧЁТ И ИТОГ ==============================
-- I_CVM_CONTACT (TRUNCATE + INSERT) и чеки перезаливаются в задании — пока задание идёт, не считаем
-- (в самом задании шаг идёт после CVM_CONTACT по порядку, там проверка не нужна).
-- Справочника категорий CVM ещё нет (появится ночным шагом CVM_CATEGORY, он в задании раньше этого шага) — первый расчёт
-- пройдёт ночью в задании, шаг в секции 4 ставится.
IF OBJECT_ID(N'dbo.I_CVM_PRODUCT_CATEGORY', N'U') IS NULL OR OBJECT_ID(N'dbo.I_CVM_CATEGORY', N'U') IS NULL
   OR OBJECT_ID(N'dbo.I_CVM_CATEGORY_MAP', N'U') IS NULL
    RAISERROR(N'Справочника категорий CVM в базе ещё нет — первый расчёт пропущен, посчитается ночью шагом CLUSTER_SCORE.', 10, 1);
ELSE IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobactivity AS a WITH (NOLOCK)
           JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = a.job_id
           WHERE j.name = N'MCI Regular update'
             AND a.session_id = (SELECT MAX(session_id) FROM msdb.dbo.syssessions WITH (NOLOCK))
             AND a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL)
    RAISERROR(N'СТОП: идёт «MCI Regular update» (I_CVM_CONTACT и чеки могут перезаливаться) — первый расчёт не запущен, запусти скрипт после окончания.', 16, 1);
ELSE
    EXEC dbo.cluster_score;
GO

-- Итог по кластерам: клиентов, категорий и товаров, окна
SELECT c.CLUSTER, c.CLUSTER_NAME, c.C_ALL, categories = COUNT(*),
       products = (SELECT COUNT(*) FROM dbo.I_CLUSTER_PRODUCT_SCORE AS p WITH (NOLOCK)
                   WHERE p.ID_COMPANY = 1 AND p.ID_ORGANIZATION = 1 AND p.CLUSTER = c.CLUSTER),
       CLUSTER_DATE = MAX(c.CLUSTER_DATE), DATE_FROM = MIN(c.DATE_FROM), DATE_TO = MAX(c.DATE_TO), CALC_DT = MAX(c.CALC_DT)
FROM dbo.I_CLUSTER_CATEGORY_SCORE AS c WITH (NOLOCK)
WHERE c.ID_COMPANY = 1 AND c.ID_ORGANIZATION = 1
GROUP BY c.CLUSTER, c.CLUSTER_NAME, c.C_ALL
ORDER BY c.CLUSTER;
GO

-- Пример чтения: по 10 категорий с наибольшим отклонением трафик-скора от базы в каждом кластере
SELECT x.CLUSTER, x.CLUSTER_NAME, x.CATEGORY_NAME, x.FREQ_INDEX, x.CHECK_INDEX, x.FREQ_RANK, x.CHECK_RANK, x.CONTACTS, x.EXT_CODES
FROM (SELECT c.*, rn = ROW_NUMBER() OVER (PARTITION BY c.CLUSTER ORDER BY c.FREQ_INDEX DESC)
      FROM dbo.I_CLUSTER_CATEGORY_SCORE AS c WITH (NOLOCK)
      WHERE c.ID_COMPANY = 1 AND c.ID_ORGANIZATION = 1 AND c.CLUSTER <> 0) AS x
WHERE x.rn <= 10
ORDER BY x.CLUSTER, x.rn;
GO

-- ============================== 4. ШАГ ЗАДАНИЯ ==============================
-- Шаг в «MCI Regular update» сразу после PRODUCT_SCORE_REGION; при успехе и при ошибке — к следующему шагу. sp_add_jobstep
-- с @step_id = (PRODUCT_SCORE_REGION + 1) сдвигает следующие шаги на 1 и сам пересчитывает их переходы. Повторный запуск шаг
-- не задвоит. Пока задание идёт, номера шагов не сдвигаются.
USE msdb
GO
SET NOCOUNT ON;
DECLARE @job sysname = N'MCI Regular update';
DECLARE @ps int, @ps_ok int, @rc int = 0;

SELECT @ps = s.step_id, @ps_ok = s.on_success_action
FROM msdb.dbo.sysjobsteps AS s WITH (NOLOCK)
JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = s.job_id
WHERE j.name = @job AND s.step_name = N'PRODUCT_SCORE_REGION';

IF OBJECT_ID(N'mci_model.dbo.cluster_score', N'P') IS NULL OR OBJECT_ID(N'mci_model.dbo.I_CLUSTER_CATEGORY_SCORE', N'U') IS NULL
BEGIN
    RAISERROR(N'СТОП: процедуры dbo.cluster_score или таблицы I_CLUSTER_CATEGORY_SCORE в mci_model нет (верх скрипта не выполнился) — задание не изменено.', 16, 1);
END
ELSE IF OBJECT_ID(N'mci_model.dbo.I_CVM_PRODUCT_CATEGORY', N'U') IS NOT NULL
     AND NOT EXISTS (SELECT 1 FROM mci_model.dbo.I_CLUSTER_CATEGORY_SCORE WITH (NOLOCK))
BEGIN
    RAISERROR(N'СТОП: первый расчёт не дал строк (секция 3 упала) — задание не изменено. Исправь причину и запусти скрипт ещё раз.', 16, 1);
END
ELSE IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobsteps AS s WITH (NOLOCK) JOIN msdb.dbo.sysjobs AS j WITH (NOLOCK) ON j.job_id = s.job_id
                WHERE j.name = @job AND s.step_name = N'CLUSTER_SCORE')
BEGIN
    RAISERROR(N'Шаг CLUSTER_SCORE уже стоит в задании — задание не изменено.', 10, 1);
END
ELSE IF @ps IS NULL OR @ps_ok <> 3
BEGIN
    RAISERROR(N'СТОП: шаг PRODUCT_SCORE_REGION не найден или при успехе идёт не на следующий шаг — задание не изменено.', 16, 1);
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
    SET @ps += 1;
    EXEC @rc = msdb.dbo.sp_add_jobstep @job_name = @job, @step_id = @ps, @step_name = N'CLUSTER_SCORE',
         @subsystem = N'TSQL', @database_name = N'mci_model', @command = N'exec dbo.cluster_score;',
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
