-- ============================================================================================
-- CVM_PRODUCT_DEMAND — на какие товары вырос спрос в базе: витрина dbo.I_PRODUCT_DEMAND + процедура.
-- Запустить ОДИН РАЗ целиком в SSMS (F5). В конце — товары с наибольшим приростом покупателей.
--
-- Строка витрины = неделя × период × канал × регион × товар.
--   Период: 1 = неделя (пн–вс), 4 = последние 4 недели, закончившиеся этой неделей.
--   Канал: 1 офлайн, 3 e-commerce. Регион: регион отчёта (как в Общем отчёте), 0 = все регионы,
--   -1 = регион не определён. Офлайн — регион магазина; e-commerce — регион города заказа
--   (город → регион по справочнику офлайн-магазинов I_SHOP).
--   Спрос в базе = покупки по картам (ID_CONTACT <> 0), только LFL-магазины (I_LFL_SHOPS за месяц
--   конца периода; если списка за этот месяц ещё нет — последний имеющийся). Прошлый период и год назад
--   считаются на том же наборе магазинов.
--   Показатели: покупателей (уникальных клиентов), доля покупателей = покупатели товара / все покупатели
--   канала и региона за период, штук, ТО (COST_DISCOUNT), скидка (DISCOUNT), магазинов с продажами.
--   Сравнения: с прошлым периодом (предыдущая неделя / предыдущие 4 недели) и с годом назад
--   (те же дни недели 52 недели назад). Порогов и фильтров по товарам нет.
--
-- Процедура dbo.product_demand:
--   EXEC dbo.product_demand;                              — НОЧНОЙ: последняя закрытая неделя (закрыта, когда
--                                                           есть чеки и офлайна, и e-commerce за её воскресенье)
--                                                           + прошлые недели, посчитанные на списке LFL старше,
--                                                           чем доступен теперь; посчитанное не трогает
--   EXEC dbo.product_demand @WEEK_START = '2026-09-21';   — конкретная неделя (любая дата внутри недели)
--   EXEC dbo.product_demand @WEEK_START = ..., @FORCE = 1; — пересчитать уже посчитанную неделю
-- Ночной шаг джоба (после загрузки чеков):   EXEC dbo.product_demand;
-- Время: по замерам 09.10.2026 агрегат по товарам за неделю — от 4 с (чеки в памяти) до ~1 мин (с диска);
-- неделя целиком читает 30 недель I_CHECK — от нескольких минут до получаса, раз в неделю.
-- ============================================================================================

USE mci_model
GO

SET NOCOUNT ON;

-- ============================== 1. ВИТРИНА ==============================
IF OBJECT_ID('dbo.I_PRODUCT_DEMAND') IS NULL
BEGIN
    CREATE TABLE dbo.I_PRODUCT_DEMAND (
          ID_COMPANY                        int            NOT NULL
        , WEEK_START                        date           NOT NULL   -- понедельник последней недели периода
        , PERIOD_WEEKS                      tinyint        NOT NULL   -- 1 = неделя, 4 = последние 4 недели
        , [Период]                          nvarchar(60)   NOT NULL   -- «40 неделя (28.09.–04.10.)»
        , [Неделя]                          int            NOT NULL   -- ISO-номер последней недели периода
        , [Начало периода]                  date           NOT NULL
        , [Конец периода]                   date           NOT NULL
        , ID_ORGANIZATION                   int            NOT NULL
        , [Канал]                           nvarchar(20)   NOT NULL
        , ID_CITY_REPORT                    int            NOT NULL
        , [Регион]                          nvarchar(40)   NOT NULL
        , ID_PRODUCT                        bigint         NOT NULL
        , ID_PRODUCT_EXTERNAL               nvarchar(100)  NULL
        , [Товар]                           nvarchar(255)  NULL
        , [Бренд]                           nvarchar(30)   NULL
        , ID_CATEGORY_5_ext                 nvarchar(100)  NULL
        , [Категория]                       nvarchar(255)  NULL
        , [Категория 4]                     nvarchar(255)  NULL
        -- текущий период
        , [Покупателей]                     int            NOT NULL
        , [Доля покупателей, %]             decimal(9,4)   NULL
        , [Штук]                            decimal(18,3)  NULL
        , [ТО, р]                           decimal(18,2)  NULL
        , [Скидка, р]                       decimal(18,2)  NULL
        , [Магазинов]                       int            NOT NULL
        -- прошлый период
        , [Покупателей пред]                int            NOT NULL
        , [Доля пред, %]                    decimal(9,4)   NULL
        , [Штук пред]                       decimal(18,3)  NULL
        , [ТО пред, р]                      decimal(18,2)  NULL
        , [Скидка пред, р]                  decimal(18,2)  NULL
        , [Магазинов пред]                  int            NOT NULL
        -- год назад
        , [Покупателей год назад]           int            NOT NULL
        , [Доля год назад, %]               decimal(9,4)   NULL
        , [Штук год назад]                  decimal(18,3)  NULL
        , [ТО год назад, р]                 decimal(18,2)  NULL
        , [Скидка год назад, р]             decimal(18,2)  NULL
        , [Магазинов год назад]             int            NOT NULL
        -- изменения
        , [Прирост покупателей к пред]      int            NOT NULL
        , [Прирост покупателей к пред, %]   decimal(18,2)  NULL   -- пусто, если в прошлом периоде покупателей не было
        , [Прирост доли к пред, п.п.]       decimal(9,4)   NULL
        , [Прирост покупателей к году]      int            NOT NULL
        , [Прирост покупателей к году, %]   decimal(18,2)  NULL
        , [Прирост доли к году, п.п.]       decimal(9,4)   NULL
        , [Прирост ТО к пред, %]            decimal(18,2)  NULL
        , [Прирост ТО к году, %]            decimal(18,2)  NULL
        , LFL_YM                            int            NOT NULL   -- месяц списка LFL-магазинов, на котором посчитана неделя
        , LOAD_DATE                         datetime2(0)   NOT NULL
    );
    CREATE CLUSTERED COLUMNSTORE INDEX cci_I_PRODUCT_DEMAND ON dbo.I_PRODUCT_DEMAND;
END
GO

-- ============================== 2. ПРОЦЕДУРЫ ==============================
-- 2.1. Расчёт одной недели (вызывается из dbo.product_demand). Временные таблицы — #pd_*: процедура видит
--      временные таблицы вызвавшей сессии SSMS, общие имена (#agg, #shops…) задели бы рабочие таблицы Елены.
CREATE OR ALTER PROCEDURE dbo.product_demand_week
      @w      date   -- понедельник недели
    , @lfl_ym int    -- месяц списка LFL-магазинов
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @idc int = 1;

    -- периоды: текущий, прошлый, год назад (те же дни недели 52 недели назад)
    SELECT p.PERIOD_WEEKS, k.KIND
         , D_FROM = dateadd(day, k.SHIFT - 7 * (p.PERIOD_WEEKS - 1), @w)
         , D_TO   = dateadd(day, k.SHIFT + 6, @w)
    INTO #pd_periods
    FROM (VALUES (1), (4)) AS p (PERIOD_WEEKS)
         CROSS APPLY (VALUES ('cur', 0), ('prev', -7 * p.PERIOD_WEEKS), ('ly', -364)) AS k (KIND, SHIFT);

    -- LFL-магазины (список @lfl_ym) и их регион

    SELECT l.ID_SHOP, s.ID_ORGANIZATION, s.ID_CITY_REPORT
    INTO #pd_shops
    FROM dbo.I_LFL_SHOPS AS l (nolock)
         JOIN dbo.I_SHOP AS s (nolock) ON s.ID_SHOP = l.ID_SHOP AND s.ID_COMPANY = l.ID_COMPANY
    WHERE l.ID_COMPANY = @idc AND l.YEAR_month = @lfl_ym AND s.ID_ORGANIZATION IN (1, 3)
    GROUP BY l.ID_SHOP, s.ID_ORGANIZATION, s.ID_CITY_REPORT;
    CREATE UNIQUE CLUSTERED INDEX ix_shops ON #pd_shops (ID_SHOP);

    -- город → регион по офлайн-магазинам (для заказов e-commerce); город с двумя регионами не сопоставляется
    SELECT ID_CITY, ID_CITY_REPORT = min(ID_CITY_REPORT)
    INTO #pd_city_region
    FROM dbo.I_SHOP (nolock)
    WHERE ID_COMPANY = @idc AND ID_ORGANIZATION = 1 AND ID_CITY IS NOT NULL AND ID_CITY_REPORT IS NOT NULL
    GROUP BY ID_CITY
    HAVING min(ID_CITY_REPORT) = max(ID_CITY_REPORT);
    CREATE UNIQUE CLUSTERED INDEX ix_city ON #pd_city_region (ID_CITY);

    -- агрегаты по периодам: товар × канал × регион (регион 0 — все регионы) и база покупателей
    CREATE TABLE #pd_agg (PERIOD_WEEKS tinyint, KIND varchar(4), ID_ORGANIZATION int, ID_CITY_REPORT int, ID_PRODUCT bigint
                     , BUYERS int, QTY decimal(18,3), TURNOVER decimal(18,2), DISCOUNT decimal(18,2), SHOPS int);
    CREATE TABLE #pd_base (PERIOD_WEEKS tinyint, KIND varchar(4), ID_ORGANIZATION int, ID_CITY_REPORT int, BUYERS int);

    DECLARE @pw tinyint, @kind varchar(4), @d_from date, @d_to date;
    DECLARE cur_p CURSOR LOCAL FAST_FORWARD FOR SELECT PERIOD_WEEKS, KIND, D_FROM, D_TO FROM #pd_periods;
    OPEN cur_p;
    FETCH NEXT FROM cur_p INTO @pw, @kind, @d_from, @d_to;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- по регионам
        INSERT INTO #pd_agg
        SELECT @pw, @kind, a.ID_ORGANIZATION, isnull(isnull(s.ID_CITY_REPORT, r.ID_CITY_REPORT), -1), a.ID_PRODUCT
             , count(DISTINCT a.ID_CONTACT), sum(a.QUANTITY), sum(a.COST_DISCOUNT), sum(a.DISCOUNT), count(DISTINCT a.ID_SHOP)
        FROM dbo.I_CHECK AS a (nolock)
             JOIN #pd_shops AS s ON s.ID_SHOP = a.ID_SHOP
             LEFT JOIN #pd_city_region AS r ON a.ID_ORGANIZATION = 3 AND r.ID_CITY = a.ID_CITY
        WHERE a.ID_COMPANY = @idc AND a.ID_CONTACT <> 0
          AND a.ID_ORGANIZATION IN (1, 3)
          AND a.DATA >= @d_from AND a.DATA <= @d_to
        GROUP BY a.ID_ORGANIZATION, isnull(isnull(s.ID_CITY_REPORT, r.ID_CITY_REPORT), -1), a.ID_PRODUCT
        OPTION (RECOMPILE);

        -- все регионы (покупатель в двух регионах считается один раз)
        INSERT INTO #pd_agg
        SELECT @pw, @kind, a.ID_ORGANIZATION, 0, a.ID_PRODUCT
             , count(DISTINCT a.ID_CONTACT), sum(a.QUANTITY), sum(a.COST_DISCOUNT), sum(a.DISCOUNT), count(DISTINCT a.ID_SHOP)
        FROM dbo.I_CHECK AS a (nolock)
             JOIN #pd_shops AS s ON s.ID_SHOP = a.ID_SHOP
        WHERE a.ID_COMPANY = @idc AND a.ID_CONTACT <> 0
          AND a.ID_ORGANIZATION IN (1, 3)
          AND a.DATA >= @d_from AND a.DATA <= @d_to
        GROUP BY a.ID_ORGANIZATION, a.ID_PRODUCT
        OPTION (RECOMPILE);

        -- база: все покупатели канала и региона за период (по чекам, тот же периметр)
        INSERT INTO #pd_base
        SELECT @pw, @kind, h.ID_ORGANIZATION, isnull(isnull(s.ID_CITY_REPORT, r.ID_CITY_REPORT), -1), count(DISTINCT h.ID_CONTACT)
        FROM dbo.I_CHECKHEADER AS h (nolock)
             JOIN #pd_shops AS s ON s.ID_SHOP = h.ID_SHOP
             LEFT JOIN #pd_city_region AS r ON h.ID_ORGANIZATION = 3 AND r.ID_CITY = h.ID_CITY
        WHERE h.ID_COMPANY = @idc AND h.ID_CONTACT <> 0
          AND h.ID_ORGANIZATION IN (1, 3)
          AND h.DATA >= @d_from AND h.DATA <= @d_to
        GROUP BY h.ID_ORGANIZATION, isnull(isnull(s.ID_CITY_REPORT, r.ID_CITY_REPORT), -1)
        OPTION (RECOMPILE);

        INSERT INTO #pd_base
        SELECT @pw, @kind, h.ID_ORGANIZATION, 0, count(DISTINCT h.ID_CONTACT)
        FROM dbo.I_CHECKHEADER AS h (nolock)
             JOIN #pd_shops AS s ON s.ID_SHOP = h.ID_SHOP
        WHERE h.ID_COMPANY = @idc AND h.ID_CONTACT <> 0
          AND h.ID_ORGANIZATION IN (1, 3)
          AND h.DATA >= @d_from AND h.DATA <= @d_to
        GROUP BY h.ID_ORGANIZATION
        OPTION (RECOMPILE);

        FETCH NEXT FROM cur_p INTO @pw, @kind, @d_from, @d_to;
    END
    CLOSE cur_p;
    DEALLOCATE cur_p;

    CREATE CLUSTERED COLUMNSTORE INDEX cci_pd_agg ON #pd_agg;   -- большие #temp — колоночный индекс (правило 09.10.2026)

    -- справочник товаров для строк витрины
    SELECT DISTINCT ID_PRODUCT INTO #pd_prod FROM #pd_agg;

    SELECT p.ID_PRODUCT
         , i.ID_PRODUCT_EXTERNAL, i.PRODUCT_NAME, i.BRAND, i.ID_CATEGORY_5_ext
         , CAT5 = isnull(e.I_CATEGORY_5_ext, i.I_CATEGORY_5), CAT4 = i.I_CATEGORY_4
    INTO #pd_names
    FROM #pd_prod AS p
         LEFT JOIN dbo.I_PRODUCT AS i (nolock) ON i.ID_PRODUCT = p.ID_PRODUCT AND i.ID_COMPANY = @idc
         OUTER APPLY (SELECT TOP (1) x.I_CATEGORY_5_ext
                      FROM dbo.I_CATEGORY_EXT AS x (nolock)
                      WHERE x.ID_COMPANY = @idc AND x.ID_CATEGORY_5_ext = i.ID_CATEGORY_5_ext
                      ORDER BY x.I_CATEGORY_5_ext) AS e;

    SELECT ID_CITY_REPORT, CITY_REPORT = max(CITY_REPORT)
    INTO #pd_region_names
    FROM dbo.I_SHOP (nolock)
    WHERE ID_COMPANY = @idc AND ID_CITY_REPORT IS NOT NULL
    GROUP BY ID_CITY_REPORT;

    -- сборка строк: текущий, прошлый и год назад рядом
    SELECT g.PERIOD_WEEKS, g.ID_ORGANIZATION, g.ID_CITY_REPORT, g.ID_PRODUCT
         , B_CUR  = sum(IIF(g.KIND = 'cur',  g.BUYERS, 0)),   B_PREV  = sum(IIF(g.KIND = 'prev', g.BUYERS, 0)),   B_LY  = sum(IIF(g.KIND = 'ly', g.BUYERS, 0))
         , Q_CUR  = sum(IIF(g.KIND = 'cur',  g.QTY, NULL)),   Q_PREV  = sum(IIF(g.KIND = 'prev', g.QTY, NULL)),   Q_LY  = sum(IIF(g.KIND = 'ly', g.QTY, NULL))
         , T_CUR  = sum(IIF(g.KIND = 'cur',  g.TURNOVER, NULL)), T_PREV = sum(IIF(g.KIND = 'prev', g.TURNOVER, NULL)), T_LY = sum(IIF(g.KIND = 'ly', g.TURNOVER, NULL))
         , D_CUR  = sum(IIF(g.KIND = 'cur',  g.DISCOUNT, NULL)), D_PREV = sum(IIF(g.KIND = 'prev', g.DISCOUNT, NULL)), D_LY = sum(IIF(g.KIND = 'ly', g.DISCOUNT, NULL))
         , S_CUR  = sum(IIF(g.KIND = 'cur',  g.SHOPS, 0)),    S_PREV  = sum(IIF(g.KIND = 'prev', g.SHOPS, 0)),    S_LY  = sum(IIF(g.KIND = 'ly', g.SHOPS, 0))
    INTO #pd_rows
    FROM #pd_agg AS g
    GROUP BY g.PERIOD_WEEKS, g.ID_ORGANIZATION, g.ID_CITY_REPORT, g.ID_PRODUCT;

    BEGIN TRANSACTION;

    -- один запуск за раз: ночной шаг и ручной EXEC не задвоят неделю
    DECLARE @lock int;
    EXEC @lock = sp_getapplock @Resource = N'dbo.product_demand', @LockMode = 'Exclusive', @LockOwner = 'Transaction';
    IF @lock < 0
        THROW 50001, N'product_demand: не удалось взять блокировку записи витрины', 1;

    DELETE FROM dbo.I_PRODUCT_DEMAND WHERE ID_COMPANY = @idc AND WEEK_START = @w;

    INSERT INTO dbo.I_PRODUCT_DEMAND (
          ID_COMPANY, WEEK_START, PERIOD_WEEKS, [Период], [Неделя], [Начало периода], [Конец периода]
        , ID_ORGANIZATION, [Канал], ID_CITY_REPORT, [Регион]
        , ID_PRODUCT, ID_PRODUCT_EXTERNAL, [Товар], [Бренд], ID_CATEGORY_5_ext, [Категория], [Категория 4]
        , [Покупателей], [Доля покупателей, %], [Штук], [ТО, р], [Скидка, р], [Магазинов]
        , [Покупателей пред], [Доля пред, %], [Штук пред], [ТО пред, р], [Скидка пред, р], [Магазинов пред]
        , [Покупателей год назад], [Доля год назад, %], [Штук год назад], [ТО год назад, р], [Скидка год назад, р], [Магазинов год назад]
        , [Прирост покупателей к пред], [Прирост покупателей к пред, %], [Прирост доли к пред, п.п.]
        , [Прирост покупателей к году], [Прирост покупателей к году, %], [Прирост доли к году, п.п.]
        , [Прирост ТО к пред, %], [Прирост ТО к году, %]
        , LFL_YM, LOAD_DATE)
    SELECT @idc, @w, x.PERIOD_WEEKS
         , [Период] = IIF(x.PERIOD_WEEKS = 1
                         , concat(datepart(iso_week, @w), N' неделя (', convert(char(6), pc.D_FROM, 104), N'–', convert(char(6), pc.D_TO, 104), N')')
                         , concat(datepart(iso_week, pc.D_FROM), N'–', datepart(iso_week, @w), N' недели (', convert(char(6), pc.D_FROM, 104), N'–', convert(char(6), pc.D_TO, 104), N')'))
         , datepart(iso_week, @w), pc.D_FROM, pc.D_TO
         , x.ID_ORGANIZATION, IIF(x.ID_ORGANIZATION = 1, N'офлайн', N'e-commerce')
         , x.ID_CITY_REPORT
         , CASE x.ID_CITY_REPORT WHEN 0 THEN N'Все регионы' WHEN -1 THEN N'Регион не определён' ELSE isnull(rn.CITY_REPORT, cast(x.ID_CITY_REPORT AS nvarchar(10))) END
         , x.ID_PRODUCT, n.ID_PRODUCT_EXTERNAL, n.PRODUCT_NAME, n.BRAND, n.ID_CATEGORY_5_ext, n.CAT5, n.CAT4
         -- текущий
         , x.B_CUR,  100.0 * x.B_CUR  / nullif(bc.BUYERS, 0), x.Q_CUR,  x.T_CUR,  x.D_CUR,  x.S_CUR
         -- прошлый
         , x.B_PREV, 100.0 * x.B_PREV / nullif(bp.BUYERS, 0), x.Q_PREV, x.T_PREV, x.D_PREV, x.S_PREV
         -- год назад
         , x.B_LY,   100.0 * x.B_LY   / nullif(bl.BUYERS, 0), x.Q_LY,   x.T_LY,   x.D_LY,   x.S_LY
         -- изменения
         , x.B_CUR - x.B_PREV
         , 100.0 * (x.B_CUR - x.B_PREV) / nullif(x.B_PREV, 0)
         , 100.0 * x.B_CUR / nullif(bc.BUYERS, 0) - 100.0 * x.B_PREV / nullif(bp.BUYERS, 0)
         , x.B_CUR - x.B_LY
         , 100.0 * (x.B_CUR - x.B_LY) / nullif(x.B_LY, 0)
         , 100.0 * x.B_CUR / nullif(bc.BUYERS, 0) - 100.0 * x.B_LY / nullif(bl.BUYERS, 0)
         , 100.0 * (isnull(x.T_CUR, 0) - x.T_PREV) / nullif(x.T_PREV, 0)
         , 100.0 * (isnull(x.T_CUR, 0) - x.T_LY)   / nullif(x.T_LY, 0)
         , @lfl_ym, sysdatetime()
    FROM #pd_rows AS x
         JOIN #pd_periods AS pc ON pc.PERIOD_WEEKS = x.PERIOD_WEEKS AND pc.KIND = 'cur'
         LEFT JOIN #pd_base AS bc ON bc.PERIOD_WEEKS = x.PERIOD_WEEKS AND bc.KIND = 'cur'  AND bc.ID_ORGANIZATION = x.ID_ORGANIZATION AND bc.ID_CITY_REPORT = x.ID_CITY_REPORT
         LEFT JOIN #pd_base AS bp ON bp.PERIOD_WEEKS = x.PERIOD_WEEKS AND bp.KIND = 'prev' AND bp.ID_ORGANIZATION = x.ID_ORGANIZATION AND bp.ID_CITY_REPORT = x.ID_CITY_REPORT
         LEFT JOIN #pd_base AS bl ON bl.PERIOD_WEEKS = x.PERIOD_WEEKS AND bl.KIND = 'ly'   AND bl.ID_ORGANIZATION = x.ID_ORGANIZATION AND bl.ID_CITY_REPORT = x.ID_CITY_REPORT
         LEFT JOIN #pd_names AS n ON n.ID_PRODUCT = x.ID_PRODUCT
         LEFT JOIN #pd_region_names AS rn ON rn.ID_CITY_REPORT = x.ID_CITY_REPORT;

    COMMIT TRANSACTION;
END
GO

-- 2.2. Какие недели считать
CREATE OR ALTER PROCEDURE dbo.product_demand
      @WEEK_START date = NULL   -- любая дата недели; NULL = ночной режим
    , @FORCE      bit  = 0      -- 1 = пересчитать, даже если неделя уже посчитана на этом списке LFL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @idc int = 1;

    -- последний день, за который есть чеки и офлайна, и e-commerce; 07.01.1900 — воскресенье, 01.01.1900 — понедельник
    DECLARE @last date = (SELECT min(t.MX)
                          FROM (SELECT MX = max(DATA)
                                FROM dbo.I_CHECKHEADER (nolock)
                                WHERE ID_COMPANY = @idc AND ID_ORGANIZATION IN (1, 3)
                                GROUP BY ID_ORGANIZATION) AS t);
    DECLARE @last_monday date = dateadd(day, -6 - datediff(day, '19000107', @last) % 7, @last);   -- последняя закрытая неделя

    CREATE TABLE #pd_weeks (WEEK_START date PRIMARY KEY);

    IF @WEEK_START IS NOT NULL
    BEGIN
        DECLARE @w0 date = dateadd(day, -(datediff(day, '19000101', @WEEK_START) % 7), @WEEK_START);
        IF dateadd(day, 6, @w0) > @last
        BEGIN
            DECLARE @msg nvarchar(200) = concat(N'Неделя с ', convert(char(10), @w0, 104), N' ещё не закрыта: чеки есть по ', convert(char(10), @last, 104), N'.');
            RAISERROR(@msg, 16, 1);
            RETURN;
        END
        INSERT INTO #pd_weeks (WEEK_START) VALUES (@w0);
    END
    ELSE
    BEGIN
        -- НОЧНОЙ: последняя закрытая неделя + прошлые недели, посчитанные на списке LFL старше, чем доступен теперь
        INSERT INTO #pd_weeks (WEEK_START) VALUES (@last_monday);
        INSERT INTO #pd_weeks (WEEK_START)
        SELECT w.WEEK_START
        FROM (SELECT DISTINCT WEEK_START, LFL_YM
              FROM dbo.I_PRODUCT_DEMAND
              WHERE ID_COMPANY = @idc AND WEEK_START < @last_monday) AS w
             CROSS APPLY (SELECT BEST = max(l.YEAR_month)
                          FROM dbo.I_LFL_SHOPS AS l (nolock)
                          WHERE l.ID_COMPANY = @idc
                            AND l.YEAR_month <= year(dateadd(day, 6, w.WEEK_START)) * 100 + month(dateadd(day, 6, w.WEEK_START))) AS b
        WHERE w.LFL_YM < b.BEST
        GROUP BY w.WEEK_START;
    END

    DECLARE @w date, @lfl_ym int;
    WHILE EXISTS (SELECT 1 FROM #pd_weeks)
    BEGIN
        SELECT TOP (1) @w = WEEK_START FROM #pd_weeks ORDER BY WEEK_START DESC;   -- сначала свежая неделя
        DELETE FROM #pd_weeks WHERE WEEK_START = @w;

        -- список LFL — за месяц конца недели; если его ещё нет — последний имеющийся
        SET @lfl_ym = (SELECT max(YEAR_month) FROM dbo.I_LFL_SHOPS (nolock)
                       WHERE ID_COMPANY = @idc AND YEAR_month <= year(dateadd(day, 6, @w)) * 100 + month(dateadd(day, 6, @w)));

        IF @lfl_ym IS NULL   -- списка LFL на эту дату нет: неделю не трогаем (иначе DELETE сотрёт её без вставки)
            CONTINUE;
        IF @FORCE = 0 AND EXISTS (SELECT 1 FROM dbo.I_PRODUCT_DEMAND WHERE ID_COMPANY = @idc AND WEEK_START = @w AND LFL_YM >= @lfl_ym)
            CONTINUE;

        EXEC dbo.product_demand_week @w = @w, @lfl_ym = @lfl_ym;
    END
END
GO

-- ============================== 3. ПЕРВЫЙ ПРОГОН: последняя закрытая неделя ==============================
EXEC dbo.product_demand;
GO

-- ============================== 4. ИТОГ: где спрос вырос сильнее всего ==============================
-- Последние 4 недели к тем же 4 неделям год назад, все регионы, по 50 товаров на канал.
DECLARE @w date = (SELECT max(WEEK_START) FROM dbo.I_PRODUCT_DEMAND WHERE ID_COMPANY = 1);

SELECT t.[Период], t.[Канал], t.[Товар], t.[Категория], t.[Бренд]
     , t.[Покупателей], t.[Покупателей год назад], t.[Прирост покупателей к году], t.[Прирост покупателей к году, %]
     , t.[Доля покупателей, %], t.[Доля год назад, %], t.[Прирост доли к году, п.п.]
     , t.[Покупателей пред], t.[Прирост покупателей к пред, %]
     , t.[Магазинов], t.[Магазинов год назад], t.[ТО, р], t.[Прирост ТО к году, %], t.[Скидка, р]
FROM (SELECT d.*, RN = row_number() OVER (PARTITION BY d.ID_ORGANIZATION ORDER BY d.[Прирост покупателей к году] DESC)
      FROM dbo.I_PRODUCT_DEMAND AS d
      WHERE d.ID_COMPANY = 1 AND d.WEEK_START = @w AND d.PERIOD_WEEKS = 4 AND d.ID_CITY_REPORT = 0) AS t
WHERE t.RN <= 50
ORDER BY t.ID_ORGANIZATION, t.RN;
