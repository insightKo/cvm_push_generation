-- Т3 (бэклог ночного задания), контроль после ночного шага CLIENT_ITEM — только чтение, наша витрина + один день I_CHECK.
-- Когда: в тот же день — после того, как шаг CLIENT_ITEM прошёл (строка night в CVM_CLIENT_ITEM_LOG со STATUS = 'OK'), и до 01:00
-- (в 01:00 Reload перезаливает 10 дней I_CHECK, утром шаги задания меняют справочники — контроль покажет расхождение, которого нет).
-- Критерий «готово» Т3: день, который шаг не переписал, совпадает с тем, что дала бы полная перезапись.
-- Секции витрины сравниваются через копию во временную: EXCEPT прямо по витрине создал бы статистики по SUMMA/QTY на всю таблицу.

-- К0. Журнал: сколько дней не переписано (ERROR_MSG строки night), время загрузки и статистик
SELECT TOP (6) ID, MODE, STARTED, FINISHED, MIN_ = DATEDIFF(minute, STARTED, FINISHED), DATE_FROM, DATE_TO, DAYS_DONE,
       ROWS_, KEY_ROWS, STATUS, MSG = LEFT(ERROR_MSG, 200)
FROM dbo.CVM_CLIENT_ITEM_LOG WITH (NOLOCK)
ORDER BY ID DESC;

-- К1. Отпечатки дней (замер П9 — сколько дней меняет перезаливка): сравнить с прошлым снимком (Т3_замер_П9_*.txt).
--     Отпечаток — для замера, не доказательство совпадения (доказательство — К2, К3).
DECLARE @t date = CAST(GETDATE() AS date);
SELECT DATA, n = COUNT_BIG(*), s = SUM(SUMMA), q = SUM(QTY), mn = MIN(ID_CHECK), mx = MAX(ID_CHECK),
       ck = CHECKSUM_AGG(BINARY_CHECKSUM(ID_ORGANIZATION, ID_CONTACT, ID_CHECK, ID_PRODUCT, SUMMA, QTY, ID_CATEGORY_5_EXT))
FROM dbo.I_CLIENT_ITEM_DAY WITH (NOLOCK)
WHERE ID_COMPANY = 1 AND DATA BETWEEN DATEADD(day, -13, @t) AND DATEADD(day, -1, @t)
GROUP BY DATA ORDER BY DATA;

-- К2. Один день без перезаписи: товарные строки, как их собрала бы перезапись из I_CHECK (тот же запрос, что порция
--     client_item_load, те же типы), против секции витрины. Совпало — n_rewrite = n_mart, оба EXCEPT = 0.
DECLARE @d date = DATEADD(day, -5, CAST(GETDATE() AS date));     -- день из окна, не переписанный шагом (по К1/К0)
DECLARE @n_rw bigint, @n_mart bigint, @a bigint, @b bigint;
DROP TABLE IF EXISTS #k2_prod, #k2_new, #k2_old;
SELECT ID_PRODUCT = CAST(p.ID_PRODUCT AS bigint), EXT5 = MAX(TRY_CAST(p.ID_CATEGORY_5_ext AS bigint))
INTO #k2_prod
FROM dbo.I_PRODUCT AS p WITH (NOLOCK)
GROUP BY CAST(p.ID_PRODUCT AS bigint);
CREATE CLUSTERED COLUMNSTORE INDEX CCI_k2_prod ON #k2_prod;
CREATE TABLE #k2_new (ID_ORGANIZATION int NOT NULL, ID_CONTACT int NOT NULL, ID_CHECK bigint NOT NULL, ID_PRODUCT bigint NOT NULL,
                      SUMMA decimal(18, 2) NULL, QTY decimal(18, 3) NULL, EXT5 bigint NULL, INDEX CCI_k2_new CLUSTERED COLUMNSTORE);
INSERT INTO #k2_new WITH (TABLOCK) (ID_ORGANIZATION, ID_CONTACT, ID_CHECK, ID_PRODUCT, SUMMA, QTY, EXT5)
SELECT x.ID_ORGANIZATION, x.ID_CONTACT, x.ID_CHECK, x.ID_PRODUCT, x.SUMMA, x.QTY, p.EXT5
FROM (SELECT c.ID_ORGANIZATION, c.ID_CONTACT, c.ID_CHECK, ID_PRODUCT = CAST(c.ID_PRODUCT AS bigint),
             SUMMA = SUM(c.COST_DISCOUNT), QTY = SUM(c.QUANTITY)
      FROM dbo.I_CHECK AS c WITH (NOLOCK)
      WHERE c.ID_COMPANY = 1 AND c.ID_ORGANIZATION IN (1, 3) AND c.ID_CONTACT <> 0
        AND c.DATA >= @d AND c.DATA < DATEADD(day, 1, @d)
      GROUP BY c.ID_ORGANIZATION, c.ID_CONTACT, c.ID_CHECK, CAST(c.ID_PRODUCT AS bigint)) AS x
LEFT JOIN #k2_prod AS p ON p.ID_PRODUCT = x.ID_PRODUCT
OPTION (RECOMPILE);
CREATE TABLE #k2_old (ID_ORGANIZATION int NOT NULL, ID_CONTACT int NOT NULL, ID_CHECK bigint NOT NULL, ID_PRODUCT bigint NOT NULL,
                      SUMMA decimal(18, 2) NULL, QTY decimal(18, 3) NULL, EXT5 bigint NULL, INDEX CCI_k2_old CLUSTERED COLUMNSTORE);
INSERT INTO #k2_old WITH (TABLOCK) (ID_ORGANIZATION, ID_CONTACT, ID_CHECK, ID_PRODUCT, SUMMA, QTY, EXT5)
SELECT ID_ORGANIZATION, ID_CONTACT, ID_CHECK, ID_PRODUCT, SUMMA, QTY, ID_CATEGORY_5_EXT
FROM dbo.I_CLIENT_ITEM_DAY WITH (NOLOCK)
WHERE DATA = @d AND ID_COMPANY = 1 AND ID_CATEGORY_CVM IS NULL;
SELECT @n_rw = COUNT_BIG(*) FROM #k2_new;
SELECT @n_mart = COUNT_BIG(*) FROM dbo.I_CLIENT_ITEM_DAY WITH (NOLOCK) WHERE DATA = @d;
SELECT @a = COUNT_BIG(*) FROM (SELECT * FROM #k2_new EXCEPT SELECT * FROM #k2_old) AS x OPTION (RECOMPILE);
SELECT @b = COUNT_BIG(*) FROM (SELECT * FROM #k2_old EXCEPT SELECT * FROM #k2_new) AS x OPTION (RECOMPILE);
SELECT check_day = @d, n_rewrite = @n_rw, n_mart = @n_mart, rewrite_not_in_mart = @a, mart_not_in_rewrite = @b;
DROP TABLE IF EXISTS #k2_prod, #k2_new, #k2_old;

-- К3. Ключи того же дня: как их собрала бы перезапись из секции (запрос client_item_keys_day) против секции ключей.
--     Совпало — n_rewrite = n_mart, оба EXCEPT = 0. cat_ver_same = 1 — справочник категорий тот же, с которым ключи дня
--     собраны или сверены шагом (иначе расхождение по категориям ожидаемо и о шаге не говорит).
DECLARE @dk date = DATEADD(day, -5, CAST(GETDATE() AS date));
DECLARE @kn_rw bigint, @kn_mart bigint, @ka bigint, @kb bigint, @ver int;
DROP TABLE IF EXISTS #k3_cat, #k3_miss, #k3_new, #k3_old;
SELECT pc.ID_PRODUCT, ID_CAT = CAST(pc.ID_CVM_CATEGORY AS bigint)
     , CHK = CAST(SUBSTRING(HASHBYTES('SHA2_256', CAST(pc.ID_PRODUCT AS binary(8)) + CAST(pc.ID_CVM_CATEGORY AS binary(4))
                                                   + CAST(pc.[HOW] AS binary(1))), 1, 4) AS int)
INTO #k3_cat FROM dbo.I_CVM_PRODUCT_CATEGORY AS pc WITH (NOLOCK) WHERE pc.[HOW] > 0;
CREATE CLUSTERED COLUMNSTORE INDEX CCI_k3_cat ON #k3_cat;
SELECT @ver = ISNULL(CHECKSUM_AGG(CHK), 0) FROM #k3_cat;
SELECT DISTINCT EXT5 = TRY_CAST(m.ID_CATEGORY_5_ext AS bigint), m.ID_MISSION
INTO #k3_miss FROM dbo.I_MISSION AS m WITH (NOLOCK)
WHERE m.ID_COMPANY = 1 AND m.MISSION <> N'Служебное (не миссия)' AND m.ID_MISSION <> 999
  AND TRY_CAST(m.ID_CATEGORY_5_ext AS bigint) IS NOT NULL;
CREATE CLUSTERED COLUMNSTORE INDEX CCI_k3_miss ON #k3_miss;
CREATE TABLE #k3_new (ID_ORGANIZATION int NOT NULL, ID_CONTACT int NOT NULL, KEY_TYPE tinyint NOT NULL, ID_KEY bigint NOT NULL,
                      CHECKS int NOT NULL, SUMMA decimal(18, 2) NOT NULL, INDEX CCI_k3_new CLUSTERED COLUMNSTORE);
INSERT INTO #k3_new WITH (TABLOCK) (ID_ORGANIZATION, ID_CONTACT, KEY_TYPE, ID_KEY, CHECKS, SUMMA)
SELECT d.ID_ORGANIZATION, d.ID_CONTACT, CAST(1 AS tinyint), CAST(d.ID_PRODUCT AS bigint), COUNT(*), ISNULL(SUM(d.SUMMA), 0)
FROM dbo.I_CLIENT_ITEM_DAY AS d WITH (NOLOCK)
WHERE d.DATA = @dk AND d.ID_COMPANY = 1
GROUP BY d.ID_ORGANIZATION, d.ID_CONTACT, d.ID_PRODUCT
UNION ALL
SELECT g.ID_ORGANIZATION, g.ID_CONTACT, CAST(2 AS tinyint), g.ID_CAT, COUNT(*), ISNULL(SUM(g.SUMMA), 0)
FROM (SELECT d.ID_ORGANIZATION, d.ID_CONTACT, d.ID_CHECK, c.ID_CAT, SUMMA = SUM(d.SUMMA)
      FROM dbo.I_CLIENT_ITEM_DAY AS d WITH (NOLOCK)
      JOIN #k3_cat AS c ON c.ID_PRODUCT = d.ID_PRODUCT
      WHERE d.DATA = @dk AND d.ID_COMPANY = 1
      GROUP BY d.ID_ORGANIZATION, d.ID_CONTACT, d.ID_CHECK, c.ID_CAT) AS g
GROUP BY g.ID_ORGANIZATION, g.ID_CONTACT, g.ID_CAT
UNION ALL
SELECT g.ID_ORGANIZATION, g.ID_CONTACT, CAST(3 AS tinyint), CAST(g.ID_MISSION AS bigint), COUNT(*), ISNULL(SUM(g.SUMMA), 0)
FROM (SELECT d.ID_ORGANIZATION, d.ID_CONTACT, d.ID_CHECK, m.ID_MISSION, SUMMA = SUM(d.SUMMA)
      FROM dbo.I_CLIENT_ITEM_DAY AS d WITH (NOLOCK)
      JOIN #k3_miss AS m ON m.EXT5 = d.ID_CATEGORY_5_EXT
      WHERE d.DATA = @dk AND d.ID_COMPANY = 1
      GROUP BY d.ID_ORGANIZATION, d.ID_CONTACT, d.ID_CHECK, m.ID_MISSION) AS g
GROUP BY g.ID_ORGANIZATION, g.ID_CONTACT, g.ID_MISSION
OPTION (RECOMPILE);
CREATE TABLE #k3_old (ID_ORGANIZATION int NOT NULL, ID_CONTACT int NOT NULL, KEY_TYPE tinyint NOT NULL, ID_KEY bigint NOT NULL,
                      CHECKS int NOT NULL, SUMMA decimal(18, 2) NOT NULL, INDEX CCI_k3_old CLUSTERED COLUMNSTORE);
INSERT INTO #k3_old WITH (TABLOCK) (ID_ORGANIZATION, ID_CONTACT, KEY_TYPE, ID_KEY, CHECKS, SUMMA)
SELECT ID_ORGANIZATION, ID_CONTACT, KEY_TYPE, ID_KEY, CHECKS, SUMMA
FROM dbo.I_CLIENT_ITEM_KEY_DAY WITH (NOLOCK)
WHERE DATA = @dk AND ID_COMPANY = 1;
SELECT @kn_rw = COUNT_BIG(*) FROM #k3_new;
SELECT @kn_mart = COUNT_BIG(*) FROM dbo.I_CLIENT_ITEM_KEY_DAY WITH (NOLOCK) WHERE DATA = @dk;
SELECT @ka = COUNT_BIG(*) FROM (SELECT * FROM #k3_new EXCEPT SELECT * FROM #k3_old) AS x OPTION (RECOMPILE);
SELECT @kb = COUNT_BIG(*) FROM (SELECT * FROM #k3_old EXCEPT SELECT * FROM #k3_new) AS x OPTION (RECOMPILE);
SELECT check_day = @dk, n_rewrite = @kn_rw, n_mart = @kn_mart, rewrite_not_in_mart = @ka, mart_not_in_rewrite = @kb,
       cat_ver_now = @ver, cat_ver_day = v.CAT_VER, built_at = v.BUILT_AT,
       cat_ver_same = CASE WHEN v.CAT_VER = @ver THEN 1 ELSE 0 END
FROM (SELECT x = 1) AS one
LEFT JOIN dbo.CVM_CLIENT_ITEM_CAT_VER AS v WITH (NOLOCK) ON v.DATA = @dk;
-- есть разница — по типу ключа (у миссий версии нет: смена I_MISSION после шага даёт разницу в типе 3 и при cat_ver_same = 1)
IF @ka + @kb > 0
    SELECT side = 'rewrite_not_in_mart', KEY_TYPE, n = COUNT_BIG(*)
    FROM (SELECT * FROM #k3_new EXCEPT SELECT * FROM #k3_old) AS x GROUP BY KEY_TYPE
    UNION ALL
    SELECT 'mart_not_in_rewrite', KEY_TYPE, COUNT_BIG(*)
    FROM (SELECT * FROM #k3_old EXCEPT SELECT * FROM #k3_new) AS x GROUP BY KEY_TYPE
    OPTION (RECOMPILE);
DROP TABLE IF EXISTS #k3_cat, #k3_miss, #k3_new, #k3_old;
