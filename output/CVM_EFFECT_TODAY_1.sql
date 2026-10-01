-- CVM_EFFECT_TODAY · версия 1
-- Что изменено относительно текущей процедуры:
--   1. Эффект считается в разрезе сегмента клиента на момент отправки (I_PROMO_OFFER.SEGMENT):
--      Активные/Новые (1,2,3) · Случайные (4) · Отток (5) · Спящие (6) · плюс строка «Все» по акции целиком.
--   2. Препериод по группе: Активные/Новые — 7 нед. (выборка делается заранее), Случайные — 54 нед., Отток — 26 мес., Спящие — вся история до старта.
--      Препериод нормируется к длине акции; доп. ТО в акцию дополнительно считается как diff-in-diff (ЦГ−КГ с поправкой на «до»).
--   3. Стат-тесты: на ВСЕХ предложенных клиентах (не купил = 0), после IQR-чистки, три метрики: ARPU, DiD, трафик.
--   4. Исправлены: UPDATE I_PROMO без фильтра по акциям/компании; жёсткое ID_COMPANY=1 в #promo;
--      SHARE_BUDGET_AFTER; исключение выбросов из CLIENT_ALL без канала; неиспользуемый @act_date убран.
--
-- Порядок: файл целиком — ШАГ 0 (схема, backfill сегмента, полная чистка) → ALTER PROCEDURE → EXEC (все акции с 2025-09-01) → контроль.
-- В скриптах выборок (CVM_NN_26.sql) в #x добавить ПОСЛЕДНЕЙ колонкой SEGMENT из I_CVM_CONTACT —
-- INSERT INTO I_PROMO_OFFER SELECT * FROM #x позиционный.

USE [mci_model]
GO

-- ============================== ШАГ 0. Схема ==============================
IF COL_LENGTH('dbo.I_PROMO_OFFER', 'SEGMENT') IS NULL
    ALTER TABLE dbo.I_PROMO_OFFER ADD SEGMENT tinyint NULL;

IF COL_LENGTH('dbo.I_CVM_EFFECT_CG', 'SEGMENT_GROUP') IS NULL
    ALTER TABLE dbo.I_CVM_EFFECT_CG ADD
          SEGMENT_GROUP   nvarchar(20) NULL
        , CLIENTS_PRE     int NULL
        , TO_PRE          float NULL
        , TO_PRE_NORM     float NULL
        , BUDGET_PRE_NORM float NULL;

IF COL_LENGTH('dbo.I_CVM_EFFECT', 'Сегмент клиента') IS NULL
    ALTER TABLE dbo.I_CVM_EFFECT ADD
          [Сегмент клиента]        nvarchar(20) NULL
        , [ARPU ЦГ до р]           float NULL
        , [ARPU КГ до р]           float NULL
        , [Доп ТО в акцию DiD]     float NULL
        , [PL DiD]                 float NULL
        , [Стат значимость DiD]    nvarchar(60) NULL
        , [Эффект DiD]             nvarchar(20) NULL;
GO

-- ============================== ШАГ 0. Backfill сегмента для незакрытых акций ==============================
UPDATE o
SET SEGMENT = c.SEGMENT
FROM dbo.I_PROMO_OFFER AS o
     INNER JOIN dbo.I_PROMO AS p
     ON p.ID_PROMO = o.ID_PROMO AND p.ID_COMPANY = o.ID_COMPANY
     INNER JOIN dbo.I_CVM_CONTACT AS c (nolock)
     ON c.ID_CONTACT = o.ID_CONTACT AND c.ID_COMPANY = o.ID_COMPANY AND c.ID_ORGANIZATION = o.ID_ORGANIZATION
WHERE o.SEGMENT IS NULL
  AND p.START_DATE >= '2025-09-01'
  AND p.ID_PROMO LIKE '1010%';   -- backfill только по акциям 1010xx

-- контроль: сколько клиентов осталось без сегмента (попадут в группу «Без сегмента», препериод 7 нед.)
SELECT o.ID_PROMO, p.PROMO_NAME, CLIENTS = count(*), NO_SEGMENT = sum(iif(o.SEGMENT IS NULL, 1, 0))
FROM dbo.I_PROMO_OFFER AS o
     INNER JOIN dbo.I_PROMO AS p ON p.ID_PROMO = o.ID_PROMO AND p.ID_COMPANY = o.ID_COMPANY
WHERE p.START_DATE >= '2025-09-01' AND p.ID_PROMO LIKE '1010%'
GROUP BY o.ID_PROMO, p.PROMO_NAME
HAVING sum(iif(o.SEGMENT IS NULL, 1, 0)) > 0
ORDER BY o.ID_PROMO;
GO

-- ============================== ШАГ 0. Полная чистка отчётных таблиц ==============================
-- Все акции с сентября 2025 пересчитаются процедурой заново в новом формате (сегмент клиента, DiD, тесты на всех предложенных).
-- Для давно закрытых акций сегмент — сегодняшний из I_CVM_CONTACT, разрез по сегментам условный; строка «Все» корректна.
DELETE FROM dbo.I_CVM_EFFECT    WHERE [ID]     BETWEEN 100000 AND 111999 AND [Дата старта] >= '2025-09-01';
DELETE FROM dbo.I_CVM_EFFECT_CG WHERE ID_PROMO BETWEEN 100000 AND 111999 AND START_DATE    >= '2025-09-01';
GO

-- ============================== ПРОЦЕДУРА ==============================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[CVM_EFFECT_TODAY]
AS
SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;

DROP TABLE IF EXISTS #c;

SELECT ID_COMPANY, RN = ROW_NUMBER() OVER (ORDER BY ID_COMPANY)
INTO #c
FROM I_ORGANIZATION
GROUP BY ID_COMPANY;

DECLARE @c int = 1;
DECLARE @idc int = (SELECT ID_COMPANY FROM #c WHERE RN = @c);

WHILE @c <= (SELECT max(RN) FROM #c)
BEGIN

DECLARE @min_data date = (SELECT min(DATA) FROM I_CHECKHEADER (nolock) WHERE ID_COMPANY = @idc);
DECLARE @delta_min date = (SELECT min(DATA) FROM I_CHECK_DELTA (nolock) WHERE ID_COMPANY = @idc);

-- ---------- акции к пересчёту ----------
DROP TABLE IF EXISTS #promo;

SELECT ID_PROMO
INTO #promo
FROM I_PROMO (nolock)
WHERE START_DATE >= '2025-09-01'
  AND ID_COMPANY = @idc
  AND ID_PROMO BETWEEN 100000 AND 111999
  AND (   ID_PROMO NOT IN (SELECT ID FROM I_CVM_EFFECT (nolock) WHERE ID_COMPANY = @idc)
       OR FINISH_DATE_AFTER >= @delta_min)
GROUP BY ID_PROMO;

DELETE FROM I_CVM_EFFECT    WHERE ID       IN (SELECT ID_PROMO FROM #promo) AND ID_COMPANY = @idc;
DELETE FROM I_CVM_EFFECT_CG WHERE ID_PROMO IN (SELECT ID_PROMO FROM #promo) AND ID_COMPANY = @idc;

-- ---------- окна «после» (только по пересчитываемым акциям своей компании) ----------
UPDATE I_PROMO
SET START_DATE_AFTER  = dateadd(day, 1,   FINISH_DATE)
  , FINISH_DATE_AFTER = dateadd(day, 365, FINISH_DATE)
WHERE ID_SEGMENT = 13 AND ID_COMPANY = @idc AND ID_PROMO IN (SELECT ID_PROMO FROM #promo);

UPDATE I_PROMO
SET START_DATE_AFTER  = dateadd(day, 1, FINISH_DATE)
  , FINISH_DATE_AFTER = dateadd(day, 3, FINISH_DATE)
WHERE ID_PROMO LIKE '1010%' AND ID_COMPANY = @idc AND ID_PROMO IN (SELECT ID_PROMO FROM #promo);

-- окно «до» на уровне акции оставляем для совместимости; фактический препериод — по сегменту клиента (ниже)
UPDATE I_PROMO
SET FINISH_DATE_FROM = dateadd(day, -1, START_DATE)
WHERE ID_COMPANY = @idc AND ID_PROMO IN (SELECT ID_PROMO FROM #promo);

IF (SELECT max(ID_PROMO) FROM #promo) IS NOT NULL
BEGIN

-- ---------- база: предложенные клиенты × группа сегмента × окна ----------
-- строка с фактической группой + дубль с группой «Все» (итог по акции); препериод у клиента всегда свой
DROP TABLE IF EXISTS #base;

SELECT  a.ID_PROMO
      , a.ID_CONTACT
      , a.CONTROL_GROUP
      , a.ID_COMPANY
      , SEG_GROUP = g.SEG_GROUP
      , b.START_DATE
      , b.FINISH_DATE
      , b.START_DATE_AFTER
      , b.FINISH_DATE_AFTER
      -- начало препериода по группе; для Оттока/Спящих не раньше первой покупки клиента (иначе нормировка «на день» занижает базу)
      , PRE_START = CASE
                        WHEN a.SEGMENT IN (1, 2, 3) THEN dateadd(week,  -7,  b.START_DATE)
                        WHEN a.SEGMENT = 4          THEN dateadd(week,  -54, b.START_DATE)
                        WHEN a.SEGMENT = 5          THEN CASE WHEN min(cc.FIRST_DATA) > dateadd(month, -26, b.START_DATE)
                                                              THEN min(cc.FIRST_DATA) ELSE dateadd(month, -26, b.START_DATE) END
                        WHEN a.SEGMENT = 6          THEN isnull(min(cc.FIRST_DATA), @min_data)
                        ELSE dateadd(week, -7, b.START_DATE)
                    END
      , PRE_FINISH = dateadd(day, -1, b.START_DATE)
INTO #base
FROM  I_PROMO_OFFER AS a (nolock)
      INNER JOIN I_PROMO AS b (nolock)
      ON a.ID_PROMO = b.ID_PROMO AND a.ID_COMPANY = b.ID_COMPANY
      LEFT JOIN I_CVM_CONTACT AS cc (nolock)
      ON a.ID_CONTACT = cc.ID_CONTACT AND a.ID_COMPANY = cc.ID_COMPANY AND a.ID_ORGANIZATION = cc.ID_ORGANIZATION
      CROSS APPLY (SELECT SEG_GROUP = CASE
                                          WHEN a.SEGMENT IN (1, 2, 3) THEN N'Активные/Новые'
                                          WHEN a.SEGMENT = 4          THEN N'Случайные'
                                          WHEN a.SEGMENT = 5          THEN N'Отток'
                                          WHEN a.SEGMENT = 6          THEN N'Спящие'
                                          ELSE N'Без сегмента'
                                      END
                   UNION ALL SELECT N'Все') AS g
      LEFT JOIN I_MISTAKE_CONTACT AS d (nolock)
      ON a.ID_CONTACT = d.ID_CONTACT AND a.ID_COMPANY = d.ID_COMPANY
      LEFT JOIN (SELECT ID_COMPANY, ID_CONTACT FROM I_FRAUD_CONTACT (nolock) GROUP BY ID_COMPANY, ID_CONTACT) AS e
      ON a.ID_CONTACT = e.ID_CONTACT AND a.ID_COMPANY = e.ID_COMPANY
WHERE a.ID_COMPANY = @idc
  AND a.ID_PROMO IN (SELECT ID_PROMO FROM #promo)
  AND d.ID_CONTACT IS NULL
  AND e.ID_CONTACT IS NULL
GROUP BY a.ID_PROMO, a.ID_CONTACT, a.CONTROL_GROUP, a.ID_COMPANY, g.SEG_GROUP
       , b.START_DATE, b.FINISH_DATE, b.START_DATE_AFTER, b.FINISH_DATE_AFTER, a.SEGMENT;

-- препериод не раньше первой даты в базе и не короче 1 дня
UPDATE #base SET PRE_START = @min_data WHERE PRE_START < @min_data;
UPDATE #base SET PRE_START = PRE_FINISH WHERE PRE_START > PRE_FINISH;

-- ---------- чеки по трём периодам: -1 до, 0 в акцию, 1 после ----------
DROP TABLE IF EXISTS #chk;

SELECT  a.ID_PROMO
      , a.ID_CONTACT
      , a.CONTROL_GROUP
      , a.ID_COMPANY
      , a.SEG_GROUP
      , c.ID_ORGANIZATION
      , [PERIOD] = CASE WHEN c.DATA BETWEEN a.START_DATE       AND a.FINISH_DATE       THEN 0
                        WHEN c.DATA BETWEEN a.START_DATE_AFTER AND a.FINISH_DATE_AFTER THEN 1
                        ELSE -1 END
      , COST_DISCOUNT = sum(c.COST_DISCOUNT) + sum(c.DISCOUNT)
      , COUNT_CHECK   = count(c.ID_CHECK)
      , SKU           = sum(c.SKU)
      , REAL_DISCOUNT = sum(c.REAL_DISCOUNT)
      , DISCOUNT      = sum(c.DISCOUNT)
      , BONUS_PAY     = sum(c.BONUS_PAY)
      , BONUS_ACCRUAL = sum(c.BONUS_ACCRUAL)
INTO #chk
FROM  #base AS a
      INNER JOIN I_CHECKHEADER AS c (nolock)
      ON a.ID_CONTACT = c.ID_CONTACT
      AND a.ID_COMPANY = c.ID_COMPANY
      AND (   c.DATA BETWEEN a.PRE_START        AND a.PRE_FINISH
           OR c.DATA BETWEEN a.START_DATE       AND a.FINISH_DATE
           OR c.DATA BETWEEN a.START_DATE_AFTER AND a.FINISH_DATE_AFTER)
GROUP BY a.ID_PROMO, a.ID_CONTACT, a.CONTROL_GROUP, a.ID_COMPANY, a.SEG_GROUP, c.ID_ORGANIZATION
       , CASE WHEN c.DATA BETWEEN a.START_DATE       AND a.FINISH_DATE       THEN 0
              WHEN c.DATA BETWEEN a.START_DATE_AFTER AND a.FINISH_DATE_AFTER THEN 1
              ELSE -1 END;

-- ---------- выбросы (IQR × 1.96) в акцию: промо × канал × группа ----------
DROP TABLE IF EXISTS #nosale;

;WITH t AS (
    SELECT ID_PROMO, ID_ORGANIZATION, SEG_GROUP, ID_CONTACT, COST_DISCOUNT
         , RN  = ROW_NUMBER() OVER (PARTITION BY ID_PROMO, ID_ORGANIZATION, SEG_GROUP ORDER BY COST_DISCOUNT)
         , CNT = COUNT(*)     OVER (PARTITION BY ID_PROMO, ID_ORGANIZATION, SEG_GROUP)
    FROM #chk
    WHERE [PERIOD] = 0
),
q AS (
    SELECT ID_PROMO, ID_ORGANIZATION, SEG_GROUP
         , Q1_lower = max(CASE WHEN RN = floor  (0.25 * (CNT - 1) + 1) THEN COST_DISCOUNT END)
         , Q1_upper = max(CASE WHEN RN = ceiling(0.25 * (CNT - 1) + 1) THEN COST_DISCOUNT END)
         , Q3_lower = max(CASE WHEN RN = floor  (0.75 * (CNT - 1) + 1) THEN COST_DISCOUNT END)
         , Q3_upper = max(CASE WHEN RN = ceiling(0.75 * (CNT - 1) + 1) THEN COST_DISCOUNT END)
         , pos25 = 0.25 * (CNT - 1) + 1
         , pos75 = 0.75 * (CNT - 1) + 1
    FROM t
    GROUP BY ID_PROMO, ID_ORGANIZATION, SEG_GROUP, CNT
),
iq AS (
    SELECT ID_PROMO, ID_ORGANIZATION, SEG_GROUP
         , Q1 = CASE WHEN Q1_lower = Q1_upper THEN Q1_lower ELSE Q1_lower + (pos25 - floor(pos25)) * (Q1_upper - Q1_lower) END
         , Q3 = CASE WHEN Q3_lower = Q3_upper THEN Q3_lower ELSE Q3_lower + (pos75 - floor(pos75)) * (Q3_upper - Q3_lower) END
    FROM q
)
SELECT t.ID_PROMO, t.ID_CONTACT, t.ID_ORGANIZATION, t.SEG_GROUP
INTO #nosale
FROM t
     INNER JOIN iq
     ON t.ID_PROMO = iq.ID_PROMO AND t.ID_ORGANIZATION = iq.ID_ORGANIZATION AND t.SEG_GROUP = iq.SEG_GROUP
WHERE t.COST_DISCOUNT < iq.Q1 - 1.96 * (iq.Q3 - iq.Q1)
   OR t.COST_DISCOUNT > iq.Q3 + 1.96 * (iq.Q3 - iq.Q1);

-- выброс убираем целиком: из всех периодов и каналов и из знаменателя (CLIENT_ALL) — в той же группе
DELETE a
FROM #chk AS a
     INNER JOIN #nosale AS b
     ON a.ID_PROMO = b.ID_PROMO AND a.ID_CONTACT = b.ID_CONTACT AND a.SEG_GROUP = b.SEG_GROUP;

DELETE a
FROM #base AS a
     INNER JOIN #nosale AS b
     ON a.ID_PROMO = b.ID_PROMO AND a.ID_CONTACT = b.ID_CONTACT AND a.SEG_GROUP = b.SEG_GROUP;

-- ---------- выбросы «после» (как в исходнике — только из периода 1) ----------
DROP TABLE IF EXISTS #nosale1;

;WITH t AS (
    SELECT ID_PROMO, ID_ORGANIZATION, SEG_GROUP, ID_CONTACT, COST_DISCOUNT
         , RN  = ROW_NUMBER() OVER (PARTITION BY ID_PROMO, ID_ORGANIZATION, SEG_GROUP ORDER BY COST_DISCOUNT)
         , CNT = COUNT(*)     OVER (PARTITION BY ID_PROMO, ID_ORGANIZATION, SEG_GROUP)
    FROM #chk
    WHERE [PERIOD] = 1
),
q AS (
    SELECT ID_PROMO, ID_ORGANIZATION, SEG_GROUP
         , Q1_lower = max(CASE WHEN RN = floor  (0.25 * (CNT - 1) + 1) THEN COST_DISCOUNT END)
         , Q1_upper = max(CASE WHEN RN = ceiling(0.25 * (CNT - 1) + 1) THEN COST_DISCOUNT END)
         , Q3_lower = max(CASE WHEN RN = floor  (0.75 * (CNT - 1) + 1) THEN COST_DISCOUNT END)
         , Q3_upper = max(CASE WHEN RN = ceiling(0.75 * (CNT - 1) + 1) THEN COST_DISCOUNT END)
         , pos25 = 0.25 * (CNT - 1) + 1
         , pos75 = 0.75 * (CNT - 1) + 1
    FROM t
    GROUP BY ID_PROMO, ID_ORGANIZATION, SEG_GROUP, CNT
),
iq AS (
    SELECT ID_PROMO, ID_ORGANIZATION, SEG_GROUP
         , Q1 = CASE WHEN Q1_lower = Q1_upper THEN Q1_lower ELSE Q1_lower + (pos25 - floor(pos25)) * (Q1_upper - Q1_lower) END
         , Q3 = CASE WHEN Q3_lower = Q3_upper THEN Q3_lower ELSE Q3_lower + (pos75 - floor(pos75)) * (Q3_upper - Q3_lower) END
    FROM q
)
SELECT t.ID_PROMO, t.ID_CONTACT, t.ID_ORGANIZATION, t.SEG_GROUP
INTO #nosale1
FROM t
     INNER JOIN iq
     ON t.ID_PROMO = iq.ID_PROMO AND t.ID_ORGANIZATION = iq.ID_ORGANIZATION AND t.SEG_GROUP = iq.SEG_GROUP
WHERE t.COST_DISCOUNT < iq.Q1 - 1.96 * (iq.Q3 - iq.Q1)
   OR t.COST_DISCOUNT > iq.Q3 + 1.96 * (iq.Q3 - iq.Q1);

DELETE a
FROM #chk AS a
     INNER JOIN #nosale1 AS b
     ON a.ID_PROMO = b.ID_PROMO AND a.ID_ORGANIZATION = b.ID_ORGANIZATION
     AND a.ID_CONTACT = b.ID_CONTACT AND a.SEG_GROUP = b.SEG_GROUP
WHERE a.[PERIOD] = 1;

-- ---------- клиент-уровень для стат-тестов: все предложенные, не купил = 0 ----------
-- препериод нормирован к длине акции: TO_PRE_NORM = TO_PRE × дни_акции / дни_препериода
DROP TABLE IF EXISTS #cl;

SELECT  b.ID_PROMO
      , b.SEG_GROUP
      , b.CONTROL_GROUP
      , b.ID_CONTACT
      , TO_IN       = isnull(i.TO_IN, 0.0)
      , BUY_IN      = iif(i.TO_IN IS NULL, 0.0, 1.0)
      , TO_PRE      = isnull(p.TO_PRE, 0.0)
      , TO_PRE_NORM = isnull(p.TO_PRE, 0.0) * (datediff(day, b.START_DATE, b.FINISH_DATE) + 1) * 1.0
                      / nullif(datediff(day, b.PRE_START, b.PRE_FINISH) + 1, 0)
INTO #cl
FROM  #base AS b
      LEFT JOIN (SELECT ID_PROMO, SEG_GROUP, CONTROL_GROUP, ID_CONTACT, TO_IN = sum(COST_DISCOUNT)
                 FROM #chk WHERE [PERIOD] = 0
                 GROUP BY ID_PROMO, SEG_GROUP, CONTROL_GROUP, ID_CONTACT) AS i
      ON b.ID_PROMO = i.ID_PROMO AND b.SEG_GROUP = i.SEG_GROUP AND b.CONTROL_GROUP = i.CONTROL_GROUP AND b.ID_CONTACT = i.ID_CONTACT
      LEFT JOIN (SELECT ID_PROMO, SEG_GROUP, CONTROL_GROUP, ID_CONTACT, TO_PRE = sum(COST_DISCOUNT)
                 FROM #chk WHERE [PERIOD] = -1
                 GROUP BY ID_PROMO, SEG_GROUP, CONTROL_GROUP, ID_CONTACT) AS p
      ON b.ID_PROMO = p.ID_PROMO AND b.SEG_GROUP = p.SEG_GROUP AND b.CONTROL_GROUP = p.CONTROL_GROUP AND b.ID_CONTACT = p.ID_CONTACT;

-- три метрики в одной таблице: ARPU (ТО в акцию), DID (ТО в акцию − нормированное «до»), TRAF (купил/нет)
DROP TABLE IF EXISTS #m;

SELECT ID_PROMO, SEG_GROUP, CONTROL_GROUP, METRIC, X
INTO #m
FROM #cl
     CROSS APPLY (VALUES ('ARPU', TO_IN), ('DID', TO_IN - TO_PRE_NORM), ('TRAF', BUY_IN)) AS v(METRIC, X)
WHERE CONTROL_GROUP IN (0, 1);

-- ---------- t-тест Уэлча по каждой метрике ----------
DROP TABLE IF EXISTS #stat;

;WITH s AS (
    SELECT ID_PROMO, SEG_GROUP, METRIC, CONTROL_GROUP
         , n = count(*), mean_x = avg(X), var_x = var(X), stdev_x = stdev(X)
    FROM #m
    GROUP BY ID_PROMO, SEG_GROUP, METRIC, CONTROL_GROUP
),
cs AS (
    SELECT s0.ID_PROMO, s0.SEG_GROUP, s0.METRIC
         , n_control = s1.n, mean_control = s1.mean_x, var_control = s1.var_x, stdev_control = s1.stdev_x
         , n_test    = s0.n, mean_test    = s0.mean_x, var_test    = s0.var_x, stdev_test    = s0.stdev_x
         , mean_diff = s0.mean_x - s1.mean_x
         , se_diff   = CASE WHEN s0.n > 1 AND s1.n > 1 AND s0.var_x IS NOT NULL AND s1.var_x IS NOT NULL
                                 AND s0.var_x / s0.n + s1.var_x / s1.n > 0
                            THEN sqrt(s0.var_x / s0.n + s1.var_x / s1.n) END
    FROM s AS s1
         INNER JOIN s AS s0
         ON s1.ID_PROMO = s0.ID_PROMO AND s1.SEG_GROUP = s0.SEG_GROUP AND s1.METRIC = s0.METRIC
         AND s1.CONTROL_GROUP = 1 AND s0.CONTROL_GROUP = 0
)
SELECT ID_PROMO, SEG_GROUP, METRIC
     , n_control, n_test, mean_control, mean_test, mean_diff, stdev_control, stdev_test, se_diff
     , t_stat_welch = CASE WHEN se_diff IS NOT NULL AND abs(se_diff) > 1e-10 THEN mean_diff / se_diff END
     , ci_lower_95  = mean_diff - 1.96 * se_diff
     , ci_upper_95  = mean_diff + 1.96 * se_diff
     , relative_diff_pct = CASE WHEN abs(mean_control) > 1e-10 THEN 100.0 * mean_diff / mean_control END
     , sample_size_note  = CASE WHEN n_control < 30 OR n_test < 30 THEN N'Маленькие группы - интерпретируйте с осторожностью'
                                ELSE N'Достаточный размер групп' END
INTO #stat
FROM cs;

DROP TABLE IF EXISTS #interpret;

SELECT ID_PROMO, SEG_GROUP, METRIC, t_stat_welch, sample_size_note
     , significance_level = CASE
           WHEN t_stat_welch IS NULL                         THEN N'Проверьте данные'
           WHEN abs(t_stat_welch) < 1.65                     THEN N'Незначимо (вероятность > 10%)'
           WHEN abs(t_stat_welch) BETWEEN 1.65 AND 1.96      THEN N'Слабо значимо (p ≈ 0.05-0.10)'
           WHEN abs(t_stat_welch) BETWEEN 1.96 AND 2.58      THEN N'Значимо (p < 0.05)'
           WHEN abs(t_stat_welch) BETWEEN 2.58 AND 3.29      THEN N'Сильно значимо (p < 0.01)'
           WHEN abs(t_stat_welch) >= 3.29                    THEN N'Очень сильно значимо (p < 0.001)'
           ELSE N'Проверьте данные' END
     , effect_direction = CASE WHEN t_stat_welch > 0 THEN N'Тест > Контроль'
                               WHEN t_stat_welch < 0 THEN N'Тест < Контроль'
                               ELSE N'Нет разницы' END
INTO #interpret
FROM #stat;

-- ---------- агрегаты по периодам: промо × канал × группа × КГ ----------
DROP TABLE IF EXISTS #t;

SELECT  a.ID_ORGANIZATION
      , a.ID_COMPANY
      , a.ID_PROMO
      , a.SEG_GROUP
      , a.CONTROL_GROUP
      , a.[PERIOD]
      , CLIENTS       = count(a.ID_CONTACT)
      , COST_DISCOUNT = sum(a.COST_DISCOUNT)
      , COUNT_CHECK   = sum(a.COUNT_CHECK)
      , SKU           = sum(a.SKU)
      , REAL_DISCOUNT = sum(a.REAL_DISCOUNT)
      , DISCOUNT      = sum(a.DISCOUNT)
      , BONUS_PAY     = sum(a.BONUS_PAY)
      , BONUS_ACCRUAL = sum(a.BONUS_ACCRUAL)
      , BUDGET           = sum(a.COST_DISCOUNT) / count(a.ID_CONTACT)
      , AVG_CHECK        = sum(a.COST_DISCOUNT) / nullif(sum(a.COUNT_CHECK), 0)
      , CHECK_PER_CLIENT = sum(a.COUNT_CHECK) * 1.00 / count(a.ID_CONTACT)
      , AVG_COST_SKU     = sum(a.COST_DISCOUNT) / nullif(sum(a.SKU), 0)
      , AVG_SKU          = sum(a.SKU) * 1.00 / nullif(sum(a.COUNT_CHECK), 0)
INTO #t
FROM #chk AS a
GROUP BY a.ID_ORGANIZATION, a.ID_COMPANY, a.ID_PROMO, a.SEG_GROUP, a.CONTROL_GROUP, a.[PERIOD];

-- препериод в нормированном виде (сумма по клиентам) — для DiD на уровне агрегата
DROP TABLE IF EXISTS #pre_norm;

SELECT ID_PROMO, SEG_GROUP, CONTROL_GROUP, TO_PRE_NORM = sum(TO_PRE_NORM)
INTO #pre_norm
FROM #cl
GROUP BY ID_PROMO, SEG_GROUP, CONTROL_GROUP;

-- ---------- знаменатель: все предложенные (без выбросов) ----------
DROP TABLE IF EXISTS #all_cl;

SELECT ID_PROMO, SEG_GROUP, CONTROL_GROUP, ID_COMPANY, CLIENT_ALL = count(DISTINCT ID_CONTACT)
INTO #all_cl
FROM #base
GROUP BY ID_PROMO, SEG_GROUP, CONTROL_GROUP, ID_COMPANY;

-- ---------- эффект по ЦГ/КГ ----------
DROP TABLE IF EXISTS #effect;

SELECT  b.ID_ORGANIZATION
      , b.ID_COMPANY
      , b.ID_PROMO
      , b.CONTROL_GROUP
      , p.START_DATE
      , p.FINISH_DATE
      , d.CLIENT_ALL
      , CLIENTS_IN    = b.CLIENTS
      , CLIENTS_AFTER = c.CLIENTS
      , SHARE_IN      = b.CLIENTS * 1.00 / d.CLIENT_ALL
      , SHARE_AFTER   = c.CLIENTS * 1.00 / b.CLIENTS
      , [TO_IN]       = b.COST_DISCOUNT
      , [TO_AFTER]    = c.COST_DISCOUNT
      , SHARE_TO_AFTER = c.COST_DISCOUNT / b.COST_DISCOUNT
      , [BUDGET_IN]    = b.BUDGET
      , [BUDGET_AFTER] = c.BUDGET
      , SHARE_BUDGET_AFTER = c.BUDGET / b.BUDGET
      , [AVG_CHECK_IN]    = b.AVG_CHECK
      , [AVG_CHECK_AFTER] = c.AVG_CHECK
      , SHARE_AVG_CHECK_AFTER = c.AVG_CHECK / b.AVG_CHECK
      , [CHECK_PER_CLIENT_IN]    = b.CHECK_PER_CLIENT
      , [CHECK_PER_CLIENT_AFTER] = c.CHECK_PER_CLIENT
      , SHARE_CHECK_PER_CLIENT_AFTER = c.CHECK_PER_CLIENT / b.CHECK_PER_CLIENT
      , [AVG_COST_SKU_IN]    = b.AVG_COST_SKU
      , [AVG_COST_SKU_AFTER] = c.AVG_COST_SKU
      , SHARE_AVG_COST_SKU_AFTER = c.AVG_COST_SKU / b.AVG_COST_SKU
      , [AVG_SKU_IN]    = b.AVG_SKU
      , [AVG_SKU_AFTER] = c.AVG_SKU
      , SHARE_AVG_SKU_AFTER = c.AVG_SKU / b.AVG_SKU
      , SEGMENT_GROUP   = b.SEG_GROUP
      , CLIENTS_PRE     = a.CLIENTS
      , TO_PRE          = a.COST_DISCOUNT
      , TO_PRE_NORM     = n.TO_PRE_NORM
      , BUDGET_PRE_NORM = n.TO_PRE_NORM / d.CLIENT_ALL
INTO #effect
FROM  #t AS b
      INNER JOIN I_PROMO AS p (nolock)
      ON b.ID_PROMO = p.ID_PROMO AND b.ID_COMPANY = p.ID_COMPANY
      LEFT JOIN #t AS c
      ON b.ID_ORGANIZATION = c.ID_ORGANIZATION AND b.ID_PROMO = c.ID_PROMO AND b.SEG_GROUP = c.SEG_GROUP
      AND b.CONTROL_GROUP = c.CONTROL_GROUP AND b.ID_COMPANY = c.ID_COMPANY AND c.[PERIOD] = 1
      LEFT JOIN #t AS a
      ON b.ID_ORGANIZATION = a.ID_ORGANIZATION AND b.ID_PROMO = a.ID_PROMO AND b.SEG_GROUP = a.SEG_GROUP
      AND b.CONTROL_GROUP = a.CONTROL_GROUP AND b.ID_COMPANY = a.ID_COMPANY AND a.[PERIOD] = -1
      INNER JOIN #all_cl AS d
      ON b.CONTROL_GROUP = d.CONTROL_GROUP AND b.ID_PROMO = d.ID_PROMO AND b.SEG_GROUP = d.SEG_GROUP AND b.ID_COMPANY = d.ID_COMPANY
      LEFT JOIN #pre_norm AS n
      ON b.ID_PROMO = n.ID_PROMO AND b.SEG_GROUP = n.SEG_GROUP AND b.CONTROL_GROUP = n.CONTROL_GROUP
WHERE b.[PERIOD] = 0;

INSERT INTO I_CVM_EFFECT_CG
      ( ID_ORGANIZATION, ID_COMPANY, ID_PROMO, CONTROL_GROUP, START_DATE, FINISH_DATE, CLIENT_ALL
      , CLIENTS_IN, CLIENTS_AFTER, SHARE_IN, SHARE_AFTER, [TO_IN], [TO_AFTER], SHARE_TO_AFTER
      , [BUDGET_IN], [BUDGET_AFTER], SHARE_BUDGET_AFTER
      , [AVG_CHECK_IN], [AVG_CHECK_AFTER], SHARE_AVG_CHECK_AFTER
      , [CHECK_PER_CLIENT_IN], [CHECK_PER_CLIENT_AFTER], SHARE_CHECK_PER_CLIENT_AFTER
      , [AVG_COST_SKU_IN], [AVG_COST_SKU_AFTER], SHARE_AVG_COST_SKU_AFTER
      , [AVG_SKU_IN], [AVG_SKU_AFTER], SHARE_AVG_SKU_AFTER
      , SEGMENT_GROUP, CLIENTS_PRE, TO_PRE, TO_PRE_NORM, BUDGET_PRE_NORM)
SELECT  ID_ORGANIZATION, ID_COMPANY, ID_PROMO, CONTROL_GROUP, START_DATE, FINISH_DATE, CLIENT_ALL
      , CLIENTS_IN, CLIENTS_AFTER, SHARE_IN, SHARE_AFTER, [TO_IN], [TO_AFTER], SHARE_TO_AFTER
      , [BUDGET_IN], [BUDGET_AFTER], SHARE_BUDGET_AFTER
      , [AVG_CHECK_IN], [AVG_CHECK_AFTER], SHARE_AVG_CHECK_AFTER
      , [CHECK_PER_CLIENT_IN], [CHECK_PER_CLIENT_AFTER], SHARE_CHECK_PER_CLIENT_AFTER
      , [AVG_COST_SKU_IN], [AVG_COST_SKU_AFTER], SHARE_AVG_COST_SKU_AFTER
      , [AVG_SKU_IN], [AVG_SKU_AFTER], SHARE_AVG_SKU_AFTER
      , SEGMENT_GROUP, CLIENTS_PRE, TO_PRE, TO_PRE_NORM, BUDGET_PRE_NORM
FROM #effect;

-- ---------- ЦГ vs КГ ----------
DROP TABLE IF EXISTS #I_EFFECT_MONTH;

SELECT  a.ID_PROMO
      , c.PROMO_NAME
      , a.ID_ORGANIZATION
      , a.ID_COMPANY
      , a.SEGMENT_GROUP
      , a.START_DATE
      , a.FINISH_DATE
      , a.CLIENT_ALL
      , a.CLIENTS_IN
      , CLIENTS_IN_CG    = b.CLIENTS_IN
      , a.CLIENTS_AFTER
      , CLIENTS_AFTER_CG = b.CLIENTS_AFTER
      , a.[TO_IN]
      , [TO_IN_CG]       = b.[TO_IN]
      , a.[TO_AFTER]
      , [TO_AFTER_CG]    = b.[TO_AFTER]
      , RESPONSE       = a.CLIENTS_IN    * 1.0 / nullif(a.CLIENT_ALL, 0) - b.CLIENTS_IN    * 1.0 / nullif(b.CLIENT_ALL, 0)
      , RESPONSE_AFTER = a.CLIENTS_AFTER * 1.0 / nullif(a.CLIENT_ALL, 0) - b.CLIENTS_AFTER * 1.0 / nullif(b.CLIENT_ALL, 0)
      , ADD_TO_IN     = (a.[TO_IN]    / a.CLIENT_ALL - b.[TO_IN]    / b.CLIENT_ALL) * a.CLIENT_ALL
      , ADD_TO_AFTER  = (isnull(a.[TO_AFTER], 0) / a.CLIENT_ALL - isnull(b.[TO_AFTER], 0) / b.CLIENT_ALL) * a.CLIENT_ALL
      -- diff-in-diff: (ЦГ_in − ЦГ_pre_norm) − (КГ_in − КГ_pre_norm), на клиента × размер ЦГ
      , ADD_TO_IN_DID = ( (a.[TO_IN] - isnull(a.TO_PRE_NORM, 0)) / a.CLIENT_ALL
                        - (b.[TO_IN] - isnull(b.TO_PRE_NORM, 0)) / b.CLIENT_ALL ) * a.CLIENT_ALL
      , a.[BUDGET_IN]
      , a.[BUDGET_AFTER]
      , [BUDGET_IN_CG]    = b.[BUDGET_IN]
      , [BUDGET_AFTER_CG] = b.[BUDGET_AFTER]
      , BUDGET_PRE    = a.BUDGET_PRE_NORM
      , BUDGET_PRE_CG = b.BUDGET_PRE_NORM
INTO #I_EFFECT_MONTH
FROM  #effect AS a
      INNER JOIN #effect AS b
      ON a.ID_ORGANIZATION = b.ID_ORGANIZATION AND a.ID_PROMO = b.ID_PROMO
      AND a.ID_COMPANY = b.ID_COMPANY AND a.SEGMENT_GROUP = b.SEGMENT_GROUP
      INNER JOIN I_PROMO AS c
      ON a.ID_PROMO = c.ID_PROMO AND a.ID_COMPANY = c.ID_COMPANY
WHERE a.CONTROL_GROUP = 0 AND b.CONTROL_GROUP = 1;

-- ---------- скидка по акции: промо × канал × группа ----------
DROP TABLE IF EXISTS #bonus;

SELECT  b.ID_PROMO
      , a.ID_COMPANY
      , h.ID_ORGANIZATION
      , x.SEG_GROUP
      , BONUS_VALUE = sum(abs(a.[VALUE]))
      , COUNT_CHECK = count(DISTINCT a.ID_CHECK)
INTO #bonus
FROM  I_CHECK_RULE AS a (nolock)
      INNER JOIN I_PROMO AS b (nolock)
      ON a.ID_CAMPAIGN = b.ID_CAMPAIGN AND a.ID_COMPANY = b.ID_COMPANY AND a.ID_COMPANY = @idc
      INNER JOIN I_CHECKHEADER AS h (nolock)
      ON a.ID_COMPANY = h.ID_COMPANY AND a.ID_CHECK = h.ID_CHECK
      INNER JOIN #base AS x
      ON x.ID_PROMO = b.ID_PROMO AND x.ID_CONTACT = h.ID_CONTACT AND x.ID_COMPANY = h.ID_COMPANY
WHERE b.ID_PROMO IN (SELECT ID_PROMO FROM #promo)
GROUP BY b.ID_PROMO, a.ID_COMPANY, h.ID_ORGANIZATION, x.SEG_GROUP;

-- ---------- доставка: промо × группа (только ЦГ) ----------
DROP TABLE IF EXISTS #delivery_status;

SELECT  a.ID_COMPANY
      , d.ID_PROMO
      , d.SEG_GROUP
      , DELIVERED_SHARE   = sum(iif(a.[STATUS] = N'Доставлено', 1, 0)) * 1.0 / d.CLIENT_ALL
      , SENDED_SHARE      = sum(iif(a.[STATUS] IN (N'Отправлено', N'Доставлено'), 1, 0)) * 1.0 / d.CLIENT_ALL
      , ERROR_SHARE       = sum(iif(a.[STATUS] = N'Ошибка', 1.0, 0.0)) / d.CLIENT_ALL
      , FOLLOW_LINK_SHARE = sum(iif(a.[STATUS] = N'Переход по ссылке', 1, 0)) * 1.0 / d.CLIENT_ALL
      , COUNT_MSG         = cast(sum(iif(a.[STATUS] = N'Отправлено', 1.0, 0.0)) / d.CLIENT_ALL AS int)
INTO #delivery_status
FROM  I_PROMO_DELIVERY_REPORT AS a (nolock)
      INNER JOIN #base AS b
      ON try_convert(int, left(a.ACTION_NAME, 6)) = b.ID_PROMO
      AND a.ID_COMPANY = b.ID_COMPANY AND a.ID_CONTACT = b.ID_CONTACT AND b.CONTROL_GROUP = 0
      INNER JOIN #all_cl AS d
      ON b.ID_PROMO = d.ID_PROMO AND b.SEG_GROUP = d.SEG_GROUP AND b.CONTROL_GROUP = d.CONTROL_GROUP AND b.ID_COMPANY = d.ID_COMPANY
WHERE a.ID_COMPANY = @idc
GROUP BY a.ID_COMPANY, d.ID_PROMO, d.SEG_GROUP, d.CLIENT_ALL;

-- ---------- итоговый отчёт ----------
INSERT INTO I_CVM_EFFECT
      ( [Дата отчета], COMPANY, [Год], [Месяц], [Канал продаж], [ID], [Название], [Дата старта], [Дата окончания]
      , [Сегмент], [Механика], [Канал], [Выборка], [Доп ТО], [Доп ТО в акцию], [Доп ТО после акции], [Скидка р], PL
      , [Клиентов ЦГ в акцию], [Отклик], [Стат значимость траффик], [Эффект по трафику]
      , [ARPU ЦГ р], [ARPU КГ р], [Стат значимость ARPU], [Эффект по ARPU]
      , [Период после], [Клиентов ЦГ после акции], [Отклик после акции], [ARPU ЦГ после акции р], [ARPU КГ после акции р]
      , [Доставлено %], [Отправлено %], [Переход по ссылке %], [Ошибка %], sample_size_note, ID_ORGANIZATION, ID_COMPANY
      , [Сегмент клиента], [ARPU ЦГ до р], [ARPU КГ до р], [Доп ТО в акцию DiD], [PL DiD], [Стат значимость DiD], [Эффект DiD])
SELECT  [Дата отчета]     = cast(getdate() AS date)
      , j.COMPANY
      , [Год]             = year(a.START_DATE)
      , [Месяц]           = month(a.START_DATE)
      , [Канал продаж]    = j.ORGANIZATION
      , [ID]              = a.ID_PROMO
      , [Название]        = a.PROMO_NAME
      , [Дата старта]     = a.START_DATE
      , [Дата окончания]  = a.FINISH_DATE
      , [Сегмент]         = c.SEGMENT
      , [Механика]        = c.MECHANICS
      , [Канал]           = c.CHANNEL
      , [Выборка]         = a.CLIENT_ALL
      , [Доп ТО]          = a.ADD_TO_IN + a.ADD_TO_AFTER
      , [Доп ТО в акцию]  = a.ADD_TO_IN
      , [Доп ТО после акции] = a.ADD_TO_AFTER
      , [Скидка р]        = isnull(e.BONUS_VALUE, 0)
      , PL                = (isnull(a.ADD_TO_IN, 0) + isnull(a.ADD_TO_AFTER, 0)) / 1.015 * 0.3 - isnull(e.BONUS_VALUE, 0) / 1.015
      , [Клиентов ЦГ в акцию] = a.CLIENTS_IN
      , [Отклик]          = a.RESPONSE
      , [Стат значимость траффик] = n.significance_level
      , [Эффект по трафику]       = n.effect_direction
      , [ARPU ЦГ р]       = a.[BUDGET_IN]
      , [ARPU КГ р]       = a.[BUDGET_IN_CG]
      , [Стат значимость ARPU] = m.significance_level
      , [Эффект по ARPU]       = m.effect_direction
      , [Период после]    = concat(c.START_DATE_AFTER, ' - ', c.FINISH_DATE_AFTER)
      , [Клиентов ЦГ после акции] = a.CLIENTS_AFTER
      , [Отклик после акции]      = a.RESPONSE_AFTER
      , [ARPU ЦГ после акции р]   = a.[BUDGET_AFTER]
      , [ARPU КГ после акции р]   = a.[BUDGET_AFTER_CG]
      , [Доставлено %]        = k.DELIVERED_SHARE / nullif(k.SENDED_SHARE, 0)
      , [Отправлено %]        = k.SENDED_SHARE
      , [Переход по ссылке %] = k.FOLLOW_LINK_SHARE / nullif(k.DELIVERED_SHARE, 0)
      , [Ошибка %]            = k.ERROR_SHARE / nullif(k.SENDED_SHARE, 0)
      , m.sample_size_note
      , a.ID_ORGANIZATION
      , a.ID_COMPANY
      , [Сегмент клиента]     = a.SEGMENT_GROUP
      , [ARPU ЦГ до р]        = a.BUDGET_PRE
      , [ARPU КГ до р]        = a.BUDGET_PRE_CG
      , [Доп ТО в акцию DiD]  = a.ADD_TO_IN_DID
      , [PL DiD]              = (isnull(a.ADD_TO_IN_DID, 0) + isnull(a.ADD_TO_AFTER, 0)) / 1.015 * 0.3 - isnull(e.BONUS_VALUE, 0) / 1.015
      , [Стат значимость DiD] = q.significance_level
      , [Эффект DiD]          = q.effect_direction
FROM  #I_EFFECT_MONTH AS a
      INNER JOIN I_PROMO AS c
      ON a.ID_PROMO = c.ID_PROMO AND a.ID_COMPANY = c.ID_COMPANY
      INNER JOIN I_ORGANIZATION AS j
      ON a.ID_ORGANIZATION = j.ID_ORGANIZATION AND a.ID_COMPANY = j.ID_COMPANY
      LEFT JOIN #bonus AS e
      ON e.ID_PROMO = a.ID_PROMO AND a.ID_COMPANY = e.ID_COMPANY
      AND a.ID_ORGANIZATION = e.ID_ORGANIZATION AND a.SEGMENT_GROUP = e.SEG_GROUP
      LEFT JOIN #delivery_status AS k
      ON a.ID_PROMO = k.ID_PROMO AND a.ID_COMPANY = k.ID_COMPANY AND a.SEGMENT_GROUP = k.SEG_GROUP
      LEFT JOIN #interpret AS m
      ON a.ID_PROMO = m.ID_PROMO AND a.SEGMENT_GROUP = m.SEG_GROUP AND m.METRIC = 'ARPU'
      LEFT JOIN #interpret AS n
      ON a.ID_PROMO = n.ID_PROMO AND a.SEGMENT_GROUP = n.SEG_GROUP AND n.METRIC = 'TRAF'
      LEFT JOIN #interpret AS q
      ON a.ID_PROMO = q.ID_PROMO AND a.SEGMENT_GROUP = q.SEG_GROUP AND q.METRIC = 'DID';

END

SET @c = @c + 1;
SET @idc = (SELECT ID_COMPANY FROM #c WHERE RN = @c);

END
GO

-- ============================== РАСЧЁТ: все акции с сентября 2025 ==============================
-- После чистки #promo подхватывает всё с 2025-09-01 (ID NOT IN I_CVM_EFFECT). Долгий шаг: чеки за 26 мес. по оттоку/спящим.
EXEC dbo.CVM_EFFECT_TODAY;
GO

-- контроль: что посчиталось
SELECT [Сегмент клиента], PROMOS = count(DISTINCT [ID]), ROWS_ = count(*)
     , MIN_START = min([Дата старта]), MAX_START = max([Дата старта])
FROM dbo.I_CVM_EFFECT
WHERE [Дата старта] >= '2025-09-01'
GROUP BY [Сегмент клиента]
ORDER BY [Сегмент клиента];
GO
