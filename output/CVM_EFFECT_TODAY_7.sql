-- CVM_EFFECT · версия 7 — САМОСТОЯТЕЛЬНЫЙ СКРИПТ, оптимизирован под большие данные (после senior-SQL ревью версии 3), процедуру CVM_EFFECT_TODAY и её таблицы не трогает
-- Считает эффект акций с 2025-09-01 (ID_PROMO LIKE '101%', флаг CVM в I_PROMO, ID_COMPANY=1) в разрезе сегмента клиента.
--   Сегмент клиента считается НА ДАТУ СТАРТА акции из I_CHECKHEADER (пороги 6/15 нед. от последней покупки) и фиксируется в I_PROMO_OFFER_SEGMENT.
--   · Сегмент хранится в отдельной таблице I_PROMO_OFFER_SEGMENT (I_PROMO_OFFER без изменений — INSERT SELECT * в выборках не ломается).
--     Заполняется здесь для тех же акций (101%, CVM, с 2025-09-01) расчётом по чекам на дату старта; новые акции подхватываются при следующем запуске.
--   · Группы: Активные/Новые (1,2,3) · Случайные (4) · Отток (5) · Спящие (6) · Без сегмента · «Все» (итог по акции).
--   · Препериод: Активные/Новые — 7 нед., Случайные — 54 нед., Отток — 26 мес. (не раньше первой покупки), Спящие — вся история.
--     Нормируется к длине акции; доп. ТО в акцию считается ещё и как diff-in-diff.
--   · Выбросы: не удаляются — каждый чек обрезается до P99 чеков КГ (промо × группа), одинаково ЦГ/КГ, во всех периодах.
--   · Стат-тесты на ВСЕХ предложенных (не купил = 0): ARPU, DiD, трафик.
--   · Результат: I_CVM_EFFECT_SEG (формат I_CVM_EFFECT + колонки сегмента/DiD) и I_CVM_EFFECT_CG_SEG. Пересоздаются при каждом запуске.
--   · I_PROMO только читается: окна «после» — как в I_PROMO; если NULL — +1..+7 дней после финиша.
--   Оптимизация: один прицельный скан I_CHECKHEADER по клиентам выборок (#ch, индекс ID_CONTACT+DATA), периоды одним диапазоном;
--   группа «Все» добавляется после скана чеков; ключи групп — int SEG_ID; индексы на temp-таблицах; delivery-report предфильтр.

USE [mci_model]
GO

-- ============================== 1. Сегмент клиента на момент отправки ==============================
IF OBJECT_ID('dbo.I_PROMO_OFFER_SEGMENT') IS NULL
    CREATE TABLE dbo.I_PROMO_OFFER_SEGMENT
    ( ID_PROMO   int      NOT NULL
    , ID_CONTACT int      NOT NULL
    , ID_COMPANY int      NOT NULL
    , SEGMENT    tinyint  NULL
    , FIRST_DATA date     NULL      -- первая покупка до старта акции
    , LAST_DATA  date     NULL      -- последняя покупка до старта акции
    , DT_FIX     date     NOT NULL DEFAULT (cast(getdate() AS date))
    , CONSTRAINT PK_I_PROMO_OFFER_SEGMENT PRIMARY KEY (ID_COMPANY, ID_PROMO, ID_CONTACT));
GO

-- дозаполнение: только клиенты, которых ещё нет.
-- Сегмент считается НА ДАТУ СТАРТА АКЦИИ из I_CHECKHEADER (последняя/первая покупка до старта), пороги жизненного цикла:
--   ≤6 нед. от последней покупки → Активные (2), из них первая покупка тоже ≤6 нед. → Новые (1); 6–15 нед. → Отток (5); >15 нед. → Спящие (6);
--   покупок до старта нет → NULL (группа «Без сегмента»). Случайные (4) по давности не восстанавливаются.
DROP TABLE IF EXISTS #need;

SELECT o.ID_PROMO, o.ID_CONTACT, o.ID_COMPANY, p.START_DATE
INTO #need
FROM dbo.I_PROMO_OFFER AS o (nolock)
     INNER JOIN dbo.I_PROMO AS p (nolock)
     ON p.ID_PROMO = o.ID_PROMO AND p.ID_COMPANY = o.ID_COMPANY
WHERE o.ID_COMPANY = 1
  AND p.START_DATE >= '2025-09-01'
  AND p.ID_PROMO LIKE '101%'
  AND p.TYPE_PROMO = 'CVM'
  AND NOT EXISTS (SELECT 1 FROM dbo.I_PROMO_OFFER_SEGMENT AS s
                  WHERE s.ID_PROMO = o.ID_PROMO AND s.ID_CONTACT = o.ID_CONTACT AND s.ID_COMPANY = o.ID_COMPANY)
GROUP BY o.ID_PROMO, o.ID_CONTACT, o.ID_COMPANY, p.START_DATE;

CREATE UNIQUE CLUSTERED INDEX ix ON #need (ID_CONTACT, ID_PROMO);

DROP TABLE IF EXISTS #need_cust;
SELECT ID_CONTACT, DT_MAX = max(START_DATE) INTO #need_cust FROM #need GROUP BY ID_CONTACT;
CREATE UNIQUE CLUSTERED INDEX ix ON #need_cust (ID_CONTACT);

-- один скан чеков по нужным клиентам: даты покупок до самого позднего старта
DROP TABLE IF EXISTS #need_ch;

SELECT c.ID_CONTACT, c.DATA
INTO #need_ch
FROM #need_cust AS k
     INNER JOIN dbo.I_CHECKHEADER AS c (nolock)
     ON c.ID_CONTACT = k.ID_CONTACT AND c.ID_COMPANY = 1 AND c.DATA < k.DT_MAX
GROUP BY c.ID_CONTACT, c.DATA
OPTION (RECOMPILE);

CREATE CLUSTERED INDEX ix ON #need_ch (ID_CONTACT, DATA);

INSERT INTO dbo.I_PROMO_OFFER_SEGMENT (ID_PROMO, ID_CONTACT, ID_COMPANY, SEGMENT, FIRST_DATA, LAST_DATA, DT_FIX)
SELECT n.ID_PROMO, n.ID_CONTACT, n.ID_COMPANY
     , SEGMENT = CASE WHEN x.LAST_DATA IS NULL                                  THEN NULL
                      WHEN datediff(day, x.LAST_DATA,  n.START_DATE) <= 42
                       AND datediff(day, x.FIRST_DATA, n.START_DATE) <= 42       THEN 1   -- Новые
                      WHEN datediff(day, x.LAST_DATA,  n.START_DATE) <= 42       THEN 2   -- Активные
                      WHEN datediff(day, x.LAST_DATA,  n.START_DATE) <= 105      THEN 5   -- Отток (6–15 нед.)
                      ELSE 6 END                                                          -- Спящие (>15 нед.)
     , x.FIRST_DATA
     , x.LAST_DATA
     , DT_FIX = n.START_DATE      -- сегмент зафиксирован на дату старта
FROM #need AS n
     OUTER APPLY (SELECT LAST_DATA = max(c.DATA), FIRST_DATA = min(c.DATA)
                  FROM #need_ch AS c
                  WHERE c.ID_CONTACT = n.ID_CONTACT AND c.DATA < n.START_DATE) AS x;

-- контроль: акции с клиентами без сегмента (пойдут в группу «Без сегмента», окно 7 нед.)
SELECT o.ID_PROMO, p.PROMO_NAME, CLIENTS = count(DISTINCT o.ID_CONTACT), NO_SEGMENT = sum(iif(s.SEGMENT IS NULL, 1, 0))
FROM dbo.I_PROMO_OFFER AS o (nolock)
     INNER JOIN dbo.I_PROMO AS p (nolock) ON p.ID_PROMO = o.ID_PROMO AND p.ID_COMPANY = o.ID_COMPANY
     LEFT JOIN dbo.I_PROMO_OFFER_SEGMENT AS s
     ON s.ID_PROMO = o.ID_PROMO AND s.ID_CONTACT = o.ID_CONTACT AND s.ID_COMPANY = o.ID_COMPANY
WHERE o.ID_COMPANY = 1 AND p.START_DATE >= '2025-09-01' AND p.ID_PROMO LIKE '101%' AND p.TYPE_PROMO = 'CVM'
GROUP BY o.ID_PROMO, p.PROMO_NAME
HAVING sum(iif(s.SEGMENT IS NULL, 1, 0)) > 0
ORDER BY o.ID_PROMO;
GO

-- ============================== 2. Расчёт ==============================
SET NOCOUNT ON;

DECLARE @idc int = 1;
DECLARE @min_data date = (SELECT min(DATA) FROM I_CHECKHEADER (nolock) WHERE ID_COMPANY = @idc);

DROP TABLE IF EXISTS dbo.I_CVM_EFFECT_SEG;
DROP TABLE IF EXISTS dbo.I_CVM_EFFECT_CG_SEG;

-- ---------- акции ----------
DROP TABLE IF EXISTS #promo;

-- окно «после»: как выставлено в I_PROMO; если NULL — +1..+7 дней после финиша (как в CVM_august_2026_I_PROMO.sql)
SELECT ID_PROMO
     , START_DATE  = min(START_DATE)
     , FINISH_DATE = max(FINISH_DATE)
     , START_DATE_AFTER  = isnull(min(START_DATE_AFTER),  dateadd(day, 1, max(FINISH_DATE)))
     , FINISH_DATE_AFTER = isnull(max(FINISH_DATE_AFTER), dateadd(day, 7, max(FINISH_DATE)))
     , DAYS_IN = datediff(day, min(START_DATE), max(FINISH_DATE)) + 1
     , ID_CAMPAIGN = min(ID_CAMPAIGN)
INTO #promo
FROM I_PROMO (nolock)
WHERE START_DATE >= '2025-09-01'
  AND ID_COMPANY = @idc
  AND ID_PROMO LIKE '101%'
  AND TYPE_PROMO = 'CVM'
GROUP BY ID_PROMO;

CREATE UNIQUE CLUSTERED INDEX ix ON #promo (ID_PROMO);

-- ---------- справочник групп: SEG_ID — ключ для join'ов, имя только в отчёте ----------
DROP TABLE IF EXISTS #seg;
CREATE TABLE #seg (SEG_ID tinyint NOT NULL PRIMARY KEY, SEG_GROUP nvarchar(20) NOT NULL);
INSERT INTO #seg VALUES (0, N'Все'), (1, N'Активные/Новые'), (2, N'Случайные'), (3, N'Отток'), (4, N'Спящие'), (5, N'Без сегмента');

-- ---------- база: 1 строка = предложенный клиент (дубль «Все» добавляется в #base2 ниже) ----------
DROP TABLE IF EXISTS #base;

SELECT  a.ID_PROMO
      , a.ID_CONTACT
      , CONTROL_GROUP = min(a.CONTROL_GROUP)
      , SEG_ID = CASE WHEN s.SEGMENT IN (1, 2, 3) THEN 1
                      WHEN s.SEGMENT = 4          THEN 2
                      WHEN s.SEGMENT = 5          THEN 3
                      WHEN s.SEGMENT = 6          THEN 4
                      ELSE 5 END
      , PRE_START = CASE WHEN s.SEGMENT IN (1, 2, 3) THEN dateadd(week,  -7,  p.START_DATE)
                         WHEN s.SEGMENT = 4          THEN dateadd(week,  -54, p.START_DATE)
                         WHEN s.SEGMENT = 5          THEN CASE WHEN s.FIRST_DATA > dateadd(month, -26, p.START_DATE)
                                                               THEN s.FIRST_DATA ELSE dateadd(month, -26, p.START_DATE) END
                         WHEN s.SEGMENT = 6          THEN isnull(s.FIRST_DATA, @min_data)
                         ELSE dateadd(week, -7, p.START_DATE) END
      , PRE_FINISH = dateadd(day, -1, p.START_DATE)
INTO #base
FROM  I_PROMO_OFFER AS a (nolock)
      INNER JOIN #promo AS p
      ON a.ID_PROMO = p.ID_PROMO
      LEFT JOIN I_PROMO_OFFER_SEGMENT AS s (nolock)
      ON a.ID_PROMO = s.ID_PROMO AND a.ID_CONTACT = s.ID_CONTACT AND a.ID_COMPANY = s.ID_COMPANY
WHERE a.ID_COMPANY = @idc
  AND NOT EXISTS (SELECT 1 FROM I_MISTAKE_CONTACT AS d (nolock) WHERE d.ID_CONTACT = a.ID_CONTACT AND d.ID_COMPANY = a.ID_COMPANY)
  AND NOT EXISTS (SELECT 1 FROM I_FRAUD_CONTACT   AS e (nolock) WHERE e.ID_CONTACT = a.ID_CONTACT AND e.ID_COMPANY = a.ID_COMPANY)
GROUP BY a.ID_PROMO, a.ID_CONTACT, s.SEGMENT, s.FIRST_DATA, p.START_DATE;

UPDATE #base SET PRE_START = @min_data  WHERE PRE_START < @min_data;
UPDATE #base SET PRE_START = PRE_FINISH WHERE PRE_START > PRE_FINISH;

CREATE UNIQUE CLUSTERED INDEX ix ON #base (ID_CONTACT, ID_PROMO);

-- ---------- один прицельный скан чеков: только клиенты выборок, только нужный диапазон дат ----------
DROP TABLE IF EXISTS #cust;

SELECT b.ID_CONTACT, DT_FROM = min(b.PRE_START), DT_TO = max(p.FINISH_DATE_AFTER)
INTO #cust
FROM #base AS b INNER JOIN #promo AS p ON p.ID_PROMO = b.ID_PROMO
GROUP BY b.ID_CONTACT;

CREATE UNIQUE CLUSTERED INDEX ix ON #cust (ID_CONTACT);

DROP TABLE IF EXISTS #ch;

SELECT  c.ID_CONTACT
      , c.DATA
      , c.ID_CHECK
      , c.ID_ORGANIZATION
      , COST_DISCOUNT = isnull(c.COST_DISCOUNT, 0) + isnull(c.DISCOUNT, 0)
      , c.SKU
      , c.REAL_DISCOUNT
      , c.DISCOUNT
      , c.BONUS_PAY
      , c.BONUS_ACCRUAL
INTO #ch
FROM  #cust AS k
      INNER JOIN I_CHECKHEADER AS c (nolock)
      ON c.ID_CONTACT = k.ID_CONTACT
      AND c.ID_COMPANY = @idc
      AND c.DATA BETWEEN k.DT_FROM AND k.DT_TO     -- диапазон свой у каждого клиента: активным не тянем всю историю
OPTION (RECOMPILE);

CREATE CLUSTERED INDEX ix ON #ch (ID_CONTACT, DATA);

-- ---------- база с группой «Все» (SEG_ID=0) — ДО расчёта чеков, чтобы порог обрезки был свой у каждой группы ----------
DROP TABLE IF EXISTS #base2;
SELECT ID_PROMO, ID_CONTACT, CONTROL_GROUP, SEG_ID, PRE_START, PRE_FINISH INTO #base2 FROM #base
UNION ALL
SELECT ID_PROMO, ID_CONTACT, CONTROL_GROUP, 0,      PRE_START, PRE_FINISH FROM #base;

CREATE UNIQUE CLUSTERED INDEX ix ON #base2 (ID_CONTACT, ID_PROMO, SEG_ID);

-- ---------- порог обрезки (winsorize): P99 чеков КОНТРОЛЬНОЙ группы в акцию, промо × группа ----------
-- КГ не получала акцию → её распределение не зависит от эффекта; порог применяется одинаково к ЦГ и КГ и ко всем периодам.
-- Чек выше порога приравнивается к порогу, клиент остаётся в знаменателе (отклик не искажается).
DROP TABLE IF EXISTS #cg_chk;

SELECT b.ID_PROMO, b.SEG_ID, c.COST_DISCOUNT
     , RN  = ROW_NUMBER() OVER (PARTITION BY b.ID_PROMO, b.SEG_ID ORDER BY c.COST_DISCOUNT)
     , CNT = COUNT(*)     OVER (PARTITION BY b.ID_PROMO, b.SEG_ID)
INTO #cg_chk
FROM  #base2 AS b
      INNER JOIN #promo AS p ON b.ID_PROMO = p.ID_PROMO
      INNER JOIN #ch AS c ON c.ID_CONTACT = b.ID_CONTACT AND c.DATA BETWEEN p.START_DATE AND p.FINISH_DATE
WHERE b.CONTROL_GROUP = 1;

DROP TABLE IF EXISTS #cap;

SELECT ID_PROMO, SEG_ID, CAP = max(COST_DISCOUNT), N_CG_CHECKS = max(CNT)
INTO #cap
FROM #cg_chk
WHERE RN <= ceiling(0.99 * CNT)
GROUP BY ID_PROMO, SEG_ID;

CREATE UNIQUE CLUSTERED INDEX ix ON #cap (ID_PROMO, SEG_ID);

-- ---------- чеки по периодам: -1 до / 0 в акцию / 1 после; один диапазон на клиента; чек обрезан по CAP ----------
DROP TABLE IF EXISTS #chk;

SELECT  b.ID_PROMO
      , b.ID_CONTACT
      , b.CONTROL_GROUP
      , b.SEG_ID
      , c.ID_ORGANIZATION
      , [PERIOD]      = x.[PERIOD]
      , COST_DISCOUNT = sum(CASE WHEN k.CAP IS NOT NULL AND c.COST_DISCOUNT > k.CAP THEN k.CAP ELSE c.COST_DISCOUNT END)
      , COST_RAW      = sum(c.COST_DISCOUNT)
      , CAPPED_CHECKS = sum(CASE WHEN k.CAP IS NOT NULL AND c.COST_DISCOUNT > k.CAP THEN 1 ELSE 0 END)
      , COUNT_CHECK   = count(c.ID_CHECK)
      , SKU           = sum(c.SKU)
      , REAL_DISCOUNT = sum(c.REAL_DISCOUNT)
      , DISCOUNT      = sum(c.DISCOUNT)
      , BONUS_PAY     = sum(c.BONUS_PAY)
      , BONUS_ACCRUAL = sum(c.BONUS_ACCRUAL)
INTO #chk
FROM  #base2 AS b
      INNER JOIN #promo AS p
      ON b.ID_PROMO = p.ID_PROMO
      INNER JOIN #ch AS c
      ON c.ID_CONTACT = b.ID_CONTACT
      AND c.DATA BETWEEN b.PRE_START AND p.FINISH_DATE_AFTER
      LEFT JOIN #cap AS k
      ON k.ID_PROMO = b.ID_PROMO AND k.SEG_ID = b.SEG_ID
      CROSS APPLY (SELECT [PERIOD] = CASE WHEN c.DATA <= b.PRE_FINISH          THEN -1
                                          WHEN c.DATA <= p.FINISH_DATE         THEN 0
                                          WHEN c.DATA >= p.START_DATE_AFTER    THEN 1 END) AS x
WHERE x.[PERIOD] IS NOT NULL
GROUP BY b.ID_PROMO, b.ID_CONTACT, b.CONTROL_GROUP, b.SEG_ID, c.ID_ORGANIZATION, x.[PERIOD];

CREATE CLUSTERED INDEX ix ON #chk (ID_PROMO, SEG_ID, [PERIOD], ID_ORGANIZATION, ID_CONTACT);

-- ---------- клиент-уровень: все предложенные, не купил = 0; препериод нормирован к длине акции ----------
DROP TABLE IF EXISTS #cl;

SELECT  b.ID_PROMO, b.SEG_ID, b.CONTROL_GROUP, b.ID_CONTACT
      , TO_IN       = isnull(i.TO_IN, 0.0)
      , BUY_IN      = iif(i.TO_IN IS NULL, 0.0, 1.0)
      , TO_PRE      = isnull(i.TO_PRE, 0.0)
      , TO_PRE_NORM = isnull(i.TO_PRE, 0.0) * p.DAYS_IN * 1.0 / nullif(datediff(day, b.PRE_START, b.PRE_FINISH) + 1, 0)
INTO #cl
FROM  #base2 AS b
      INNER JOIN #promo AS p ON b.ID_PROMO = p.ID_PROMO
      LEFT JOIN (SELECT ID_PROMO, SEG_ID, ID_CONTACT
                      , TO_IN  = sum(CASE WHEN [PERIOD] = 0  THEN COST_DISCOUNT END)
                      , TO_PRE = sum(CASE WHEN [PERIOD] = -1 THEN COST_DISCOUNT END)
                 FROM #chk WHERE [PERIOD] IN (-1, 0)
                 GROUP BY ID_PROMO, SEG_ID, ID_CONTACT) AS i
      ON b.ID_PROMO = i.ID_PROMO AND b.SEG_ID = i.SEG_ID AND b.ID_CONTACT = i.ID_CONTACT;

CREATE CLUSTERED INDEX ix ON #cl (ID_PROMO, SEG_ID, CONTROL_GROUP);

-- ---------- t-тест Уэлча: три метрики одним проходом по #cl (без UNPIVOT-утроения) ----------
DROP TABLE IF EXISTS #stat;

;WITH s AS (
    SELECT ID_PROMO, SEG_ID, CONTROL_GROUP, n = count(*)
         , m_arpu = avg(TO_IN),               v_arpu = var(TO_IN)
         , m_did  = avg(TO_IN - TO_PRE_NORM), v_did  = var(TO_IN - TO_PRE_NORM)
         , m_traf = avg(BUY_IN),              v_traf = var(BUY_IN)
    FROM #cl WHERE CONTROL_GROUP IN (0, 1)
    GROUP BY ID_PROMO, SEG_ID, CONTROL_GROUP
),
u AS (
    SELECT s0.ID_PROMO, s0.SEG_ID, v.METRIC
         , n_control = s1.n, n_test = s0.n
         , mean_control = v.m1, mean_test = v.m0, mean_diff = v.m0 - v.m1
         , se_diff = CASE WHEN s0.n > 1 AND s1.n > 1 AND v.v0 IS NOT NULL AND v.v1 IS NOT NULL AND v.v0 / s0.n + v.v1 / s1.n > 0
                          THEN sqrt(v.v0 / s0.n + v.v1 / s1.n) END
    FROM s AS s0
         INNER JOIN s AS s1 ON s0.ID_PROMO = s1.ID_PROMO AND s0.SEG_ID = s1.SEG_ID AND s0.CONTROL_GROUP = 0 AND s1.CONTROL_GROUP = 1
         CROSS APPLY (VALUES ('ARPU', s0.m_arpu, s1.m_arpu, s0.v_arpu, s1.v_arpu)
                           , ('DID',  s0.m_did,  s1.m_did,  s0.v_did,  s1.v_did)
                           , ('TRAF', s0.m_traf, s1.m_traf, s0.v_traf, s1.v_traf)) AS v(METRIC, m0, m1, v0, v1)
)
SELECT ID_PROMO, SEG_ID, METRIC, n_control, n_test, mean_control, mean_test, mean_diff, se_diff
     , t_stat_welch = CASE WHEN se_diff IS NOT NULL AND abs(se_diff) > 1e-10 THEN mean_diff / se_diff END
     , ci_lower_95 = mean_diff - 1.96 * se_diff
     , ci_upper_95 = mean_diff + 1.96 * se_diff
     , relative_diff_pct = CASE WHEN abs(mean_control) > 1e-10 THEN 100.0 * mean_diff / mean_control END
     , sample_size_note  = CASE WHEN n_control < 30 OR n_test < 30 THEN N'Маленькие группы - интерпретируйте с осторожностью'
                                ELSE N'Достаточный размер групп' END
INTO #stat
FROM u;

DROP TABLE IF EXISTS #interpret;

SELECT ID_PROMO, SEG_ID, METRIC, t_stat_welch, sample_size_note
     , significance_level = CASE
           WHEN t_stat_welch IS NULL                    THEN N'Проверьте данные'
           WHEN abs(t_stat_welch) < 1.65                THEN N'Незначимо (вероятность > 10%)'
           WHEN abs(t_stat_welch) BETWEEN 1.65 AND 1.96 THEN N'Слабо значимо (p ≈ 0.05-0.10)'
           WHEN abs(t_stat_welch) BETWEEN 1.96 AND 2.58 THEN N'Значимо (p < 0.05)'
           WHEN abs(t_stat_welch) BETWEEN 2.58 AND 3.29 THEN N'Сильно значимо (p < 0.01)'
           WHEN abs(t_stat_welch) >= 3.29               THEN N'Очень сильно значимо (p < 0.001)'
           ELSE N'Проверьте данные' END
     , effect_direction = CASE WHEN t_stat_welch > 0 THEN N'Тест > Контроль'
                               WHEN t_stat_welch < 0 THEN N'Тест < Контроль'
                               ELSE N'Нет разницы' END
INTO #interpret
FROM #stat;

CREATE UNIQUE CLUSTERED INDEX ix ON #interpret (ID_PROMO, SEG_ID, METRIC);

-- ---------- агрегаты по периодам ----------
DROP TABLE IF EXISTS #t;

SELECT  a.ID_ORGANIZATION, a.ID_PROMO, a.SEG_ID, a.CONTROL_GROUP, a.[PERIOD]
      , CLIENTS       = count(a.ID_CONTACT)
      , COST_DISCOUNT = sum(a.COST_DISCOUNT)
      , COST_RAW      = sum(a.COST_RAW)
      , CAPPED_CHECKS = sum(a.CAPPED_CHECKS)
      , COUNT_CHECK   = sum(a.COUNT_CHECK)
      , SKU           = sum(a.SKU)
      , REAL_DISCOUNT = sum(a.REAL_DISCOUNT)
      , DISCOUNT      = sum(a.DISCOUNT)
      , BONUS_PAY     = sum(a.BONUS_PAY)
      , BONUS_ACCRUAL = sum(a.BONUS_ACCRUAL)
      , BUDGET           = sum(a.COST_DISCOUNT) * 1.0 / count(a.ID_CONTACT)
      , AVG_CHECK        = sum(a.COST_DISCOUNT) * 1.0 / nullif(sum(a.COUNT_CHECK), 0)
      , CHECK_PER_CLIENT = sum(a.COUNT_CHECK) * 1.00 / count(a.ID_CONTACT)
      , AVG_COST_SKU     = sum(a.COST_DISCOUNT) * 1.0 / nullif(sum(a.SKU), 0)
      , AVG_SKU          = sum(a.SKU) * 1.00 / nullif(sum(a.COUNT_CHECK), 0)
INTO #t
FROM #chk AS a
GROUP BY a.ID_ORGANIZATION, a.ID_PROMO, a.SEG_ID, a.CONTROL_GROUP, a.[PERIOD];

CREATE UNIQUE CLUSTERED INDEX ix ON #t (ID_PROMO, SEG_ID, CONTROL_GROUP, [PERIOD], ID_ORGANIZATION);

-- знаменатель: все предложенные (без выбросов)
DROP TABLE IF EXISTS #pre_norm;
SELECT ID_PROMO, SEG_ID, CONTROL_GROUP, CLIENT_ALL = count(*)
INTO #pre_norm
FROM #cl GROUP BY ID_PROMO, SEG_ID, CONTROL_GROUP;

CREATE UNIQUE CLUSTERED INDEX ix ON #pre_norm (ID_PROMO, SEG_ID, CONTROL_GROUP);

-- нормированный препериод ПО КАНАЛУ — чтобы DiD по каналу вычитал «до» того же канала
DROP TABLE IF EXISTS #pre_norm_org;
SELECT k.ID_PROMO, k.SEG_ID, k.CONTROL_GROUP, k.ID_ORGANIZATION
     , TO_PRE_NORM = sum(k.COST_DISCOUNT * p.DAYS_IN * 1.0 / nullif(datediff(day, b.PRE_START, b.PRE_FINISH) + 1, 0))
INTO #pre_norm_org
FROM #chk AS k
     INNER JOIN #base2 AS b ON b.ID_PROMO = k.ID_PROMO AND b.SEG_ID = k.SEG_ID AND b.ID_CONTACT = k.ID_CONTACT
     INNER JOIN #promo AS p ON p.ID_PROMO = k.ID_PROMO
WHERE k.[PERIOD] = -1
GROUP BY k.ID_PROMO, k.SEG_ID, k.CONTROL_GROUP, k.ID_ORGANIZATION;

CREATE UNIQUE CLUSTERED INDEX ix ON #pre_norm_org (ID_PROMO, SEG_ID, CONTROL_GROUP, ID_ORGANIZATION);

-- ---------- эффект по ЦГ/КГ ----------
DROP TABLE IF EXISTS #effect;

SELECT  b.ID_ORGANIZATION
      , ID_COMPANY = @idc
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
      , SHARE_TO_AFTER = c.COST_DISCOUNT / nullif(b.COST_DISCOUNT, 0)
      , [BUDGET_IN]    = b.BUDGET
      , [BUDGET_AFTER] = c.BUDGET
      , SHARE_BUDGET_AFTER = c.BUDGET / nullif(b.BUDGET, 0)
      , [AVG_CHECK_IN]    = b.AVG_CHECK
      , [AVG_CHECK_AFTER] = c.AVG_CHECK
      , SHARE_AVG_CHECK_AFTER = c.AVG_CHECK / nullif(b.AVG_CHECK, 0)
      , [CHECK_PER_CLIENT_IN]    = b.CHECK_PER_CLIENT
      , [CHECK_PER_CLIENT_AFTER] = c.CHECK_PER_CLIENT
      , SHARE_CHECK_PER_CLIENT_AFTER = c.CHECK_PER_CLIENT / nullif(b.CHECK_PER_CLIENT, 0)
      , [AVG_COST_SKU_IN]    = b.AVG_COST_SKU
      , [AVG_COST_SKU_AFTER] = c.AVG_COST_SKU
      , SHARE_AVG_COST_SKU_AFTER = c.AVG_COST_SKU / nullif(b.AVG_COST_SKU, 0)
      , [AVG_SKU_IN]    = b.AVG_SKU
      , [AVG_SKU_AFTER] = c.AVG_SKU
      , SHARE_AVG_SKU_AFTER = c.AVG_SKU / nullif(b.AVG_SKU, 0)
      , b.SEG_ID
      , SEGMENT_GROUP   = g.SEG_GROUP
      , CLIENTS_PRE     = a.CLIENTS
      , TO_PRE          = a.COST_DISCOUNT
      , TO_PRE_NORM     = isnull(pn.TO_PRE_NORM, 0)
      , BUDGET_PRE_NORM = isnull(pn.TO_PRE_NORM, 0) / d.CLIENT_ALL
      , CAP_CG_P99      = k.CAP
      , TO_IN_RAW       = b.COST_RAW
      , CAPPED_CHECKS_IN = b.CAPPED_CHECKS
INTO #effect
FROM  #t AS b
      INNER JOIN #promo AS p ON b.ID_PROMO = p.ID_PROMO
      INNER JOIN #seg AS g ON b.SEG_ID = g.SEG_ID
      INNER JOIN #pre_norm AS d
      ON b.ID_PROMO = d.ID_PROMO AND b.SEG_ID = d.SEG_ID AND b.CONTROL_GROUP = d.CONTROL_GROUP
      LEFT JOIN #pre_norm_org AS pn
      ON b.ID_PROMO = pn.ID_PROMO AND b.SEG_ID = pn.SEG_ID AND b.CONTROL_GROUP = pn.CONTROL_GROUP AND b.ID_ORGANIZATION = pn.ID_ORGANIZATION
      LEFT JOIN #cap AS k
      ON b.ID_PROMO = k.ID_PROMO AND b.SEG_ID = k.SEG_ID
      LEFT JOIN #t AS c
      ON b.ID_PROMO = c.ID_PROMO AND b.SEG_ID = c.SEG_ID AND b.CONTROL_GROUP = c.CONTROL_GROUP AND b.ID_ORGANIZATION = c.ID_ORGANIZATION AND c.[PERIOD] = 1
      LEFT JOIN #t AS a
      ON b.ID_PROMO = a.ID_PROMO AND b.SEG_ID = a.SEG_ID AND b.CONTROL_GROUP = a.CONTROL_GROUP AND b.ID_ORGANIZATION = a.ID_ORGANIZATION AND a.[PERIOD] = -1
WHERE b.[PERIOD] = 0;

SELECT *
INTO dbo.I_CVM_EFFECT_CG_SEG
FROM #effect;

-- ---------- ЦГ vs КГ ----------
DROP TABLE IF EXISTS #I_EFFECT_MONTH;

SELECT  a.ID_PROMO
      , a.ID_ORGANIZATION
      , a.ID_COMPANY
      , a.SEG_ID
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
      , ADD_TO_IN     = (a.[TO_IN] * 1.0 / a.CLIENT_ALL - b.[TO_IN] * 1.0 / b.CLIENT_ALL) * a.CLIENT_ALL
      , ADD_TO_AFTER  = (isnull(a.[TO_AFTER], 0) * 1.0 / a.CLIENT_ALL - isnull(b.[TO_AFTER], 0) * 1.0 / b.CLIENT_ALL) * a.CLIENT_ALL
      , ADD_TO_IN_DID = ( (a.[TO_IN] - a.TO_PRE_NORM) * 1.0 / a.CLIENT_ALL
                        - (b.[TO_IN] - b.TO_PRE_NORM) * 1.0 / b.CLIENT_ALL ) * a.CLIENT_ALL
      , a.[BUDGET_IN]
      , a.[BUDGET_AFTER]
      , [BUDGET_IN_CG]    = b.[BUDGET_IN]
      , [BUDGET_AFTER_CG] = b.[BUDGET_AFTER]
      , BUDGET_PRE    = a.BUDGET_PRE_NORM
      , BUDGET_PRE_CG = b.BUDGET_PRE_NORM
      , a.CAP_CG_P99
      , a.TO_IN_RAW
      , a.CAPPED_CHECKS_IN
INTO #I_EFFECT_MONTH
FROM  #effect AS a
      INNER JOIN #effect AS b
      ON a.ID_ORGANIZATION = b.ID_ORGANIZATION AND a.ID_PROMO = b.ID_PROMO AND a.SEG_ID = b.SEG_ID
WHERE a.CONTROL_GROUP = 0 AND b.CONTROL_GROUP = 1;

-- ---------- скидка по акции: I_CHECK_RULE → I_CHECKHEADER по ID_CHECK (PK) ----------
DROP TABLE IF EXISTS #bonus;

SELECT  p.ID_PROMO
      , h.ID_ORGANIZATION
      , x.SEG_ID
      , BONUS_VALUE = sum(abs(a.[VALUE]))
      , COUNT_CHECK = count(DISTINCT a.ID_CHECK)
INTO #bonus
FROM  #promo AS p
      INNER JOIN I_CHECK_RULE AS a (nolock) ON a.ID_CAMPAIGN = p.ID_CAMPAIGN AND a.ID_COMPANY = @idc
      INNER JOIN I_CHECKHEADER AS h (nolock) ON a.ID_CHECK = h.ID_CHECK AND h.ID_COMPANY = @idc
      INNER JOIN #base2 AS x ON x.ID_PROMO = p.ID_PROMO AND x.ID_CONTACT = h.ID_CONTACT
GROUP BY p.ID_PROMO, h.ID_ORGANIZATION, x.SEG_ID;

-- ---------- доставка: предфильтр отчёта, ID акции вычисляется один раз ----------
DROP TABLE IF EXISTS #dlv;

SELECT ID_PROMO = try_convert(int, left(a.ACTION_NAME, 6)), a.ID_CONTACT, a.[STATUS]
INTO #dlv
FROM I_PROMO_DELIVERY_REPORT AS a (nolock)
WHERE a.ID_COMPANY = @idc
  AND a.ACTION_NAME LIKE '101%'
  AND try_convert(int, left(a.ACTION_NAME, 6)) IN (SELECT ID_PROMO FROM #promo);

CREATE CLUSTERED INDEX ix ON #dlv (ID_PROMO, ID_CONTACT);

DROP TABLE IF EXISTS #delivery_status;

SELECT  d.ID_PROMO
      , d.SEG_ID
      , DELIVERED_SHARE   = sum(iif(a.[STATUS] = N'Доставлено', 1, 0)) * 1.0 / d.CLIENT_ALL
      , SENDED_SHARE      = sum(iif(a.[STATUS] IN (N'Отправлено', N'Доставлено'), 1, 0)) * 1.0 / d.CLIENT_ALL
      , ERROR_SHARE       = sum(iif(a.[STATUS] = N'Ошибка', 1.0, 0.0)) / d.CLIENT_ALL
      , FOLLOW_LINK_SHARE = sum(iif(a.[STATUS] = N'Переход по ссылке', 1, 0)) * 1.0 / d.CLIENT_ALL
      , COUNT_MSG         = cast(sum(iif(a.[STATUS] = N'Отправлено', 1.0, 0.0)) / d.CLIENT_ALL AS int)
INTO #delivery_status
FROM  #base2 AS b
      INNER JOIN #dlv AS a ON a.ID_PROMO = b.ID_PROMO AND a.ID_CONTACT = b.ID_CONTACT
      INNER JOIN #pre_norm AS d ON b.ID_PROMO = d.ID_PROMO AND b.SEG_ID = d.SEG_ID AND b.CONTROL_GROUP = d.CONTROL_GROUP
WHERE b.CONTROL_GROUP = 0
GROUP BY d.ID_PROMO, d.SEG_ID, d.CLIENT_ALL;

-- ---------- итоговый отчёт ----------
SELECT  [Дата отчета]     = cast(getdate() AS date)
      , j.COMPANY
      , [Год]             = year(a.START_DATE)
      , [Месяц]           = month(a.START_DATE)
      , [Канал продаж]    = j.ORGANIZATION
      , [ID]              = a.ID_PROMO
      , [Название]        = c.PROMO_NAME
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
      , [Порог обрезки чека р] = a.CAP_CG_P99
      , [Обрезано чеков ЦГ]    = a.CAPPED_CHECKS_IN
      , [ТО ЦГ в акцию без обрезки] = a.TO_IN_RAW
INTO dbo.I_CVM_EFFECT_SEG
FROM  #I_EFFECT_MONTH AS a
      INNER JOIN (SELECT *, RN = ROW_NUMBER() OVER (PARTITION BY ID_PROMO ORDER BY START_DATE)
                  FROM I_PROMO (nolock) WHERE ID_COMPANY = @idc) AS c      -- защита от дублей ID_PROMO
      ON a.ID_PROMO = c.ID_PROMO AND c.RN = 1
      INNER JOIN I_ORGANIZATION AS j (nolock) ON a.ID_ORGANIZATION = j.ID_ORGANIZATION AND j.ID_COMPANY = @idc
      LEFT JOIN #bonus AS e ON e.ID_PROMO = a.ID_PROMO AND a.ID_ORGANIZATION = e.ID_ORGANIZATION AND a.SEG_ID = e.SEG_ID
      LEFT JOIN #delivery_status AS k ON a.ID_PROMO = k.ID_PROMO AND a.SEG_ID = k.SEG_ID
      LEFT JOIN #interpret AS m ON a.ID_PROMO = m.ID_PROMO AND a.SEG_ID = m.SEG_ID AND m.METRIC = 'ARPU'
      LEFT JOIN #interpret AS n ON a.ID_PROMO = n.ID_PROMO AND a.SEG_ID = n.SEG_ID AND n.METRIC = 'TRAF'
      LEFT JOIN #interpret AS q ON a.ID_PROMO = q.ID_PROMO AND a.SEG_ID = q.SEG_ID AND q.METRIC = 'DID';
GO

-- ============================== 3. Контроль ==============================
SELECT [Сегмент клиента], PROMOS = count(DISTINCT [ID]), ROWS_ = count(*)
     , MIN_START = min([Дата старта]), MAX_START = max([Дата старта])
FROM dbo.I_CVM_EFFECT_SEG
GROUP BY [Сегмент клиента]
ORDER BY [Сегмент клиента];
GO
