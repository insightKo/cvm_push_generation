# Разложение Доп ТО ГКГ за октябрь 2026 по признакам клиентов. ТОЛЬКО ЧТЕНИЕ: временные таблицы #, витрины не меняются.
# Повторяет dbo.gcg_report (партии, слои → ячейки, κ = ½ по «ДО» и по месяцу, копии чеков один раз),
# но вместо итога выдаёт суммы N, ΣY, ΣX, ΣY², ΣX², ΣXY по ячейке × группе × признакам — остальное считается локально.
# Запуск: ssh kz 'ssh ru "docker exec -i cvm-api python -"' < gcg_oct_decomp.py > gcg_oct_decomp.jsonl
import sys, json, time, datetime, decimal
import pyodbc
from app.config import Settings

YM_FROM = '2026-10-01'
YM_TO = '2026-11-01'   # граница не включается

cn = pyodbc.connect(Settings.from_env().connection_string(), timeout=30, autocommit=True)
cn.timeout = 3600
cur = cn.cursor()
T0 = time.time()


def log(msg):
    print(f'[{time.time() - T0:7.1f}s] {msg}', file=sys.stderr)
    sys.stderr.flush()


# Без параметров ?: запрос с параметрами драйвер исполняет через sp_prepexec, и #таблица, созданная в нём,
# удаляется по его окончании (следующий шаг падает с Invalid object name). Даты — константы скрипта, подставляются текстом.
def run(name, sql):
    t = time.time()
    cur.execute(sql)
    while cur.nextset():
        pass
    log(f'{name}: {time.time() - t:.1f}s')


def conv(v):
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    return v


def dump(t, sql):
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    n = 0
    while True:
        rows = cur.fetchmany(5000)
        if not rows:
            break
        for r in rows:
            d = {'t': t}
            d.update({c: conv(v) for c, v in zip(cols, r)})
            print(json.dumps(d, ensure_ascii=False))
            n += 1
    log(f'dump {t}: {n} rows')
    sys.stdout.flush()


run('settings', "SET NOCOUNT ON; SET LOCK_TIMEOUT 60000;")

# 1. Партии, действующие в октябре; период партии — с max(1-е число, дата нарезки)
run('#b', f"""
DROP TABLE IF EXISTS #b;
SELECT BATCH_ID, PRE_TO, P_FROM = IIF(DRAW_DATE > '{YM_FROM}', DRAW_DATE, cast('{YM_FROM}' AS date))
INTO #b
FROM dbo.I_GCG_BATCH (nolock)
WHERE IIF(DRAW_DATE > '{YM_FROM}', DRAW_DATE, cast('{YM_FROM}' AS date)) < '{YM_TO}';
""")

# 2. Популяция: как в процедуре (LAST_DATA есть, «ДО» по сегменту > 0), слой давности на дату нарезки
run('#pop', """
DROP TABLE IF EXISTS #pop;
SELECT p.BATCH_ID, p.ID_CONTACT, p.GCG, p.SEGMENT, b.P_FROM, b.PRE_TO
     , X     = IIF(p.SEGMENT = 5, p.X_OTT, p.X_ACT)
     , LAYER = IIF(p.SEGMENT = 2, (datediff(day, p.LAST_DATA, b.PRE_TO) + 6) / 7, year(p.LAST_DATA) * 100 + month(p.LAST_DATA))
INTO #pop
FROM dbo.I_GCG_PRE AS p (nolock)
     JOIN #b AS b ON b.BATCH_ID = p.BATCH_ID
WHERE p.LAST_DATA IS NOT NULL
  AND IIF(p.SEGMENT = 5, p.X_OTT, p.X_ACT) > 0;
CREATE CLUSTERED COLUMNSTORE INDEX cci_pop ON #pop;
""")

# 2.1 Ячейки — тот же курсор слияния слоёв, что в процедуре
run('#lay', """
DROP TABLE IF EXISTS #lay;
SELECT BATCH_ID, SEGMENT, LAYER
     , NT = sum(IIF(GCG = 0, 1, 0)), NC = sum(IIF(GCG = 1, 1, 0))
     , ORD = row_number() OVER (PARTITION BY BATCH_ID, SEGMENT ORDER BY IIF(SEGMENT = 2, LAYER, -LAYER))
     , CELL = cast(NULL AS int)
INTO #lay
FROM #pop
GROUP BY BATCH_ID, SEGMENT, LAYER;

DECLARE @lb int, @ls int, @lo int, @lnt int, @lnc int, @pb int, @ps int, @cell int, @at int, @ac int;
SELECT @pb = NULL, @ps = NULL, @cell = 0, @at = 0, @ac = 0;
DECLARE lay_cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT BATCH_ID, SEGMENT, ORD, NT, NC FROM #lay ORDER BY BATCH_ID, SEGMENT, ORD;
OPEN lay_cur;
FETCH NEXT FROM lay_cur INTO @lb, @ls, @lo, @lnt, @lnc;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF @pb IS NULL OR @lb <> @pb OR @ls <> @ps
    BEGIN
        IF @pb IS NOT NULL AND (@at = 0 OR @ac = 0) AND @cell > 1
            UPDATE #lay SET CELL = @cell - 1 WHERE BATCH_ID = @pb AND SEGMENT = @ps AND CELL = @cell;
        SELECT @cell = 1, @at = 0, @ac = 0, @pb = @lb, @ps = @ls;
    END
    ELSE IF @at > 0 AND @ac > 0
        SELECT @cell += 1, @at = 0, @ac = 0;
    UPDATE #lay SET CELL = @cell WHERE BATCH_ID = @lb AND SEGMENT = @ls AND ORD = @lo;
    SELECT @at += @lnt, @ac += @lnc;
    FETCH NEXT FROM lay_cur INTO @lb, @ls, @lo, @lnt, @lnc;
END
IF @pb IS NOT NULL AND (@at = 0 OR @ac = 0) AND @cell > 1
    UPDATE #lay SET CELL = @cell - 1 WHERE BATCH_ID = @pb AND SEGMENT = @ps AND CELL = @cell;
CLOSE lay_cur; DEALLOCATE lay_cur;
""")

# 3. Чеки октября: копии чека один раз (ключ процедуры), плюс максимум чеков за день × канал (ID_ORGANIZATION)
run('#cur', f"""
DROP TABLE IF EXISTS #cur;
SELECT BATCH_ID, ID_CONTACT, Y = sum(CD), CHECKS = sum(N), MAXDAY = max(N)
INTO #cur
FROM (SELECT d.BATCH_ID, d.ID_CONTACT, d.DATA, d.ID_ORGANIZATION, CD = sum(d.COST_DISCOUNT), N = count(*)
      FROM (SELECT m.BATCH_ID, a.ID_CONTACT, a.DATA, a.ID_ORGANIZATION, COST_DISCOUNT = cast(a.COST_DISCOUNT AS float)
            FROM dbo.I_CHECKHEADER AS a (nolock)
                 JOIN #pop AS m ON m.ID_CONTACT = a.ID_CONTACT
            WHERE a.ID_COMPANY = 1 AND a.ID_CONTACT <> 0
              AND a.DATA >= '{YM_FROM}' AND a.DATA < '{YM_TO}'
              AND a.DATA >= m.P_FROM
            GROUP BY m.BATCH_ID, a.ID_CONTACT, a.DATA, a.ID_ORGANIZATION, a.ID_SHOP, a.CLOCK, a.TIME
                   , a.CARD_NUMBER, a.COST, a.COST_DISCOUNT, a.SKU) AS d
      GROUP BY d.BATCH_ID, d.ID_CONTACT, d.DATA, d.ID_ORGANIZATION) AS x
GROUP BY BATCH_ID, ID_CONTACT
OPTION (RECOMPILE);
CREATE CLUSTERED COLUMNSTORE INDEX cci_cur ON #cur;
""")

# 3.1 «Больше 5 чеков в день» ДО нарезки (правило отчёта эффекта, окно только «до»: 105 дней до PRE_TO), копии — один раз.
#     Партия 1 — отдельный запрос (окно 18.03–30.06), партия 2 — отдельный (окно 30.06–12.10): своя граница дат у каждого скана.
run('#pre5', """
DROP TABLE IF EXISTS #pre5;
CREATE TABLE #pre5 (BATCH_ID int NOT NULL, ID_CONTACT int NOT NULL);
""")
#     Границы окна — строкой 'YYYY-MM-DD' из самого сервера: тип даты в Python от драйвера не зависит.
for bid, w_from, w_to in cur.execute("""SELECT BATCH_ID, convert(char(10), dateadd(day, -105, PRE_TO), 23), convert(char(10), PRE_TO, 23)
                                        FROM #b ORDER BY BATCH_ID""").fetchall():
    bid = int(bid)
    run(f'#pre5 b{bid} {w_from}..{w_to}', f"""
    INSERT INTO #pre5 (BATCH_ID, ID_CONTACT)
    SELECT DISTINCT {bid}, d.ID_CONTACT
    FROM (SELECT a.ID_CONTACT, a.DATA, a.ID_ORGANIZATION
          FROM dbo.I_CHECKHEADER AS a (nolock)
               JOIN #pop AS m ON m.ID_CONTACT = a.ID_CONTACT AND m.BATCH_ID = {bid}
          WHERE a.ID_COMPANY = 1 AND a.ID_CONTACT <> 0
            AND a.DATA >= '{w_from}' AND a.DATA < '{w_to}'
          GROUP BY a.ID_CONTACT, a.DATA, a.ID_ORGANIZATION, a.ID_SHOP, a.CLOCK, a.TIME
                 , a.CARD_NUMBER, a.COST, a.COST_DISCOUNT, a.SKU) AS d
    GROUP BY d.ID_CONTACT, d.DATA, d.ID_ORGANIZATION
    HAVING count(*) > 5
    OPTION (RECOMPILE);
    """)

# 3.2 Списки акций, идущих в октябре (I_PROMO: старт ≤ 31.10, финиш ≥ 01.10), кроме тестовых 100000–100019.
#     Для ГКГ любое попадание с CONTROL_GROUP = 0 — «заражение».
run('#op', f"""
DROP TABLE IF EXISTS #op;
SELECT DISTINCT ID_PROMO INTO #op FROM dbo.I_PROMO (nolock)
WHERE ID_COMPANY = 1 AND START_DATE < '{YM_TO}' AND FINISH_DATE >= '{YM_FROM}' AND ID_PROMO NOT BETWEEN 100000 AND 100019;
""")
run('#off', """
DROP TABLE IF EXISTS #off;
SELECT o.ID_CONTACT
     , N_TG  = count(DISTINCT IIF(o.CONTROL_GROUP = 0, o.ID_PROMO, NULL))
     , T426  = max(IIF(o.ID_PROMO = 101426 AND o.CONTROL_GROUP = 0, 1, 0))
     , K426  = max(IIF(o.ID_PROMO = 101426 AND o.CONTROL_GROUP = 1, 1, 0))
     , PP_TG = max(IIF(o.ID_PROMO >= 100000000 AND o.CONTROL_GROUP = 0, 1, 0))
     , DCO_TG = max(IIF(o.ID_PROMO BETWEEN 101476 AND 101496 AND o.CONTROL_GROUP = 0, 1, 0))
INTO #off
FROM dbo.I_PROMO_OFFER AS o (nolock)
     JOIN #op AS p ON p.ID_PROMO = o.ID_PROMO
WHERE o.ID_COMPANY = 1
GROUP BY o.ID_CONTACT
OPTION (RECOMPILE);
CREATE CLUSTERED COLUMNSTORE INDEX cci_off ON #off;
""")

# 3.3 Текущие признаки клиента из I_CVM_CONTACT (одна строка на клиента: меньшая организация)
run('#cc', """
DROP TABLE IF EXISTS #cc;
SELECT ID_CONTACT, LC = SEGMENT, HAS_PUSH, CLUSTER
INTO #cc
FROM (SELECT c.ID_CONTACT, c.SEGMENT, c.HAS_PUSH, c.CLUSTER
           , RN = row_number() OVER (PARTITION BY c.ID_CONTACT ORDER BY c.ID_ORGANIZATION)
      FROM dbo.I_CVM_CONTACT AS c (nolock)
      WHERE c.ID_COMPANY = 1 AND EXISTS (SELECT 1 FROM #pop AS p WHERE p.ID_CONTACT = c.ID_CONTACT)) AS t
WHERE RN = 1;
CREATE CLUSTERED COLUMNSTORE INDEX cci_cc ON #cc;
""")

# 4. Клиентская таблица
run('#cl', """
DROP TABLE IF EXISTS #cl;
SELECT p.BATCH_ID, p.ID_CONTACT, p.SEGMENT, l.CELL, p.LAYER
     , GRP    = IIF(p.GCG = 1, 'CG', 'TG')
     , Y      = isnull(c.Y, 0)
     , BUYER  = IIF(isnull(c.CHECKS, 0) > 0, 1, 0)
     , X      = p.X
     , F_OCT5 = IIF(isnull(c.MAXDAY, 0) > 5, 1, 0)
     , F_PRE5 = IIF(q.ID_CONTACT IS NULL, 0, 1)
     , F_FRAUD = IIF(f.ID_CONTACT IS NULL, 0, 1)
     , EXPO   = CASE WHEN isnull(o.N_TG, 0) = 0 THEN 0 WHEN o.N_TG = 1 THEN 1 WHEN o.N_TG <= 3 THEN 2 ELSE 3 END
     , IN426  = CASE WHEN o.T426 = 1 THEN 1 WHEN o.K426 = 1 THEN 2 ELSE 0 END
     , PP     = isnull(o.PP_TG, 0)
     , DCO    = isnull(o.DCO_TG, 0)
     , LC     = isnull(k.LC, -9)
     , PUSH   = isnull(k.HAS_PUSH, -9)
     , CLU    = isnull(k.CLUSTER, -9)
     , OUTF   = 0
INTO #cl
FROM #pop AS p
     JOIN #lay AS l ON l.BATCH_ID = p.BATCH_ID AND l.SEGMENT = p.SEGMENT AND l.LAYER = p.LAYER
     LEFT JOIN #cur AS c ON c.BATCH_ID = p.BATCH_ID AND c.ID_CONTACT = p.ID_CONTACT
     LEFT JOIN #off AS o ON o.ID_CONTACT = p.ID_CONTACT
     LEFT JOIN #cc  AS k ON k.ID_CONTACT = p.ID_CONTACT
     -- признаки «есть / нет» — соединением с уникальным списком, а не EXISTS в списке полей (13,7 млн строк)
     LEFT JOIN (SELECT DISTINCT BATCH_ID, ID_CONTACT FROM #pre5) AS q ON q.BATCH_ID = p.BATCH_ID AND q.ID_CONTACT = p.ID_CONTACT
     LEFT JOIN (SELECT DISTINCT ID_CONTACT FROM dbo.I_FRAUD_CONTACT (nolock) WHERE ID_COMPANY = 1 AND ID_CONTACT IS NOT NULL) AS f
       ON f.ID_CONTACT = p.ID_CONTACT;
CREATE CLUSTERED COLUMNSTORE INDEX cci_cl ON #cl;
""")

ACKLAM = """CASE WHEN p.P <= 0.97575
  THEN (((((-3.969683028665376e+01 * w.R + 2.209460984245205e+02) * w.R - 2.759285104469687e+02) * w.R
          + 1.383577518672690e+02) * w.R - 3.066479806614716e+01) * w.R + 2.506628277459239e+00) * w.QC
     / (((((-5.447609879822406e+01 * w.R + 1.615858368580409e+02) * w.R - 1.556989798598866e+02) * w.R
          + 6.680131188771972e+01) * w.R - 1.328068155288572e+01) * w.R + 1.0)
  ELSE -(((((-7.784894002430293e-03 * w.QT - 3.223964580411365e-01) * w.QT - 2.400758277161838e+00) * w.QT
          - 2.549732539343734e+00) * w.QT + 4.374664141464968e+00) * w.QT + 2.938163982698783e+00)
     / ((((7.784695709041462e-03 * w.QT + 3.224671290700398e-01) * w.QT + 2.445134137142996e+00) * w.QT
          + 3.754408661907416e+00) * w.QT + 1.0)
  END"""

# 5.1 Выбросы «ДО» (κ = ½, ЦГ и ГКГ вместе) — помечаются OUTF = 1, не удаляются
run('out X', f"""
DROP TABLE IF EXISTS #gx;
SELECT q.BATCH_ID, q.SEGMENT, q.CELL, G_X = IIF(q.P75 > q.M, exp(q.M + z.Z * (q.P75 - q.M) / 0.674), NULL)
INTO #gx
FROM (SELECT BATCH_ID, SEGMENT, CELL, M, P75, N
      FROM (SELECT BATCH_ID, SEGMENT, CELL
                 , M   = PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY V) OVER (PARTITION BY BATCH_ID, SEGMENT, CELL)
                 , P75 = PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY V) OVER (PARTITION BY BATCH_ID, SEGMENT, CELL)
                 , N   = count(*) OVER (PARTITION BY BATCH_ID, SEGMENT, CELL)
                 , RN  = row_number() OVER (PARTITION BY BATCH_ID, SEGMENT, CELL ORDER BY (SELECT NULL))
            FROM (SELECT BATCH_ID, SEGMENT, CELL, V = log(IIF(X > 0, X, NULL)) FROM #cl WHERE X > 0) AS t) AS q0
      WHERE RN = 1) AS q
     CROSS APPLY (SELECT P = 1.0 - 0.5E0 / q.N) AS p   -- κ как float, как @kappa в процедуре (0.5 / N давало decimal с округлением)
     CROSS APPLY (SELECT QC = p.P - 0.5, R = square(p.P - 0.5), QT = sqrt(-2.0 * log(1.0 - p.P))) AS w
     CROSS APPLY (SELECT Z = {ACKLAM}) AS z;

UPDATE b SET OUTF = 1
FROM #cl AS b LEFT JOIN #gx AS g ON g.BATCH_ID = b.BATCH_ID AND g.SEGMENT = b.SEGMENT AND g.CELL = b.CELL
WHERE b.X < 0 OR b.X > g.G_X;
""")

# 5.1а ячейка без одной из групп после «ДО» — к предыдущей (или следующей) ячейке с обеими группами
run('cell fix', """
DROP TABLE IF EXISTS #cellx;
SELECT BATCH_ID, SEGMENT, CELL, OK = IIF(min(GRP) <> max(GRP), 1, 0)
INTO #cellx FROM #cl WHERE OUTF = 0 GROUP BY BATCH_ID, SEGMENT, CELL;

UPDATE b SET CELL = isnull(p.CELL, n.CELL)
FROM #cl AS b
     JOIN #cellx AS x ON x.BATCH_ID = b.BATCH_ID AND x.SEGMENT = b.SEGMENT AND x.CELL = b.CELL AND x.OK = 0
     OUTER APPLY (SELECT CELL = max(y.CELL) FROM #cellx AS y WHERE y.BATCH_ID = b.BATCH_ID AND y.SEGMENT = b.SEGMENT AND y.OK = 1 AND y.CELL < b.CELL) AS p
     OUTER APPLY (SELECT CELL = min(y.CELL) FROM #cellx AS y WHERE y.BATCH_ID = b.BATCH_ID AND y.SEGMENT = b.SEGMENT AND y.OK = 1 AND y.CELL > b.CELL) AS n
WHERE b.OUTF = 0 AND isnull(p.CELL, n.CELL) IS NOT NULL;
""")

# 5.2 Выбросы месяца: m, s — по покупателям ГКГ ячейки, n — все покупатели ячейки; помечаются OUTF = 2
run('out Y', f"""
DROP TABLE IF EXISTS #gy;
SELECT n.BATCH_ID, n.SEGMENT, n.CELL
     , G_Y = IIF(q.NC >= 4 AND q.P75 > q.M, exp(q.M + z.Z * (q.P75 - q.M) / 0.674), NULL)
INTO #gy
FROM (SELECT BATCH_ID, SEGMENT, CELL, N = sum(BUYER) FROM #cl WHERE OUTF = 0 GROUP BY BATCH_ID, SEGMENT, CELL) AS n
     LEFT JOIN (SELECT BATCH_ID, SEGMENT, CELL, M, P75, NC
                FROM (SELECT BATCH_ID, SEGMENT, CELL
                           , M   = PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY V) OVER (PARTITION BY BATCH_ID, SEGMENT, CELL)
                           , P75 = PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY V) OVER (PARTITION BY BATCH_ID, SEGMENT, CELL)
                           , NC  = count(*) OVER (PARTITION BY BATCH_ID, SEGMENT, CELL)
                           , RN  = row_number() OVER (PARTITION BY BATCH_ID, SEGMENT, CELL ORDER BY (SELECT NULL))
                      FROM (SELECT BATCH_ID, SEGMENT, CELL, V = log(IIF(Y > 0, Y, NULL)) FROM #cl WHERE OUTF = 0 AND GRP = 'CG' AND Y > 0) AS t) AS q0
                WHERE RN = 1) AS q
       ON q.BATCH_ID = n.BATCH_ID AND q.SEGMENT = n.SEGMENT AND q.CELL = n.CELL
     CROSS APPLY (SELECT P = 1.0 - 0.5E0 / nullif(n.N, 0)) AS p
     CROSS APPLY (SELECT QC = p.P - 0.5, R = square(p.P - 0.5), QT = sqrt(-2.0 * log(1.0 - p.P))) AS w
     CROSS APPLY (SELECT Z = {ACKLAM}) AS z;

UPDATE b SET OUTF = 2
FROM #cl AS b JOIN #gy AS g ON g.BATCH_ID = b.BATCH_ID AND g.SEGMENT = b.SEGMENT AND g.CELL = b.CELL
WHERE b.OUTF = 0 AND abs(b.Y) > g.G_Y;
""")

# 6. Выдача: суммы по ячейке × группе × признакам (наборы группировки), клиентских строк наружу нет
AGG = """N = count(*), BUY = sum(BUYER), SY = sum(Y), SX = sum(X), SYY = sum(Y * Y), SXX = sum(X * X), SXY = sum(X * Y)"""
dump('A', f"""SELECT BATCH_ID, SEGMENT, CELL, GRP, OUTF, F_FRAUD, F_PRE5, F_OCT5, EXPO, IN426, PP, DCO, {AGG}
FROM #cl GROUP BY BATCH_ID, SEGMENT, CELL, GRP, OUTF, F_FRAUD, F_PRE5, F_OCT5, EXPO, IN426, PP, DCO""")
dump('LC', f"""SELECT BATCH_ID, SEGMENT, CELL, GRP, OUTF, LC, {AGG} FROM #cl GROUP BY BATCH_ID, SEGMENT, CELL, GRP, OUTF, LC""")
dump('CLU', f"""SELECT BATCH_ID, SEGMENT, CELL, GRP, OUTF, CLU, {AGG} FROM #cl GROUP BY BATCH_ID, SEGMENT, CELL, GRP, OUTF, CLU""")
dump('PUSH', f"""SELECT BATCH_ID, SEGMENT, CELL, GRP, OUTF, PUSH, {AGG} FROM #cl GROUP BY BATCH_ID, SEGMENT, CELL, GRP, OUTF, PUSH""")
dump('LAYER', f"""SELECT BATCH_ID, SEGMENT, CELL, LAYER, GRP, OUTF, {AGG} FROM #cl GROUP BY BATCH_ID, SEGMENT, CELL, LAYER, GRP, OUTF""")
log('done')
