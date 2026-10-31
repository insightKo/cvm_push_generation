# Т2 модели оттока: проверка представительности панели 10 % (I_CVM_CHURN_PANEL) против всей базы — только чтение.
# Запуск: ssh kz 'ssh ru "docker exec -i cvm-api python -"' < t2_panel_check.py > t2_panel_check.out
# Три среза (воскресенья): 22.09.2024, 21.09.2025, 13.09.2026 (последний размеченный — исход 6 нед. известен).
# На срез: один проход I_CHECKHEADER по диапазону дат (D − 406 … D + 42, отсечение групп строк по DATA), сегмент по правилу
# CVM_CONTACT на дату (упрощение для проверки: средний интервал недель — по организации, (последняя − первая) / (недель − 1)),
# первая покупка — I_CVM_CONTACT.FIRST_DATA; пуш доступен — доставка/переход канала 1 за 4 нед. до среза.
# Сравнение: доли панели и всей базы по сегментам, организациям, каналу, региону, году регистрации, децилям трат 26 нед.,
# пушу, доле ушедших за 6 нед.; z = (доля панели − доля базы) / ошибка выборки 10 % без возвращения.
import sys, json, time, pyodbc
from app.config import Settings

cn = pyodbc.connect(Settings.from_env().connection_string(), timeout=30, autocommit=True)
cn.timeout = 3600   # один вызов среза = пакет из нескольких проходов по чекам; 900 с не хватит
cur = cn.cursor()
cur.execute("set transaction isolation level read uncommitted; set lock_timeout 5000; set deadlock_priority low")
OUT = {}


def run(name, sql, params=()):
    t = time.time()
    try:
        cur.execute(sql, params)
        rows, cols = [], None
        while True:
            if cur.description is not None:
                cols = [c[0] for c in cur.description]
                while True:
                    part = cur.fetchmany(1000)
                    if not part:
                        break
                    rows += [[v if isinstance(v, (int, float)) or v is None else str(v) for v in r] for r in part]
            if not cur.nextset():
                break
        OUT[name] = {'cols': cols, 'rows': rows, 'sec': round(time.time() - t, 1)}
    except Exception as e:
        OUT[name] = {'error': str(e)[:400], 'sec': round(time.time() - t, 1)}
    print(name, OUT[name].get('sec'), 'ERR ' + OUT[name]['error'] if 'error' in OUT[name] else len(OUT[name]['rows']))
    sys.stdout.flush()


# 0. Панель есть? Партии и итог по когортам
run('batch', "select * from dbo.I_CVM_CHURN_PANEL_BATCH with (nolock)")
run('cohort_worst', """select top 15 COHORT_YM, CLIENTS, PANEL_CLIENTS, TRIES, ERR_BUDGET, ERR_LTV, CONVERGED
from dbo.I_CVM_CHURN_PANEL_COHORT with (nolock) order by ERR_BUDGET desc""")
if not OUT['batch'].get('rows'):
    print('JSON>>' + json.dumps(OUT, ensure_ascii=False))
    sys.exit(0)

# 1. Общие справочники сессии: первая покупка (клиент × орг.), фрод, панель, регион магазина, год регистрации
run('prep', """
set nocount on;
drop table if exists #fp, #fr, #pn, #sh, #rg;
select ID_CONTACT, ID_ORGANIZATION, FIRST_DATA = min(FIRST_DATA)
into #fp from dbo.I_CVM_CONTACT with (nolock)
where ID_COMPANY = 1 and ID_ORGANIZATION in (1, 3) and ID_CONTACT <> 0 and FIRST_DATA is not null
group by ID_CONTACT, ID_ORGANIZATION;
create clustered columnstore index c_fp on #fp;
select ID_CONTACT into #fr from (select ID_CONTACT from dbo.I_FRAUD_CONTACT with (nolock) where ID_COMPANY = 1
                                 union select ID_CONTACT from dbo.I_MISTAKE_CONTACT with (nolock) where ID_COMPANY = 1) x;
create clustered index c_fr on #fr (ID_CONTACT);
select ID_CONTACT into #pn from dbo.I_CVM_CHURN_PANEL with (nolock) where ID_COMPANY = 1;
create clustered columnstore index c_pn on #pn;
select ID_SHOP, REG = max(ID_CITY_REPORT) into #sh from dbo.I_SHOP with (nolock) where ID_COMPANY = 1 group by ID_SHOP;
create clustered index c_sh on #sh (ID_SHOP);
select ID_CONTACT, RY = max(year(REGISTRATION_DATE)) into #rg from dbo.I_CONTACT with (nolock) where ID_COMPANY = 1 group by ID_CONTACT;
create clustered columnstore index c_rg on #rg;
select fp = (select count(*) from #fp), fr = (select count(*) from #fr), pn = (select count(*) from #pn), rg = (select count(*) from #rg);
""")

SLICE = """
set nocount on;
declare @D date = ?;
-- начало диапазона: -407, а не -406: окно LFL (год назад) в високосный год начинается на день раньше (22.09.2024 → 12.08.2023)
declare @f date = dateadd(day, -407, @D), @a date = dateadd(day, 42, @D), @d6 date = dateadd(day, -41, @D),
        @d52 date = dateadd(day, -363, @D), @d56 date = dateadd(day, -391, @D), @d26 date = dateadd(day, -181, @D),
        @l1 date = dateadd(year, -1, dateadd(day, -41, @D)), @l2 date = dateadd(year, -1, @D), @p4 date = dateadd(day, -27, @D);
drop table if exists #w, #ms, #pu, #s, #r;
-- клиент × орг.: недели 56 нед., чеки 52 и 6 нед., LFL-окно год назад, траты 26 нед., чеки 6 нед. после среза
select h.ID_CONTACT, h.ID_ORGANIZATION
     , W_MIN = min(case when h.DATA >= @d56 and h.DATA <= @D then h.WEEKNUMBER end)
     , W_MAX = max(case when h.DATA >= @d56 and h.DATA <= @D then h.WEEKNUMBER end)
     , N_W   = count(distinct case when h.DATA >= @d56 and h.DATA <= @D then h.WEEKNUMBER end)
     , C52   = sum(case when h.DATA >= @d52 and h.DATA <= @D then 1 else 0 end)
     , C6    = sum(case when h.DATA >= @d6  and h.DATA <= @D then 1 else 0 end)
     , LFL   = max(case when h.DATA >= @l1  and h.DATA <= @l2 then 1 else 0 end)
     , S26   = sum(case when h.DATA >= @d26 and h.DATA <= @D then h.COST_DISCOUNT else 0 end)
     , CA    = sum(case when h.DATA > @D then 1 else 0 end)
into #w
from dbo.I_CHECKHEADER h with (nolock)
where h.ID_COMPANY = 1 and h.ID_ORGANIZATION in (1, 3) and h.ID_CONTACT <> 0 and h.DATA >= @f and h.DATA <= @a
group by h.ID_CONTACT, h.ID_ORGANIZATION
option (maxdop 2, max_grant_percent = 10);
create clustered columnstore index c_w on #w;
-- основной магазин за 26 нед. (больше всего чеков) → регион
-- регион присоединяется после выбора магазина (join на сотни миллионов строк до группировки не нужен)
select x.ID_CONTACT, x.ID_ORGANIZATION, REG = max(sh.REG)
into #ms
from (select h.ID_CONTACT, h.ID_ORGANIZATION, h.ID_SHOP, rn = row_number() over (partition by h.ID_CONTACT, h.ID_ORGANIZATION order by count(*) desc, h.ID_SHOP)
      from dbo.I_CHECKHEADER h with (nolock)
      where h.ID_COMPANY = 1 and h.ID_ORGANIZATION in (1, 3) and h.ID_CONTACT <> 0 and h.DATA >= @d26 and h.DATA <= @D
      group by h.ID_CONTACT, h.ID_ORGANIZATION, h.ID_SHOP) x
     left join #sh sh on sh.ID_SHOP = x.ID_SHOP
where x.rn = 1
group by x.ID_CONTACT, x.ID_ORGANIZATION
option (maxdop 2, max_grant_percent = 10);
create clustered columnstore index c_ms on #ms;
-- пуш доставлен (или переход) за 4 нед. до среза — клиент
select distinct d.ID_CONTACT into #pu
from dbo.I_PROMO_DELIVERY_REPORT d with (nolock)
where d.ID_COMPANY = 1 and d.ID_CHANNEL = 1 and d.STATUS in (N'Доставлено', N'Переход по ссылке')
  and d.SEND_DT >= cast(@p4 as datetime) and d.SEND_DT < dateadd(day, 1, cast(@D as datetime))
option (maxdop 2, max_grant_percent = 10);
create clustered columnstore index c_pu on #pu;
-- сегмент на дату (все клиент × орг. с первой покупкой не позже среза)
select fp.ID_CONTACT, fp.ID_ORGANIZATION
     , SEG = case when fr.ID_CONTACT is not null then 0
                  when fp.FIRST_DATA >= @d6 then 1
                  when isnull(w.C52, 0) > 0
                       and (case when w.N_W <= 1 then 56 else round((w.W_MAX - w.W_MIN) * 1.0 / (w.N_W - 1), 0) end) > 6
                       then case when fp.ID_ORGANIZATION = 3 then case when w.C6 > 0 then 2 else 5 end else 4 end
                  when isnull(w.C52, 0) > 0 and w.C6 = 0 then 5
                  when w.C6 > 0 then case when fp.ID_ORGANIZATION = 3 then 2 when w.LFL = 1 then 3 else 2 end
                  else 6 end
     , C6 = isnull(w.C6, 0), C52 = isnull(w.C52, 0), S26 = isnull(w.S26, 0), CA = isnull(w.CA, 0)
     , PAN = case when pn.ID_CONTACT is not null then 1 else 0 end
into #s
from #fp fp
left join #w w on w.ID_CONTACT = fp.ID_CONTACT and w.ID_ORGANIZATION = fp.ID_ORGANIZATION
left join #fr fr on fr.ID_CONTACT = fp.ID_CONTACT
left join #pn pn on pn.ID_CONTACT = fp.ID_CONTACT
where fp.FIRST_DATA <= @D
option (maxdop 2, max_grant_percent = 10);
create clustered columnstore index c_s on #s;
-- выход: измерение, значение, вся база, панель
create table #r (DIM varchar(20), VAL varchar(40), N_ALL bigint, N_PAN bigint);
insert #r select 'seg', concat(ID_ORGANIZATION, '-', SEG), count(*), sum(PAN) from #s group by ID_ORGANIZATION, SEG;
-- дальше — население среза С1: покупка в организации за 6 нед. до среза, без фрода
insert #r select 'org', ID_ORGANIZATION, count(*), sum(PAN) from #s where C6 > 0 and SEG <> 0 group by ID_ORGANIZATION;
insert #r select 'channel', CH, count(*), sum(PAN)
  from (select ID_CONTACT, CH = case when count(*) = 2 then 'omni' else concat('only', max(ID_ORGANIZATION)) end, PAN = max(PAN)
        from #s where C52 > 0 and SEG <> 0 group by ID_CONTACT) x group by CH;
insert #r select 'region', concat(s.ID_ORGANIZATION, '-', isnull(ms.REG, -1)), count(*), sum(s.PAN)
  from #s s left join #ms ms on ms.ID_CONTACT = s.ID_CONTACT and ms.ID_ORGANIZATION = s.ID_ORGANIZATION
  where s.C6 > 0 and s.SEG <> 0 group by s.ID_ORGANIZATION, ms.REG;
insert #r select 'reg_year', concat(s.ID_ORGANIZATION, '-', isnull(rg.RY, -1)), count(*), sum(s.PAN)
  from #s s left join #rg rg on rg.ID_CONTACT = s.ID_CONTACT where s.C6 > 0 and s.SEG <> 0 group by s.ID_ORGANIZATION, rg.RY;
insert #r select 'spend_dec', concat(ID_ORGANIZATION, '-', DEC), count(*), sum(PAN)
  from (select ID_ORGANIZATION, PAN, DEC = ntile(10) over (partition by ID_ORGANIZATION order by S26) from #s where C6 > 0 and SEG <> 0) x
  group by ID_ORGANIZATION, DEC
  option (maxdop 2);
insert #r select 'push', concat(s.ID_ORGANIZATION, '-', case when pu.ID_CONTACT is null then 0 else 1 end), count(*), sum(s.PAN)
  from #s s left join #pu pu on pu.ID_CONTACT = s.ID_CONTACT where s.C6 > 0 and s.SEG <> 0
  group by s.ID_ORGANIZATION, case when pu.ID_CONTACT is null then 0 else 1 end;
insert #r select 'churn6', concat(ID_ORGANIZATION, '-', SEG, '-', case when CA = 0 then 1 else 0 end), count(*), sum(PAN)
  from #s where C6 > 0 and SEG <> 0 group by ID_ORGANIZATION, SEG, case when CA = 0 then 1 else 0 end;
insert #r select 'spend_sum', ID_ORGANIZATION, cast(sum(S26) as bigint), cast(sum(case when PAN = 1 then S26 else 0 end) as bigint)
  from #s where C6 > 0 and SEG <> 0 group by ID_ORGANIZATION;
select * from #r;
"""
for d in ('2024-09-22', '2025-09-21', '2026-09-13'):
    run('slice.' + d, SLICE, (d,))

print('JSON>>' + json.dumps(OUT, ensure_ascii=False))
