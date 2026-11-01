
CREATE PROCEDURE  [dbo].[CVM_CONTACT]
as
set nocount on;
set quoted_identifier on;


--truncate table I_CVM_CONTACT	-- перенесено прямо перед вставкой (INSERT INTO I_CVM_CONTACT)

drop table if exists #c

select ID_COMPANY
		, RN = ROW_NUMBER() over (order by ID_COMPANY)
into #c
from I_ORGANIZATION (nolock)
group by  ID_COMPANY

DECLARE @c int = 1
DECLARE @idc int = (select ID_COMPANY from #c where RN=@c)

WHILE @c<= 1--(select max(RN) from #c)

BEGIN

declare @act_date date = (select max(DATA) from I_CHECKHEADER (nolock) where ID_COMPANY=@idc) 


--- FOR NEW, ACTIVE, ACTIVE-LFL, RANDOM, CHURN

-- Т2 (01.11.2026, ускорение ночного задания; цифры те же): вся история чеков читается один раз — #hist (клиент × организация),
-- из него #first_purch, «Новые» и канал #m; раньше — три прохода по всей истории
declare @d6 date = dateadd(day,+1, dateadd(week,-6,@act_date))

drop table if exists #hist;

create table #hist (ID_CONTACT int, ID_ORGANIZATION int, ID_COMPANY int, MIN_DATA date, LAST_DATA date, LAST_YM int, FIRST_YM int
	, COUNT_CHECK_FP int, COST_DISCOUNT_FP decimal(38,2), CD_6W decimal(38,2), MIN_DATA_6W date
	, index cci_hist clustered columnstore)

insert into #hist with (tablock)
select ID_CONTACT
	, ID_ORGANIZATION
	, ID_COMPANY
	, MIN_DATA = min(DATA)
	, LAST_DATA = max(DATA)
	, LAST_YM = max(YEAR_MONTH)
	, FIRST_YM = min(YEAR_MONTH)
	, COUNT_CHECK_FP = count(ID_CHECK)
	, COST_DISCOUNT_FP = sum(COST_DISCOUNT)
	, CD_6W = sum(iif(DATA >= @d6, COST_DISCOUNT, 0))			-- для #m: сумма чеков за 6 недель
	, MIN_DATA_6W = min(iif(DATA >= @d6, DATA, null))			-- для #m: первая покупка за 6 недель
from I_CHECKHEADER (nolock)
where ID_CONTACT != 0 and ID_COMPANY = @idc
group by ID_CONTACT
	, ID_ORGANIZATION
	, ID_COMPANY
option (recompile)

drop table if exists #first_purch;

select h.ID_CONTACT
	, h.ID_ORGANIZATION
	, h.ID_COMPANY
	, h.MIN_DATA
	, h.LAST_DATA
	, h.LAST_YM
	, h.FIRST_YM
	, h.COUNT_CHECK_FP
	, h.COST_DISCOUNT_FP
into #first_purch
from	#hist h
		inner join I_ORGANIZATION c (nolock)
		on h.ID_ORGANIZATION = c.ID_ORGANIZATION

create clustered columnstore index cci_first_purch on #first_purch


---- 0 -fraud
drop table if exists  #for_basic_segment

select a.ID_COMPANY
		, b.ID_ORGANIZATION
		, ID_CONTACT
		, SEGMENT=0
		, REFERENCE_DATE = @act_date
INTO #for_basic_segment
from	I_FRAUD_CONTACT (nolock) as a
		inner join I_ORGANIZATION as b (nolock)
		on a.ID_COMPANY=b.ID_COMPANY
where a.ID_COMPANY=@idc
group by  a.ID_COMPANY
		, b.ID_ORGANIZATION
		, ID_CONTACT

INSERT INTO #for_basic_segment
select a.ID_COMPANY
		, b.ID_ORGANIZATION
		, a.ID_CONTACT
		, SEGMENT=0
		, REFERENCE_DATE = @act_date
from	I_MISTAKE_CONTACT (nolock) as a
		inner join I_ORGANIZATION as b (nolock)
		on a.ID_COMPANY=b.ID_COMPANY
		left join  (select ID_COMPANY, ID_CONTACT from I_FRAUD_CONTACT (nolock) group by ID_COMPANY, ID_CONTACT)  as g
			on a.ID_CONTACT = g.ID_CONTACT	
			and a.ID_COMPANY=g.ID_COMPANY
where g.ID_CONTACT is null and  a.ID_COMPANY=@idc
group by  a.ID_COMPANY
		, b.ID_ORGANIZATION
		, a.ID_CONTACT

drop table if exists #0
select *
into #0
from #for_basic_segment

--1- new

-- Т2: первая покупка в организации за 6 недель — прямо из #first_purch (раньше тот же отбор шёл проходом по всей истории)
INSERT INTO #for_basic_segment
select   b.ID_COMPANY
		, b.ID_ORGANIZATION
		, b.ID_CONTACT
		, SEGMENT = 1
		, REFERENCE_DATE = @act_date
from	#first_purch b
where	b.MIN_DATA between @d6 and @act_date
		and not exists (select 1 from #0 as g
						where g.ID_CONTACT = b.ID_CONTACT
							and g.ID_COMPANY = b.ID_COMPANY
							and g.ID_ORGANIZATION = b.ID_ORGANIZATION)

drop table if exists #1
select *
into #1
from #for_basic_segment

-- Т2: окна 56 и 52 недели, 6 недель и 6 недель год назад — один проход по чекам в #wk (клиент × организация × неделя,
-- число чеков в каждом окне); «Случайные», «Отток», «Активные» считаются из него (раньше — шесть проходов по чекам)
declare @d56 date = dateadd(day,+1, dateadd(week,-56,@act_date))
	, @d52 date = dateadd(day,+1, dateadd(week,-52,@act_date))
	, @ly1 date = dateadd( year,-1,dateadd(day,+1, dateadd(week,-6,@act_date)))
	, @ly2 date = dateadd(year,-1,@act_date)
declare @dw date = iif(@ly1 < @d56, @ly1, @d56)

drop table if exists #wk;

create table #wk (ID_CONTACT int, ID_ORGANIZATION int, ID_COMPANY int, WEEKNUMBER int
	, N56 int, N52 int, N6 int, NLY int
	, index cci_wk clustered columnstore)

insert into #wk with (tablock)
select ID_CONTACT
	, ID_ORGANIZATION
	, ID_COMPANY
	, WEEKNUMBER
	, N56 = sum(iif(DATA >= @d56, 1, 0))
	, N52 = sum(iif(DATA >= @d52, 1, 0))
	, N6 = sum(iif(DATA >= @d6, 1, 0))
	, NLY = sum(iif(DATA between @ly1 and @ly2, 1, 0))
from I_CHECKHEADER (nolock)
where ID_COMPANY = @idc and DATA between @dw and @act_date
	and ID_CONTACT != 0		-- ревью Т2: чеки без карты (клиент 0, NULL) в исходнике давали только строки «Отток» клиента 0 / NULL, в таблицу они не проходят (inner join #first_purch)
group by ID_CONTACT
	, ID_ORGANIZATION
	, ID_COMPANY
	, WEEKNUMBER
option (recompile)

-- 4- RANDOM

drop table if exists #weeks

;with a as
(
select  ID_CONTACT
		, ID_ORGANIZATION
		, WEEKNUMBER
		, ID_COMPANY
from #wk
where N56 > 0 and ID_CONTACT != 0
), one_purch as
(
select ID_CONTACT
	 , ID_ORGANIZATION
from a
group by ID_CONTACT
	 , ID_ORGANIZATION
having count(*) = 1
), with_first_purch as
(
select a.ID_CONTACT
	, a.ID_ORGANIZATION
	, a.WEEKNUMBER
	, a.ID_COMPANY
	, DELTA_WEEK = case when c.ID_CONTACT is not null then 56
						else a.WEEKNUMBER - lag(a.WEEKNUMBER) over(partition by a.ID_CONTACT order by a.WEEKNUMBER)
					end
from	a 
		left join one_purch c
		on a.ID_CONTACT = c.ID_CONTACT
		and a.ID_ORGANIZATION = c.ID_ORGANIZATION
)
select 
	a.ID_CONTACT
	, a.ID_ORGANIZATION
	, a.ID_COMPANY
	, AVG_DELTA_WEEK = round(avg(DELTA_WEEK*1.0), 0)
into #weeks
from with_first_purch a
where DELTA_WEEK is not null
group by a.ID_CONTACT
	, a.ID_ORGANIZATION
	, a.ID_COMPANY

create clustered columnstore index cci_weeks on #weeks
	

drop table if exists #random;
;with c as
(
select	ID_CONTACT
		, ID_ORGANIZATION
		, ID_COMPANY
		, E_SEGMENT = sum(N6)
from #wk
where	ID_CONTACT != 0
group by ID_CONTACT, ID_ORGANIZATION, ID_COMPANY
having sum(N52) > 0
)
INSERT into #for_basic_segment
select	 a.ID_COMPANY
		, a.ID_ORGANIZATION
		, a.ID_CONTACT
		, SEGMENT = IIF (a.ID_ORGANIZATION=3 , IIF(E_SEGMENT >0,2,5) ,4)
		, REFERENCE_DATE = @act_date
from	#weeks a 
		inner join  c 
			on a.ID_CONTACT = c.ID_CONTACT
			and a.ID_ORGANIZATION = c.ID_ORGANIZATION
			and a.ID_COMPANY=c.ID_COMPANY
		left join #1 as b			
			on a.ID_CONTACT = b.ID_CONTACT
			and a.ID_ORGANIZATION = b.ID_ORGANIZATION
			and a.ID_COMPANY=b.ID_COMPANY
where	AVG_DELTA_WEEK >6
		and b.ID_CONTACT is null
		and a.ID_COMPANY=@idc
group by	a.ID_CONTACT
			, a.ID_ORGANIZATION
			, a.ID_COMPANY
			, E_SEGMENT
	
drop table if exists #2
select *
into #2
from #for_basic_segment
-- 5- CHURN

;with t as
(
select	ID_CONTACT
		, ID_ORGANIZATION
		, ID_COMPANY
from #wk
where	ID_CONTACT != 0
group by ID_CONTACT, ID_ORGANIZATION, ID_COMPANY
having sum(N6) > 0
), a as (
select	ID_CONTACT
		, ID_ORGANIZATION
		, ID_COMPANY
from #wk
group by ID_CONTACT, ID_ORGANIZATION, ID_COMPANY
having sum(N52) > 0
)
INSERT INTO #for_basic_segment
select   a.ID_COMPANY
		, a.ID_ORGANIZATION
		, a.ID_CONTACT
		, SEGMENT = 5
		, REFERENCE_DATE = @act_date
from a
	left join t
		on a.ID_CONTACT = t.ID_CONTACT
		and a.ID_ORGANIZATION = t.ID_ORGANIZATION
		and a.ID_COMPANY=t.ID_COMPANY
	left join #2 as b
		on a.ID_CONTACT = b.ID_CONTACT
		and a.ID_ORGANIZATION = b.ID_ORGANIZATION
		and a.ID_COMPANY=b.ID_COMPANY
where	b.ID_CONTACT is null
		and t.ID_CONTACT is null
		and a.ID_COMPANY=@idc
group by	a.ID_CONTACT
			, a.ID_ORGANIZATION
			, a.ID_COMPANY 

drop table if exists #5
select *
into #5
from #for_basic_segment

-- 2- ACTIVE, 3 - ACTIVE LFL

;with b as
(
select	ID_CONTACT
		, ID_COMPANY
		, ID_ORGANIZATION
from #wk
group by ID_CONTACT
		, ID_COMPANY
		, ID_ORGANIZATION
having sum(NLY) > 0
), a as (
select	ID_CONTACT
		, ID_ORGANIZATION
		, ID_COMPANY
from #wk
where	ID_CONTACT != 0
group by ID_CONTACT, ID_ORGANIZATION, ID_COMPANY
having sum(N6) > 0
)
INSERT INTO #for_basic_segment
select   a.ID_COMPANY
		, a.ID_ORGANIZATION
		, a.ID_CONTACT
		, SEGMENT = IIF(a.ID_ORGANIZATION=3,2, IIF(b.ID_CONTACT is null,2,3))
		, REFERENCE_DATE = @act_date
from	a
		left join b
		on a.ID_COMPANY=b.ID_COMPANY
		and a.ID_ORGANIZATION=b.ID_ORGANIZATION
		and a.ID_CONTACT=b.ID_CONTACT
		left join #5 as c
			on a.ID_CONTACT = c.ID_CONTACT
			and a.ID_COMPANY = c.ID_COMPANY
			and a.ID_ORGANIZATION=c.ID_ORGANIZATION
where c.ID_CONTACT is null 	and 
		a.ID_COMPANY =@idc
group by a.ID_CONTACT
	, a.ID_ORGANIZATION
	, a.ID_COMPANY
	, b.ID_CONTACT

drop table if exists #23
select *
into #23
from #for_basic_segment

--sleeping

INSERT INTO #for_basic_segment
select   a.ID_COMPANY
		, a.ID_ORGANIZATION
		, a.ID_CONTACT
		, SEGMENT = 6
		, REFERENCE_DATE = @act_date
from #first_purch  as a
		left join #23 as b
		on a.ID_CONTACT = b.ID_CONTACT
		and a.ID_ORGANIZATION = b.ID_ORGANIZATION
		and a.ID_COMPANY=b.ID_COMPANY
where	 b.ID_CONTACT is null
group by	a.ID_CONTACT
			, a.ID_ORGANIZATION
			, a.ID_COMPANY 

---channel select 
--	DECLARE @c int = 1
--DECLARE @idc int = (select ID_COMPANY from #c where RN=@c)


--declare @act_date date = (select max(DATA) from I_CHECKHEADER (nolock) where ID_COMPANY=@idc) 


drop table if exists #m

-- Т2: канал — из #hist (строка на клиента × организацию, поэтому count(*) = число организаций); раньше — проход по всей истории
select	ID_CONTACT
		, [ID_SALE_CHANNEL] = IIF(count(*)=2, 2, max(a.ID_ORGANIZATION) ) --1- offline , 2-omni, 3-online
		, a.ID_COMPANY
		, COST_DISCOUNT_B = IIF(count(*)=2, sum(CD_6W), 0)
		, MIN_DATA_M =  IIF(count(*)=2, null, min(MIN_DATA_6W))
into #m
from #hist as a
where a.ID_COMPANY=@idc
and ID_ORGANIZATION<>2
group by 	ID_CONTACT
		, a.ID_COMPANY

create clustered columnstore index cci_m on #m

--basic index
drop table if exists  #all_p

select  b.ID_COMPANY
		, b.ID_ORGANIZATION
		, b.ID_CONTACT
		, SEGMENT
		, REFERENCE_DATE
		, VISITS = count(distinct ID_CONTACT_DATA)
		, CHECKS = count(ID_CHECK)
		, VISITS_PER_WEEK = (count(distinct ID_CONTACT_DATA)*1.00)/6.00
		, AVG_VISIT= IIF(isnull(count(distinct ID_CONTACT_DATA),0)=0 , NULL, sum(COST_Discount) / count(distinct ID_CONTACT_DATA))
		, AVG_CHECK= IIF(isnull(count(ID_CHECK),0)=0 , NULL, sum(COST_Discount) / count(ID_CHECK))
		, MAX_CHECK = max(COST_DISCOUNT)
		, BUDGET = sum(COST_Discount)
		, AVG_COST_SKU= IIF(isnull(sum(SKU),0)=0 , NULL, sum(COST_Discount)/ sum(SKU))
		, AVG_SKU=IIF(isnull(count(distinct ID_CONTACT_DATA),0)=0 , NULL, (sum(SKU)*1.00)/count(distinct ID_CONTACT_DATA))
		, SHARE_WEEKEND = IIF(isnull(count(distinct ID_CONTACT_DATA),0)=0 , NULL, sum( IIF(DAYTYPE!=1,1.00,0.00))/(count(distinct ID_CONTACT_DATA)*1.00))
		, SHARE_WORKDAY =  IIF(isnull(count(distinct ID_CONTACT_DATA),0)=0 , NULL, sum( IIF(DAYTYPE=1,1.00,0.00))/(count(distinct ID_CONTACT_DATA)*1.00))
		, TIME_PURCHASE_WEEKEND = case when isnull(sum( IIF(DAYTYPE!=1,1.00,0.00)),0)=0 then NULL
									else sum(IIF(DAYTYPE!=1, CLOCK*1.00+([TIME]*1.00)/60,0.00) )/sum( IIF(DAYTYPE!=1,1.00,0.00)) end 
		, TIME_PURCHASE_WORKDAY = case when isnull(sum( IIF(DAYTYPE=1,1.00,0.00)),0)=0 then NULL
									else sum(IIF(DAYTYPE=1, CLOCK*1.00+([TIME]*1.00)/60,0.00) )/sum( IIF(DAYTYPE=1,1.00,0.00)) end 
		, REAL_SALE = case when isnull((sum(REAL_DISCOUNT)+sum(COST_DISCOUNT)),0)=0 then NULL
						else sum(REAL_DISCOUNT)/ (sum(REAL_DISCOUNT)+sum(COST_DISCOUNT)) end
		, CARD_SALE =  case when isnull((sum(REAL_DISCOUNT)+sum(COST_DISCOUNT)),0)=0 then NULL
						else sum(DISCOUNT)/ (sum(REAL_DISCOUNT)+sum(COST_DISCOUNT)) end
		, FREQ_SCORE = (count(distinct ID_CONTACT_DATA)*1.00)/ DATEDIFF(day,  dateadd(day,+1, dateadd(week,-6,@act_date)), @act_date)
		, CHECK_SCORE= IIF(isnull(count(distinct ID_CONTACT_DATA),0)=0 , NULL, (sum(COST_Discount) / count(ID_CHECK))/10000)
		, BUDGET_SCORE= isnull(sum(COST_Discount),0)/100000
		--, MAX_DATA = max(DATA)
into #all_p
from	 #for_basic_segment as b
		left join I_CHECKHEADER as a (nolock)
		on a.ID_COMPANY=b.ID_COMPANY
		and a.ID_ORGANIZATION=b.ID_ORGANIZATION
		and a.ID_CONTACT=b.ID_CONTACT
		and a.ID_COMPANY=@idc 
		and a.ID_CONTACT != 0
		and DATA between dateadd(day,+1, dateadd(week,-6,@act_date)) and @act_date
group by  b.ID_COMPANY
		, b.ID_ORGANIZATION
		, b.ID_CONTACT
		, SEGMENT
		, REFERENCE_DATE
option (recompile)

create clustered columnstore index cci_all_p on #all_p

-- Т2: #for_cg (вся история чеков × I_CONTACT_PRICE_SEGMENT × I_CONTACT) больше не строится: из него в таблицу шёл только
-- LTV = round(сумма чеков клиента в организации за всю историю / 100) * 100, а эта сумма уже есть в #first_purch
-- (COST_DISCOUNT_FP); остальные поля #for_cg читала локальная КГ, отключённая 11.10.2026


--	DECLARE @c int = 1
--DECLARE @idc int = (select ID_COMPANY from #c where RN=@c)


--declare @act_date date = (select max(DATA) from I_CHECKHEADER (nolock) where ID_COMPANY=@idc) 

--truncate table I_CVM_CONTACT


-- локальная контрольная группа (#local_cg) в процедуре не считается с 11.10.2026 (решение Елены): КГ выделяется при
-- формировании списка акции (портал «Сегменты», скрипты выборок). LTV — из #first_purch (Т2, 01.11.2026).

--darkstore 
drop table if exists  #main_shop

-- Т2: любимый магазин считается по 8 последним парам «магазин × день» клиента. Раньше нумеровались все пары всей истории,
-- и дважды (CTE t читался двумя ссылками). Теперь: пары последних 26 недель (#sd); историю дочитываем только клиентам, у которых
-- в 26 неделях меньше 8 пар (#few) — у остальных старые пары в 8 последних не попадают; номера пар — один раз (#t8).
-- Чеки без карты (клиент 0) не считаем — в таблицу они не попадают. Расчёт a, x, y — как был
declare @dms date = dateadd(week,-26,@act_date)

drop table if exists #sd, #few, #t8;

create table #sd (ID_CONTACT int, ID_SHOP int, ID_COMPANY int, DATA date, COST_DISCOUNT decimal(38,2)
	, index cci_sd clustered columnstore)

insert into #sd with (tablock)
select ID_CONTACT
	, ID_SHOP
	, ID_COMPANY
	, DATA
	, COST_DISCOUNT = sum(COST_DISCOUNT)
from I_CHECKHEADER (nolock)
where ID_COMPANY = @idc and ID_CONTACT != 0 and DATA > @dms
group by ID_CONTACT
	, ID_SHOP
	, ID_COMPANY
	, DATA
option (recompile)

select k.ID_CONTACT
into #few
from	(select ID_CONTACT from #hist group by ID_CONTACT) k
		left join (select ID_CONTACT, N = count(*) from #sd group by ID_CONTACT) s
		on s.ID_CONTACT = k.ID_CONTACT
where isnull(s.N, 0) < 8

create clustered columnstore index cci_few on #few

insert into #sd with (tablock)
select h.ID_CONTACT
	, h.ID_SHOP
	, h.ID_COMPANY
	, h.DATA
	, COST_DISCOUNT = sum(h.COST_DISCOUNT)
from I_CHECKHEADER h (nolock)
where h.ID_COMPANY = @idc and h.ID_CONTACT != 0 and (h.DATA <= @dms or h.DATA is null)
	and exists (select 1 from #few f where f.ID_CONTACT = h.ID_CONTACT)
group by h.ID_CONTACT
	, h.ID_SHOP
	, h.ID_COMPANY
	, h.DATA
option (recompile)

create table #t8 (ID_CONTACT int, ID_SHOP int, ID_COMPANY int, RN bigint, COST_DISCOUNT decimal(38,2)
	, index cci_t8 clustered columnstore)

insert into #t8 with (tablock)
select ID_CONTACT
	, ID_SHOP
	, ID_COMPANY
	, RN
	, COST_DISCOUNT
from (select ID_CONTACT
		, ID_SHOP
		, ID_COMPANY
		, RN= ROW_NUMBER() over (partition by ID_CONTACT, ID_COMPANY order  by DATA desc)
		, COST_DISCOUNT
	from #sd) q
where RN <= 8

;with t as
(
select	ID_CONTACT
		, ID_SHOP
		, ID_COMPANY
		, RN
		, COST_DISCOUNT
from #t8
), a as (
select ID_CONTACT
		, ID_COMPANY
		, COUNT_CHECK= count(RN)*1.00
		, COST_DISCOUNT=sum(COST_DISCOUNT)
from t
where RN<=8
group by ID_CONTACT
		, ID_COMPANY
), x as (
select a.ID_CONTACT
		, ID_SHOP
		, a.ID_COMPANY
		, SHARE_CHECK= (count(RN)*1.00)/a.COUNT_CHECK
		, SHARE_COST=sum(t.COST_DISCOUNT)/nullif(a.COST_DISCOUNT,0)
		, MIN_RN = min(RN)

from	t
		inner join a
		on a.ID_COMPANY=t.ID_COMPANY
		and a.ID_CONTACT=t.ID_CONTACT
where RN<=8
group by a.ID_CONTACT
		, ID_SHOP
		, a.ID_COMPANY
		, a.COUNT_CHECK
		, a.COST_DISCOUNT
--having (count(RN)*1.00)/a.COUNT_CHECK>=0.5 or sum(t.COST_DISCOUNT)/nullif(a.COST_DISCOUNT,0)>=0.5 
), y as (
select ID_CONTACT
		, ID_SHOP
		, ID_COMPANY
		, NEW_RN = ROW_NUMBER() over (partition by ID_CONTACT, ID_COMPANY order by SHARE_CHECK desc ,SHARE_COST desc, MIN_RN)		
from x 
)
select ID_CONTACT
		, ID_SHOP
		, ID_COMPANY
into #main_shop
from y
where NEW_RN=1
group by ID_CONTACT
		, ID_SHOP
		, ID_COMPANY


--and max(RN)

drop table if exists  #d_contact

select  ID_CONTACT
		, ID_DARKSTORE= isnull(ID_DARKSTORE,0)
		, a.ID_SHOP
		, a.ID_COMPANY
into #d_contact
from	#main_shop as a
		left join  I_SHOP_DARKSTORE as b (nolock)
		on a.ID_COMPANY=b.ID_COMPANY
		and a.ID_SHOP=b.ID_SHOP

create clustered columnstore index cci_d_contact on #d_contact

--drop table if exists  I_CVM_CONTACT 


----------------------------------------


-- Т2: любимые товары и категории. Строки чеков 6 недель читаются один раз и только по клиентам с выбором (#chk_fav),
-- категория совпадает равенством по каждому уровню (раньше — два прохода I_CHECK и соединение через OR). Как и было:
-- FAV_*_USE = 1 у организации, где в срок выбора (от даты выбора до следующего выбора того же) куплен выбранный товар /
-- товар выбранной категории; FAV_*_CHOICE = 1 у клиента с выбором за 6 недель
drop table if exists #fp_t2, #fc_t2, #fav_cli, #chk_fav;

select ID_CONTACT
	, DATA
	, ID_PRODUCT
	, next_date = isnull(lead(DATA) over(partition by ID_CONTACT, ID_PRODUCT order by DATA), dateadd(day, 2, @act_date))
into #fp_t2
from (select ID_CONTACT
		, DATA
		, ID_PRODUCT
	from fav_product (nolock)
	where DATA between @d6 and @act_date
	group by ID_CONTACT
		, DATA
		, ID_PRODUCT) t1

create clustered columnstore index cci_fp_t2 on #fp_t2

select ID_CONTACT
	, DATA
	, ID_CATEGORY_2
	, ID_CATEGORY_3
	, ID_CATEGORY_4
	, ID_CATEGORY_5
	, ID_PRODUCT
	, next_date = isnull(lead(DATA) over(partition by ID_CONTACT, ID_CATEGORY_2, ID_CATEGORY_3, ID_CATEGORY_4, ID_CATEGORY_5, ID_PRODUCT order by DATA), dateadd(day, 2, @act_date))
into #fc_t2
from (select ID_CONTACT
		, DATA
		, ID_CATEGORY_2
		, ID_CATEGORY_3
		, ID_CATEGORY_4
		, ID_CATEGORY_5
		, ID_PRODUCT
	from fav_CATEGORY (nolock)
	where DATA between @d6 and @act_date
	group by ID_CONTACT
		, DATA
		, ID_CATEGORY_2
		, ID_CATEGORY_3
		, ID_CATEGORY_4
		, ID_CATEGORY_5
		, ID_PRODUCT) t1

create clustered columnstore index cci_fc_t2 on #fc_t2

select ID_CONTACT
into #fav_cli
from #fp_t2
union
select ID_CONTACT
from #fc_t2

create clustered columnstore index cci_fav_cli on #fav_cli

create table #chk_fav (ID_CONTACT int, ID_ORGANIZATION int, ID_COMPANY int, DATA date, ID_PRODUCT bigint
	, ID_CATEGORY_2 int, ID_CATEGORY_3 int, ID_CATEGORY_4 int, ID_CATEGORY_5 int
	, index cci_chk_fav clustered columnstore)

insert into #chk_fav with (tablock)
select a.ID_CONTACT
	, a.ID_ORGANIZATION
	, a.ID_COMPANY
	, a.DATA
	, a.ID_PRODUCT
	, a.ID_CATEGORY_2
	, a.ID_CATEGORY_3
	, a.ID_CATEGORY_4
	, a.ID_CATEGORY_5
from I_CHECK a (nolock)
where a.ID_COMPANY = @idc and a.DATA between @d6 and @act_date
	and exists (select 1 from #fav_cli f where f.ID_CONTACT = a.ID_CONTACT)
option (recompile)

drop table if exists #fav_prod;

select p.ID_CONTACT
	, c.ID_ORGANIZATION
	, c.ID_COMPANY
	, flg_use = 1
into #fav_prod
from #fp_t2 p
	join #chk_fav c
		on c.ID_CONTACT = p.ID_CONTACT
		and c.ID_PRODUCT = p.ID_PRODUCT
		and c.DATA >= p.DATA
		and c.DATA < p.next_date
group by p.ID_CONTACT
	, c.ID_ORGANIZATION
	, c.ID_COMPANY

create clustered columnstore index cci_fav_prod on #fav_prod


drop table if exists #fav_prod_choice;

select ID_CONTACT
into #fav_prod_choice
from #fp_t2
group by ID_CONTACT

create clustered columnstore index cci_fav_prod_choice on #fav_prod_choice





drop table if exists #fav_cat;

select ID_CONTACT
	, ID_ORGANIZATION
	, ID_COMPANY
	, flg_use = 1
into #fav_cat
from (
	-- ревью Т2: полусоединение (exists) вместо соединения — у одного уровня категории у клиента бывает несколько выборов
	-- с пересекающимися сроками, соединение размножало строки чеков до UNION; набор (клиент, организация) тот же
	select c.ID_CONTACT, c.ID_ORGANIZATION, c.ID_COMPANY
	from #chk_fav c
	where exists (select 1 from #fc_t2 f where f.ID_CONTACT = c.ID_CONTACT and c.DATA >= f.DATA and c.DATA < f.next_date
			and f.ID_PRODUCT = c.ID_PRODUCT)
	union
	select c.ID_CONTACT, c.ID_ORGANIZATION, c.ID_COMPANY
	from #chk_fav c
	where exists (select 1 from #fc_t2 f where f.ID_CONTACT = c.ID_CONTACT and c.DATA >= f.DATA and c.DATA < f.next_date
			and f.ID_CATEGORY_2 = c.ID_CATEGORY_2)
	union
	select c.ID_CONTACT, c.ID_ORGANIZATION, c.ID_COMPANY
	from #chk_fav c
	where exists (select 1 from #fc_t2 f where f.ID_CONTACT = c.ID_CONTACT and c.DATA >= f.DATA and c.DATA < f.next_date
			and f.ID_CATEGORY_3 = c.ID_CATEGORY_3)
	union
	select c.ID_CONTACT, c.ID_ORGANIZATION, c.ID_COMPANY
	from #chk_fav c
	where exists (select 1 from #fc_t2 f where f.ID_CONTACT = c.ID_CONTACT and c.DATA >= f.DATA and c.DATA < f.next_date
			and f.ID_CATEGORY_4 = c.ID_CATEGORY_4)
	union
	select c.ID_CONTACT, c.ID_ORGANIZATION, c.ID_COMPANY
	from #chk_fav c
	where exists (select 1 from #fc_t2 f where f.ID_CONTACT = c.ID_CONTACT and c.DATA >= f.DATA and c.DATA < f.next_date
			and f.ID_CATEGORY_5 = c.ID_CATEGORY_5)
) u

create clustered columnstore index cci_fav_cat on #fav_cat



drop table if exists #fav_cat_choice;

select ID_CONTACT
into #fav_cat_choice
from #fc_t2
group by ID_CONTACT

create clustered columnstore index cci_fav_cat_choice on #fav_cat_choice




drop table if exists #camp;

-- кампании Manzana для флагов участия за 6 недель (окно — как было); три независимых флага:
--   IS_PP      — ПП: только 5ПП «Персональные предложения» (6ПП — информирование об идущих акциях, награды нет);
--   IS_CVM     — наши CVM-акции (номера из Google): I_PROMO.ID_CAMPAIGN; кампании, название которых начинается с номера
--                акции и «_» или «.» (волны «101410_…», в 2024–2025 — «100993. …»); кампании CVM 2024–2025 из I_PROMO_all
--                с названием «10xxxx.» — тот же набор, что в витрине кластеров (#lp в процедуре clusters);
--   IS_LOYALTY — акции лояльности (наклейки и т. п.): список dbo.I_LOYALTY_PROMO
select ID_CAMPAIGN
	, IS_PP = max(IS_PP)
	, IS_CVM = max(IS_CVM)
	, IS_LOYALTY = max(IS_LOYALTY)
into #camp
from (
	select ID_CAMPAIGN, IS_PP = 1, IS_CVM = 0, IS_LOYALTY = 0
	from I_PROMO (nolock)
	where TYPE_PROMO = N'5ПП'
		and ID_COMPANY = @idc
	union all
	select ID_CAMPAIGN, 0, 1, 0
	from I_PROMO (nolock)
	where TYPE_PROMO = 'CVM'
		and ID_COMPANY = @idc
	union all
	select c.ID_CAMPAIGN, 0, 1, 0
	from I_CAMPAIGN (nolock) c
		join I_PROMO (nolock) p
			on p.TYPE_PROMO = 'CVM'
			and p.ID_COMPANY = @idc
			and p.ID_PROMO between 100000 and 999999	-- только шестизначные номера: короткий номер совпал бы с чужой кампанией
			and c.CAMPAIGN_NAME like cast(p.ID_PROMO as nvarchar(12)) + N'[_.]%'	-- «_» в [] буквальный: без скобок '101428_%' совпал бы с «1014280…»
	where c.ID_COMPANY = @idc
	union all
	select a.ID_PROMO, 0, 1, 0	-- I_PROMO_all: ID_PROMO = ID_CAMPAIGN; старые акции, номеров которых нет в I_PROMO
	from I_PROMO_all (nolock) a
	where a.TYPE_PROMO = 'CVM'
		and a.ID_COMPANY = @idc
		and a.PROMO_NAME like '10[0-9][0-9][0-9][0-9].%'	-- тип CVM ставится по названию («10» + 7-й символ «.»), здесь строже: шесть цифр и точка
	union all
	select ID_CAMPAIGN, 0, 0, 1
	from I_LOYALTY_PROMO (nolock)
) x
where ID_CAMPAIGN is not null
group by ID_CAMPAIGN;

-- диапазон ID_CHECK чеков окна (те же условия, что у заголовков ниже): в I_CHECK_RULE нет даты, без отсечения по ID_CHECK
-- она читается целиком (1,75 млрд строк). Результат тот же: соединение с заголовками окна и так оставляет только эти чеки;
-- чеков нет — NULL, #rule_use пустая, как и без отсечения (так же отсекает шаг 8в витрины кластеров)
declare @chk_min bigint, @chk_max bigint;

select @chk_min = min(ID_CHECK)
	, @chk_max = max(ID_CHECK)
from I_CHECKHEADER (nolock)
where DATA between dateadd(day,+1, dateadd(week,-6,@act_date)) and @act_date
	and ID_COMPANY = @idc
	and ID_CONTACT != 0
	and ID_ORGANIZATION <> 2;

drop table if exists #rule_use;

-- ОДИН проход I_CHECKHEADER × I_CHECK_RULE за 6 недель на ПП, CVM и лояльность (было два прохода: отдельно ПП и CVM)
select h.ID_CONTACT
	, h.ID_ORGANIZATION
	, h.ID_COMPANY
	, PP_FLG = max(k.IS_PP)
	, CVM_FLG = max(k.IS_CVM)
	, LOY_FLG = max(k.IS_LOYALTY)
into #rule_use
from I_CHECKHEADER (nolock) h
	join I_CHECK_RULE (nolock) r
		on h.ID_CHECK = r.ID_CHECK
		and h.ID_COMPANY = r.ID_COMPANY
	join #camp k
		on r.ID_CAMPAIGN = k.ID_CAMPAIGN
where h.DATA between dateadd(day,+1, dateadd(week,-6,@act_date)) and @act_date
	and h.ID_COMPANY = @idc
	and r.ID_COMPANY = @idc
	and h.ID_CONTACT != 0		-- контакт 0 в таблицу не попадает (inner join #first_purch финальной вставки)
	and h.ID_ORGANIZATION <> 2	-- организация 2 в таблицу не попадает (where d.ID_ORGANIZATION<>2)
	and r.ID_CHECK between @chk_min and @chk_max	-- отсечение I_CHECK_RULE по диапазону чеков окна (см. выше)
group by h.ID_CONTACT
	, h.ID_ORGANIZATION
	, h.ID_COMPANY
option (recompile);	-- ревью Т2 (CC-8): окно и диапазон ID_CHECK заданы переменными

create clustered columnstore index CCI_rule_use on #rule_use;

drop table if exists #push;

-- реакция на пуши за те же 6 недель: I_PROMO_DELIVERY_REPORT (наполняет communication_processing), ID_CHANNEL 1 = Push.
-- У сообщения в таблице один, последний статус: «Отправлено», «Доставлено», «Переход по ссылке» (доставлено и перешли —
-- строки «Доставлено» у такого сообщения нет), «Ошибка». Строки бывают задвоены (30.09.2026 — все), поэтому
-- сообщение = клиент × время отправки SEND_DT.
-- % доставки = доставленные («Доставлено» + «Переход по ссылке») / отправленные (+ «Отправлено»); «Ошибка» — вне
-- знаменателя, как в отчётах эффекта CVM; % переходов = переходы / доставленные; проценты целые 0–100.
-- По клиенту (организации в источнике нет — значение одно для строк организаций 1 и 3). Пушей за окно не было или только
-- «Ошибка» — оба процента NULL, переходов 0 (isnull в финальной вставке); отправлены, но не доставлены — % доставки 0,
-- % переходов NULL
select ID_COMPANY
	, ID_CONTACT
	, PUSH_DELIV_RATE = cast(round(100.0 * sum(IS_DELIV) / count(*), 0) as int)
	, PUSH_CLICK_RATE = cast(round(100.0 * sum(IS_CLICK) / nullif(sum(IS_DELIV), 0), 0) as int)
	, PUSH_CLICK_COUNT = sum(IS_CLICK)
into #push
from (
	select ID_COMPANY
		, ID_CONTACT
		, SEND_DT
		, IS_DELIV = max(iif(STATUS in (N'Доставлено', N'Переход по ссылке'), 1, 0))
		, IS_CLICK = max(iif(STATUS = N'Переход по ссылке', 1, 0))
	from I_PROMO_DELIVERY_REPORT (nolock)
	where ID_COMPANY = @idc
		and ID_CHANNEL = 1
		and STATUS in (N'Отправлено', N'Доставлено', N'Переход по ссылке')
		and SEND_DT >= cast(dateadd(day,+1, dateadd(week,-6,@act_date)) as datetime)
		and SEND_DT <  cast(dateadd(day,+1, @act_date) as datetime)
	group by ID_COMPANY
		, ID_CONTACT
		, SEND_DT
) m
group by ID_COMPANY
	, ID_CONTACT;

create clustered columnstore index CCI_push on #push;



---------------------------------------------------------------------


--all cvm

-- очистка прямо перед вставкой (раньше — в начале процедуры): пока идёт расчёт, в таблице данные прошлого прогона
-- Т2: WITH (TABLOCK) — вставка в пустую кучу параллельно и с минимальным протоколированием (база SIMPLE); строки те же
if @c = 1
	truncate table I_CVM_CONTACT

INSERT INTO I_CVM_CONTACT WITH (TABLOCK)
select d.[ID_COMPANY]
      ,d.[ID_ORGANIZATION]
      ,d.[ID_CONTACT]
      ,d.[SEGMENT]
	  , f.TOKENS
	  , f.HAS_PUSH
	  , f.HAS_EMAIL
      ,d.[REFERENCE_DATE]
      ,a.[ID_SALE_CHANNEL]
      ,b.[SALE_CHANNEL]
	  , [PRICE_INDEX]= c.PRICE_INDEX 
	  , ID_PRICE_SEGMENT= NULL
	  , [PRICE_SEGMENT] = null 
	  , LOYALTY_INDEX =c.[PENETRATION_CHECK]
	  , ID_LOYALTY_SEGMENT = null
	  , LOYALTY_SEGMENT = null 
	  , FIRST_DATA = MIN_DATA
	  , LAST_DATA
	  , e.LAST_YM
	  , FIRST_YM
	  , d.VISITS
		, d.FREQ_SCORE
		, d.CHECKS 
		, d.VISITS_PER_WEEK 
		, d.AVG_VISIT
		, d.CHECK_SCORE
		, d.AVG_CHECK
		, d.BUDGET 
		, d.BUDGET_SCORE
		, d.AVG_COST_SKU
		, d.AVG_SKU
		, d.SHARE_WEEKEND 
		, d.SHARE_WORKDAY 
		, d.TIME_PURCHASE_WEEKEND
		, d.TIME_PURCHASE_WORKDAY
		, d.REAL_SALE 
		, d.CARD_SALE
		, d.MAX_CHECK
		, POTENCIAL = null 
		, MAIN_TARGET = null 
		, CHURN_RATE = null 
		, PUSH_DELIV_RATE = pu.PUSH_DELIV_RATE
		, PUSH_CLICK_RATE = pu.PUSH_CLICK_RATE
		, ID_RFM_CLUSTER = null 
		, RFM_CLUSTER = null 
		, ID_CLUSTER = null 
		, CLUSTER = null 
		, CONTROL_GROUP = NULL	-- локальная КГ в процедуре не считается с 11.10.2026; колонка остаётся (вставка без списка колонок)
		, LTV = round(e.COST_DISCOUNT_FP/100,0)*100	-- Т2: как было в #for_cg — round(sum(COST_DISCOUNT)/100,0)*100 по всей истории
		, LOVE_STORE = x.ID_SHOP
		, ID_DARKSTORE
		, AFFINITIVE_SCORE=null
		, FAV_PRODUCT_CHOICE = iif(fpc.ID_CONTACT is null, 0, 1)
		, FAV_CATEGORY_CHOICE = iif(fcc.ID_CONTACT is null, 0, 1)
		, FAV_PRODUCT_USE = isnull(fp.flg_use, 0)
		, FAV_CATEGORY_USE = isnull(fc.flg_use, 0)
		, PP_USE = isnull(ru.PP_FLG, 0)
		, CVM_PARTICIPANT = isnull(ru.CVM_FLG, 0)
		, LOYALTY_PROMO_PARTICIPANT = isnull(ru.LOY_FLG, 0)	-- колонка 59 (INSERT без списка колонок — порядок важен)
		, PUSH_CLICK_COUNT = isnull(pu.PUSH_CLICK_COUNT, 0)	-- колонка 60
--INTO I_CVM_CONTACT 
from	#all_p as d
		inner join #m as a
		on a.ID_COMPANY=d.ID_COMPANY
		and a.ID_CONTACT=d.ID_CONTACT
		inner join I_CVM_CHANNEL as b (nolock)
		on a.ID_SALE_CHANNEL=b.ID_SALE_CHANNEL
		left join I_CONTACT_PRICE_SEGMENT as c (nolock)
		on d.ID_COMPANY=c.ID_COMPANY
		and d.ID_ORGANIZATION=c.ID_ORGANIZATION
		and d.ID_CONTACT=c.ID_CONTACT
		inner join #first_purch as e 
		on d.ID_COMPANY=e.ID_COMPANY
		and  d.ID_ORGANIZATION=e.ID_ORGANIZATION
		and d.ID_CONTACT=e.ID_CONTACT
		left join I_CONTACT as f (nolocK)
		on d.ID_COMPANY=f.ID_COMPANY
		and d.ID_CONTACT=f.ID_CONTACT
		inner join #d_contact as x
		on d.ID_COMPANY=x.ID_COMPANY
		and x.ID_CONTACT=d.ID_CONTACT
		left join #fav_cat_choice fcc
		on d.ID_CONTACT = fcc.ID_CONTACT
		left join #fav_prod_choice fpc
		on d.ID_CONTACT = fpc.ID_CONTACT
		left join #fav_prod fp
		on d.ID_CONTACT = fp.ID_CONTACT
		and d.ID_ORGANIZATION = fp.ID_ORGANIZATION
		and d.ID_COMPANY = fp.ID_COMPANY
		left join #fav_cat fc
		on d.ID_CONTACT = fc.ID_CONTACT
		and d.ID_ORGANIZATION = fc.ID_ORGANIZATION
		and d.ID_COMPANY = fc.ID_COMPANY
		left join #rule_use ru
		on d.ID_CONTACT = ru.ID_CONTACT
		and d.ID_ORGANIZATION = ru.ID_ORGANIZATION
		and d.ID_COMPANY = ru.ID_COMPANY
		left join #push pu
		on d.ID_CONTACT = pu.ID_CONTACT
		and d.ID_COMPANY = pu.ID_COMPANY
where d.ID_ORGANIZATION<>2
		
set @c=@c+1
set @idc = (select ID_COMPANY from #c where RN=@c)

END

--ALTER TABLE I_CVM_CONTACT
--ADD AFFINITIVE_SCORE float

--ALTER TABLE I_CVM_CONTACT
--ADD LOYALTY_PROMO_PARTICIPANT int NULL, PUSH_CLICK_COUNT int NULL

