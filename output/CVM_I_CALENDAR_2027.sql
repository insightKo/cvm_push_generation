-- CVM · dbo.I_CALENDAR: 2027 год (365 строк) по производственному календарю РФ
-- Елена 31.10.2026: «обнови ай календар на 2027 год, учти все официальные праздничные дни».
-- Источник: ТК РФ ст. 112 и постановление Правительства РФ от 17.09.2026 № 1187 «О переносе выходных дней в 2027 году»;
--   сверено с производственным календарём КонсультантПлюс на 2027 год: 247 рабочих дней, 118 выходных и праздничных.
-- Нерабочие праздники (ст. 112): 1–8 января, 23 февраля, 8 марта, 1 мая, 9 мая, 12 июня, 4 ноября.
-- Переносы по постановлению: сб 02.01 → пт 05.11; вс 03.01 → пт 31.12; сб 20.02 → пн 22.02 (20.02 — рабочая суббота).
-- Переносы по ст. 112 (праздник в выходной): сб 01.05 → пн 03.05; вс 09.05 → пн 10.05; сб 12.06 → пн 14.06.
-- DAYTYPE: 1 — рабочий (в т. ч. сб 20.02), 2 — суббота и воскресенье, 3 — праздник или перенесённый выходной
--   (переносы = 3, как в строках 2026: 09.01, 09.03, 11.05, 31.12).
-- Кто читает: обработка чеков (check_checkheader_processing → QUARTER, WEEKNUMBER, YEAR, DAYOFWEEK, DAYTYPE, YEAR_MONTH,
--   CALENDAR_WEEKNUMBER в I_CHECK_DELTA / I_CHECKHEADER_DELTA, left join — без строк 2027 у чеков с 01.01.2027 поля пустые),
--   assort_week (недели по WEEKNUMBER и DAYOFWEEK = 7, первая же неделя 28.12.2026–03.01.2027), CVM_CONTACT, clusters,
--   AFFINITIVE_MODEL, segment_trend, for_model_churn (доля выходных по DAYTYPE), отчёты по YEAR_MONTH.
-- Остальные колонки — по правилам строк 2025–2026 (заведены 11.2024); тот же текст генерации, запущенный на 2026 год,
--   совпадает с таблицей по всем колонкам, кроме DAYTYPE и revision_date:
--   WEEKNUMBER — сквозная неделя пн–вс, продолжает последнюю строку 2026 (28.12.2026–03.01.2027 = 1043);
--   DAYS_IN_WEEK — сколько дней этой недели в том же месяце; CALENDAR_WEEKNUMBER — неделя года, первая — с 1 января;
--   SEASON_NUMBER — зима по году декабря (янв–фев 2027 = 20261, дек 2027 = 20271);
--   WEEK_FROM_ACT_DAY = WEEKNUMBER + 16, MONTH_FROM_ACT_DAY = −1, неделя ACT — чт–ср, «месяц» ACT — 30 дней от 1 января.
--
-- Скрипт для задания CVM_API_nightly (обменник), идемпотентный: 2027 уже есть целиком и сходится — выход без изменений;
--   есть частично, стык с 2026 не тот или проверка не сошлась — ошибка 16 без изменений в базе.
--   Вставка и сверка — в одной транзакции.

SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET ANSI_WARNINGS ON
SET ARITHABORT ON
SET NOCOUNT ON
SET XACT_ABORT ON
GO

USE mci_model
GO

declare @y int = 2027
declare @d0 date = datefromparts(@y, 1, 1), @d1 date = datefromparts(@y + 1, 1, 1)
declare @wk_y date = dateadd(day, -(datediff(day, cast('19000101' as date), @d0) % 7), @d0)   -- понедельник недели 1 января

-- праздники и переносы 2027; остальные дни: сб, вс → 2, пн–пт → 1
declare @h table ([DATE] date not null primary key, DAYTYPE int not null)
insert @h ([DATE], DAYTYPE)
values
	  ('2027-01-01', 3), ('2027-01-02', 3), ('2027-01-03', 3), ('2027-01-04', 3)
	, ('2027-01-05', 3), ('2027-01-06', 3), ('2027-01-07', 3), ('2027-01-08', 3)   -- новогодние праздники и Рождество
	, ('2027-02-20', 1)                                                            -- рабочая суббота (выходной перенесён на 22.02)
	, ('2027-02-22', 3)                                                            -- перенос с сб 20.02
	, ('2027-02-23', 3)                                                            -- День защитника Отечества
	, ('2027-03-08', 3)                                                            -- Международный женский день
	, ('2027-05-01', 3), ('2027-05-03', 3)                                         -- Праздник Весны и Труда (сб) и перенос
	, ('2027-05-09', 3), ('2027-05-10', 3)                                         -- День Победы (вс) и перенос
	, ('2027-06-12', 3), ('2027-06-14', 3)                                         -- День России (сб) и перенос
	, ('2027-11-04', 3), ('2027-11-05', 3)                                         -- День народного единства и перенос с сб 02.01
	, ('2027-12-31', 3)                                                            -- перенос с вс 03.01

-- рабочих дней по месяцам (производственный календарь 2027)
declare @wd table ([MONTH] int not null primary key, N int not null)
insert @wd ([MONTH], N)
values (1, 15), (2, 19), (3, 22), (4, 22), (5, 19), (6, 21), (7, 22), (8, 22), (9, 22), (10, 21), (11, 20), (12, 22)

-- 1. уже заведено?
declare @have int = (select count(*) from dbo.I_CALENDAR (nolock) where [DATE] >= @d0 and [DATE] < @d1)
if @have > 0
begin
	declare @have_wd int = (select count(*) from dbo.I_CALENDAR (nolock) where [DATE] >= @d0 and [DATE] < @d1 and DAYTYPE = 1)
	if @have = 365 and @have_wd = 247
	begin
		print 'I_CALENDAR 2027: already loaded (365 rows, 247 work days) - nothing to do'
		return
	end
	raiserror(N'I_CALENDAR 2027: в таблице уже %d строк 2027 года, рабочих %d (ожидается 365 и 247) — стоп, ничего не меняю', 16, 1, @have, @have_wd)
	return
end

-- 2. стык с последней строкой 2026
declare @wn0 int, @wk0 date, @wfa_off int, @mfa int
select @wn0 = WEEKNUMBER, @wk0 = FIRST_DAY_IN_WEEK, @wfa_off = WEEK_FROM_ACT_DAY - WEEKNUMBER, @mfa = MONTH_FROM_ACT_DAY
from dbo.I_CALENDAR (nolock)
where [DATE] = dateadd(day, -1, @d0)

declare @max_date date = (select max([DATE]) from dbo.I_CALENDAR (nolock))
if @wn0 is null or @wn0 <> 1043 or @wk0 <> '2026-12-28' or @wfa_off <> 16 or @mfa <> -1 or @max_date <> '2026-12-31'
begin
	raiserror(N'I_CALENDAR 2027: стык с 31.12.2026 не тот (WEEKNUMBER %d, ожидается 1043; последняя дата должна быть 31.12.2026) — стоп', 16, 1, @wn0)
	return
end

-- <gen> строки года @y в #cal (нужны @y, @d0, @d1, @wk_y, @wn0, @wk0, @wfa_off, @mfa, @h)
if object_id('tempdb..#cal') is not null drop table #cal

;with d10 as (select v from (values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)) t(v))
, n as (select a.v + 10 * b.v + 100 * c.v as i from d10 a cross join d10 b cross join d10 c)
select
	  d.[DATE]
	, [DAY] = day(d.[DATE])
	, [MONTH] = month(d.[DATE])
	, [QUARTER] = (month(d.[DATE]) + 2) / 3
	, WEEKNUMBER = k.wn
	, DAYS_IN_WEEK = datediff(day, iif(b.wk_first > b.m_first, b.wk_first, b.m_first), iif(b.wk_last < b.m_last, b.wk_last, b.m_last)) + 1
	, DAYSINMONTH = day(b.m_last)
	, [YEAR] = year(d.[DATE])
	, [DAYOFWEEK] = w.dow
	, DAYTYPE = isnull(h.DAYTYPE, iif(w.dow >= 6, 2, 1))
	, SEASON = s.season
	, SEASON_NUMBER = (year(d.[DATE]) - iif(month(d.[DATE]) <= 2, 1, 0)) * 10 + s.season
	, YEAR_MONTH = year(d.[DATE]) * 100 + month(d.[DATE])
	, CALENDAR_WEEKNUMBER = datediff(day, @wk_y, b.wk_first) / 7 + 1
	, ID_DATE = year(d.[DATE]) * 10000 + month(d.[DATE]) * 100 + day(d.[DATE])
	, FIRST_DAY_IN_MONTH = b.m_first
	, LAST_DAY_IN_MONTH = b.m_last
	, FIRST_DAY_IN_WEEK = b.wk_first
	, LAST_DAY_IN_WEEK = b.wk_last
	, WEEK_FROM_ACT_DAY = k.wn + @wfa_off
	, MONTH_FROM_ACT_DAY = @mfa
	, FIRST_DAY_WEEK_ACT = dateadd(day, -((w.dow + 3) % 7), d.[DATE])     -- неделя ACT: чт–ср
	, LAST_DAY_WEEK_ACT = dateadd(day, 6 - (w.dow + 3) % 7, d.[DATE])
	, FIRST_DAY_MONTH_ACT = dateadd(day, n.i / 30 * 30, @d0)               -- «месяц» ACT: 30 дней от 1 января
	, LAST_DAY_MONTH_ACT = dateadd(day, n.i / 30 * 30 + 29, @d0)
	, SEASON_NAME = choose(s.season, N'Зима', N'Весна', N'Лето', N'Осень')
	, SEASON_FASHION_NAME = iif(s.season in (1, 4), N'Осень-Зима', N'Весна-Лето')
	, DAYOFWEEK_NAME = choose(w.dow, N'Пн', N'Вт', N'Ср', N'Чт', N'Пт', N'Сб', N'Вс')
	, NUMBER_QUARTER = year(d.[DATE]) * 10 + (month(d.[DATE]) + 2) / 3
into #cal
from n
cross apply (select dateadd(day, n.i, @d0) as [DATE]) d
cross apply (select datediff(day, cast('19000101' as date), d.[DATE]) % 7 + 1 as dow) w   -- 1 = пн … 7 = вс, не зависит от DATEFIRST
cross apply (select dateadd(day, 1 - w.dow, d.[DATE]) as wk_first, dateadd(day, 7 - w.dow, d.[DATE]) as wk_last
	, datefromparts(year(d.[DATE]), month(d.[DATE]), 1) as m_first, eomonth(d.[DATE]) as m_last) b
cross apply (select @wn0 + datediff(day, @wk0, b.wk_first) / 7 as wn) k
cross apply (select case when month(d.[DATE]) in (12, 1, 2) then 1 when month(d.[DATE]) <= 5 then 2
	when month(d.[DATE]) <= 8 then 3 else 4 end as season) s
left join @h h on h.[DATE] = d.[DATE]
where n.i < datediff(day, @d0, @d1)
-- </gen>

-- 3. сверка до записи
declare @n int, @nd int, @n1 int, @n2 int, @n3 int, @wn_min int, @wn_max int, @bad_month int
select @n = count(*), @nd = count(distinct [DATE])
	, @n1 = sum(iif(DAYTYPE = 1, 1, 0)), @n2 = sum(iif(DAYTYPE = 2, 1, 0)), @n3 = sum(iif(DAYTYPE = 3, 1, 0))
	, @wn_min = min(WEEKNUMBER), @wn_max = max(WEEKNUMBER)
from #cal

select @bad_month = count(*)
from @wd e
left join (select [MONTH], count(*) as N from #cal where DAYTYPE = 1 group by [MONTH]) f on f.[MONTH] = e.[MONTH]
where isnull(f.N, 0) <> e.N

if @n <> 365 or @nd <> 365 or @n1 <> 247 or @n2 <> 98 or @n3 <> 20 or @wn_min <> 1043 or @wn_max <> 1095 or @bad_month <> 0
	or (select count(*) from @h h left join #cal c on c.[DATE] = h.[DATE] where c.[DATE] is null) <> 0
begin
	raiserror(N'I_CALENDAR 2027: сверка не сошлась (строк %d, рабочих %d, выходных %d, праздников %d, месяцев с расхождением %d) — стоп, ничего не меняю', 16, 1, @n, @n1, @n2, @n3, @bad_month)
	return
end

-- 4. запись (блокировку таблицы ждём не дольше 5 с: занята ночным заданием — ошибка 1222, откат, раннер повторит через 10 мин)
SET LOCK_TIMEOUT 5000
begin tran

if exists (select 1 from dbo.I_CALENDAR with (tablockx, holdlock) where [DATE] >= @d0 and [DATE] < @d1)
begin
	rollback
	raiserror(N'I_CALENDAR 2027: строки 2027 появились во время работы скрипта — стоп, ничего не меняю', 16, 1)
	return
end

insert dbo.I_CALENDAR ([DATE], [DAY], [MONTH], [QUARTER], WEEKNUMBER, DAYS_IN_WEEK, DAYSINMONTH, [YEAR], [DAYOFWEEK], DAYTYPE
	, SEASON, SEASON_NUMBER, YEAR_MONTH, CALENDAR_WEEKNUMBER, ID_DATE, FIRST_DAY_IN_MONTH, LAST_DAY_IN_MONTH, FIRST_DAY_IN_WEEK
	, LAST_DAY_IN_WEEK, WEEK_FROM_ACT_DAY, MONTH_FROM_ACT_DAY, FIRST_DAY_WEEK_ACT, LAST_DAY_WEEK_ACT, FIRST_DAY_MONTH_ACT
	, LAST_DAY_MONTH_ACT, SEASON_NAME, SEASON_FASHION_NAME, DAYOFWEEK_NAME, revision_date, NUMBER_QUARTER)
select [DATE], [DAY], [MONTH], [QUARTER], WEEKNUMBER, DAYS_IN_WEEK, DAYSINMONTH, [YEAR], [DAYOFWEEK], DAYTYPE
	, SEASON, SEASON_NUMBER, YEAR_MONTH, CALENDAR_WEEKNUMBER, ID_DATE, FIRST_DAY_IN_MONTH, LAST_DAY_IN_MONTH, FIRST_DAY_IN_WEEK
	, LAST_DAY_IN_WEEK, WEEK_FROM_ACT_DAY, MONTH_FROM_ACT_DAY, FIRST_DAY_WEEK_ACT, LAST_DAY_WEEK_ACT, FIRST_DAY_MONTH_ACT
	, LAST_DAY_MONTH_ACT, SEASON_NAME, SEASON_FASHION_NAME, DAYOFWEEK_NAME, getdate(), NUMBER_QUARTER
from #cal
declare @ins int = @@rowcount

if @ins <> 365
	or (select count(*) from dbo.I_CALENDAR where [DATE] >= @d0 and [DATE] < @d1 and DAYTYPE = 1) <> 247
	or (select count([DATE]) - count(distinct [DATE]) from dbo.I_CALENDAR where [DATE] >= @d0 and [DATE] < @d1) <> 0
begin
	rollback
	raiserror(N'I_CALENDAR 2027: после вставки сверка не сошлась (вставлено %d) — откат', 16, 1, @ins)
	return
end

commit

print 'I_CALENDAR 2027: inserted 365 rows (247 work, 98 weekend, 20 holiday), weeks 1043-1095'
drop table #cal
GO
