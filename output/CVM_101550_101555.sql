-- CVM · ноябрь 2026 · общие акции на сегменты жизненного цикла (файл Елены «ноябрь общие сегменты», ОК 01.11.2026):
--   101550 «Дарим 100р. монетами на выходные»  — Активные и Активные LFL, ЦИ высокий и средний, до 4 визитов за 6 недель
--                                                 или нет покупок 3 последние недели;
--   101551 «Дарим 50р. монетами на выходные»   — Активные и Активные LFL, ЦИ высокий и средний, остальные;
--   101552 «Вернём 150р. монетами с покупки от 1000р.» (пересборка; список v1 удалён) — Активные и Активные LFL, ЦИ низкий,
--                                                 средний чек за 6 недель до 750р.; имя для ML «101552_Вернём 150р монетами брать его»;
--   101553 «Дарим 50р. монетами на выходные»   — Активные и Активные LFL, ЦИ низкий, средний чек больше 750р.;
--   101554 «50% кешбэк на категории по выходным» — Случайные;
--   101555 «Вернём 200р. монетами с покупки от 700р.» — Отток, ЦИ высокий и средний.
-- Серии по выходным (101550_1–_3 и т. д. в Google) — одна шапка и один список на весь ноябрь, как 101426.
-- ЦИ: старый PRICE_SEGMENT MCI неверен (границы 0,5 и 0,625 от размаха индекса — 98 % Активных «низкие»). Низкий ЦИ = нижняя
--   треть клиентов офлайна (I_CVM_CONTACT SEGMENT 1–6, орг. 1) по PRICE_INDEX I_CONTACT_PRICE_SEGMENT, граница считается
--   в скрипте (замер 01.11: 0,2614); высокий и средний = выше границы.
-- Все аудитории — организация 1, ID_SALE_CHANNEL 1 офлайн и 2 омни; сегменты не пересекаются (клиент — ровно в одной акции,
--   проверяется). Замер 01.11 (всего / на отправку до КГ): 101550 1 744 190 / 896 928; 101551 1 821 414 / 1 013 479;
--   101552 1 254 890 / 603 066; 101553 576 221 / 318 638; 101554 2 091 641 / 908 094; 101555 1 276 500 / 617 514.
-- Стандартная сборка: + I_CONTACT (HAS_PUSH = 1, TOKENS = 1) минус I_GLOBAL_CG; КГ по каждой акции и части:
--   SEGMENT 2–3 — 5% (RN/20) по BUDGET, SEGMENT 4–5 — 10% (RN/10) по LTV.
-- Механики (справочник → MCI): 101550/101551/101553 начисление монет с активацией (15); 101552, 101555 кешбэк монетами за
--   покупку (9); 101554 кешбэк % на категорию с активацией (16). LOAD_TO_ML = 0 (акции в Manzana Online), LOAD_TO_MC = 1 (PUSH).
--   Период «после»: финиш + 1 … позднее из (сгорание монет, финиш + 4).
--
-- Скрипт для обменника (CVM_API_nightly): шапки — только отсутствующие; списки уже есть хотя бы у одной акции — сборка
-- пропускается; прочитано меньше 7 млн клиентов аудиторий (неполное чтение) — ошибка 16, раннер повторит.

SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET ANSI_WARNINGS ON
SET ARITHABORT ON
SET NOCOUNT ON
SET XACT_ABORT ON		-- любая ошибка выполнения обрывает пакет: после сбоя до INSERT в I_PROMO_OFFER дело не дойдёт
GO

USE mci_model
GO


-- ==================================================================================
-- 1. ШАПКИ В I_PROMO (заводятся только отсутствующие) + новое имя для ML у 101552
-- ==================================================================================

if cast(getdate() as date) > '2026-11-22'
begin
	raiserror(N'101550–101555: акции прошли — шапки не заводятся, файл можно убрать из папки', 10, 1)
	return
end

insert into I_PROMO (START_DATE_FROM, FINISH_DATE_FROM, START_DATE, FINISH_DATE, ID_PROMO, PROMO_NAME, START_DATE_AFTER, FINISH_DATE_AFTER, CHANNEL, SEGMENT, ID_SEGMENT, CORE_MESSAGE, MECHANICS, ID_MECHANICS, ID_CHANNEL, CATEGORY_FOR_CLIENT, ID_CAMPAIGN, TYPE_PROMO, ID_COMPANY, PROMO_NAME_ML)
select dateadd(week, -52*3, v.D), dateadd(day, -1, v.D), v.D, v.FD, v.ID_PROMO, v.PROMO_NAME, dateadd(day, +1, v.FD), v.AFTER_TO
	 , N'PUSH', v.SEG, v.ID_SEG, null, (select MECHANICS from I_MECHANICS as m (nolock) where m.ID_MECHANICS = v.ID_MECH), v.ID_MECH, 1, null, null, N'CVM', 1, v.PROMO_NAME_ML
from (values
	  (101550, N'Дарим 100р. монетами на выходные', N'101550_Дарим 100р монетами выходные', cast('2026-11-06' as date), cast('2026-11-22' as date), cast('2026-11-26' as date), N'Активные, высокий и средний ЦИ', 7, 15)
	, (101551, N'Дарим 50р. монетами на выходные', N'101551_Дарим 50р монетами выходные', '2026-11-06', '2026-11-22', '2026-11-26', N'Активные, высокий и средний ЦИ', 7, 15)
	, (101552, N'Вернём 150р. монетами с покупки от 1000р.', N'101552_Вернём 150р монетами брать его', '2026-11-03', '2026-11-26', '2026-11-30', N'Активные, низкий ЦИ', 8, 9)
	, (101553, N'Дарим 50р. монетами на выходные', N'101553_Дарим 50р монетами выходные', '2026-11-06', '2026-11-22', '2026-11-26', N'Активные, низкий ЦИ', 8, 15)
	, (101554, N'50% кешбэк на категории по выходным', N'101554_50 кешбэк категории выходным', '2026-11-06', '2026-11-22', '2026-11-29', N'Случайные', 12, 16)
	, (101555, N'Вернём 200р. монетами с покупки от 700р.', N'101555_Вернём 200р монетами покупки 700р', '2026-11-06', '2026-11-22', '2026-11-29', N'Отток', 2, 9)
) as v (ID_PROMO, PROMO_NAME, PROMO_NAME_ML, D, FD, AFTER_TO, SEG, ID_SEG, ID_MECH)
where not exists (select 1 from I_PROMO as p (nolock) where p.ID_PROMO = v.ID_PROMO and p.ID_COMPANY = 1)

-- новое имя списка для ML у 101552 (v1 — «101552_Вернём 150р монетами покупки»)
update I_PROMO set PROMO_NAME_ML = N'101552_Вернём 150р монетами брать его'
where ID_PROMO = 101552 and ID_COMPANY = 1 and isnull(PROMO_NAME_ML, N'') <> N'101552_Вернём 150р монетами брать его'
GO


-- ==================================================================================
-- 2. СПИСКИ И КОНТРОЛЬНЫЕ ГРУППЫ
-- ==================================================================================

if cast(getdate() as date) > '2026-11-22'
begin
	raiserror(N'101550–101555: акции прошли — списки не собираются', 10, 1)
	return
end
if (select count(distinct ID_PROMO) from I_PROMO (nolock) where ID_COMPANY = 1 and ID_PROMO between 101550 and 101555) <> 6
begin
	raiserror(N'101550–101555: в I_PROMO не все 6 шапок — списки не собраны', 16, 1)
	return
end
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO in (101550, 101551, 101552, 101553, 101554, 101555))
begin
	raiserror(N'101550–101555: списки уже в I_PROMO_OFFER — сборка пропущена', 10, 1)
	return
end

declare @msg nvarchar(400)

-- клиенты офлайна с признаками и ценовым индексом
drop table if exists #c
select	c.ID_CONTACT, c.SEGMENT, c.AVG_CHECK, c.VISITS, c.LAST_DATA, c.REFERENCE_DATE, c.ID_SALE_CHANNEL, p.PRICE_INDEX
into #c
from I_CVM_CONTACT as c (nolock)
		left join I_CONTACT_PRICE_SEGMENT as p (nolock)
		on p.ID_COMPANY = c.ID_COMPANY
		and p.ID_ORGANIZATION = c.ID_ORGANIZATION
		and p.ID_CONTACT = c.ID_CONTACT
where c.ID_COMPANY = 1 and c.ID_ORGANIZATION = 1
	and c.ID_CONTACT <> 0
	and c.SEGMENT between 1 and 6
option (maxdop 4)

if exists (select ID_CONTACT from #c group by ID_CONTACT having count(*) > 1)
begin
	raiserror(N'101550–101555: клиент повторяется в I_CVM_CONTACT (компания 1, орг. 1) — списки не собраны', 16, 1)
	return
end

-- граница низкого ЦИ: нижняя треть клиентов офлайна по PRICE_INDEX
declare @n_pi bigint = (select count(*) from #c where PRICE_INDEX is not null)
declare @b_low decimal(18, 6) = (select max(PRICE_INDEX) from (select top (@n_pi / 3) PRICE_INDEX from #c where PRICE_INDEX is not null order by PRICE_INDEX) as x)
set @msg = concat(N'Клиентов офлайна с индексом ', @n_pi, N', граница низкого ЦИ PRICE_INDEX <= ', @b_low)
raiserror(@msg, 0, 1) with nowait

-- акция клиента (сегменты не пересекаются по построению)
drop table if exists #a
select	ID_CONTACT
		, SEGMENT
		, ID_PROMO = case
			when SEGMENT in (2, 3) and PRICE_INDEX > @b_low and (VISITS <= 4 or LAST_DATA <= dateadd(day, -21, REFERENCE_DATE)) then 101550
			when SEGMENT in (2, 3) and PRICE_INDEX > @b_low then 101551
			when SEGMENT in (2, 3) and PRICE_INDEX <= @b_low and AVG_CHECK <= 750 then 101552
			when SEGMENT in (2, 3) and PRICE_INDEX <= @b_low then 101553
			when SEGMENT = 4 then 101554
			when SEGMENT = 5 and PRICE_INDEX > @b_low then 101555
		  end
into #a
from #c
where ID_SALE_CHANNEL in (1, 2)

delete from #a where ID_PROMO is null

create clustered columnstore index cci_a on #a

-- защита от неполного чтения I_CVM_CONTACT / I_CONTACT_PRICE_SEGMENT (замер 01.11: 8,76 млн, из них 101552 — 1,25 млн)
declare @n_a bigint = (select count(*) from #a)
declare @n_552 bigint = (select count(*) from #a where ID_PROMO = 101552)
if @n_a < 7000000 or @n_552 < 1000000
begin
	set @msg = concat(N'101550–101555: клиентов аудиторий ', @n_a, N' (101552 — ', @n_552, N') — меньше ожидаемого (неполное чтение?), списки не собраны')
	raiserror(@msg, 16, 1)
	return
end

-- список: пуш, минус I_GLOBAL_CG
drop table if exists #x

select	a.ID_PROMO
		, a.ID_CONTACT
		, CONTROL_GROUP = 0
		, ID_ORGANIZATION = 1
		, LOAD_TO_ML = 0
		, ID_COMPANY = 1
		, CRM_GUID = max(b.CRM_GUID)
		, LOAD_TO_MC = 1
		, PART = iif(a.SEGMENT >= 4, 4, 1)
into #x
from	  #a as a
		inner join I_CONTACT as b (nolock)
		on a.ID_CONTACT = b.ID_CONTACT
		and b.ID_COMPANY = 1
		left join I_GLOBAL_CG as c (nolock)
		on a.ID_CONTACT = c.ID_CONTACT
where b.HAS_PUSH = 1 and b.TOKENS = 1
	and c.ID_CONTACT is null
group by a.ID_PROMO, a.ID_CONTACT, a.SEGMENT

create clustered columnstore index cci_x on #x

-- защита: клиент — ровно в одной акции
if exists (select ID_CONTACT from #x group by ID_CONTACT having count(*) > 1)
begin
	raiserror(N'101550–101555: клиент попал в две акции — списки не собраны', 16, 1)
	return
end

set @msg = concat(N'Клиентов аудиторий ', @n_a, N', строк списков ', (select count(*) from #x))
raiserror(@msg, 0, 1) with nowait


drop table if exists #t

select	a.ID_PROMO
		, a.PART
		, a.ID_CONTACT
		, COST_DISCOUNT = round(sum(iif(a.PART = 4, b.LTV, b.BUDGET))/10,0)
		, COST_DISCOUNT1 = sum(iif(a.PART = 4, b.LTV, b.BUDGET))
		, MIN_DATA = min(FIRST_DATA)
		, LAST_DATA = max(LAST_DATA)
into #t
from	#x as a
	join I_CVM_CONTACT b (nolock)
		on a.ID_CONTACT = b.ID_CONTACT
		and a.ID_COMPANY = b.ID_COMPANY
		and b.ID_ORGANIZATION = 1
where b.ID_CONTACT <> 0
group by a.ID_PROMO, a.PART, a.ID_CONTACT

create clustered columnstore index cci_t on #t

-- защита: I_CVM_CONTACT пересчитали между чтениями — у части строк списка нет строки для подбора КГ
if (select count(*) from #t) <> (select count(*) from #x)
begin
	raiserror(N'101550–101555: не у всех клиентов списков прочитана строка I_CVM_CONTACT — списки не собраны, раннер повторит', 16, 1)
	return
end


-- КГ по каждой акции и части (1 — SEGMENT 2–3, RN/20; 4 — SEGMENT 4–5, RN/10)
drop table if exists #cg
create table #cg (ID_PROMO int, ID_CONTACT int)
drop table if exists #best_cg
create table #best_cg (ID_CONTACT int)

declare @p int, @part int, @div int, @error float, @best float, @iter int

drop table if exists #parts
select distinct ID_PROMO, PART into #parts from #t

declare parts cursor local fast_forward for
	select ID_PROMO, PART from #parts
open parts
fetch next from parts into @p, @part

while @@fetch_status = 0
begin
	set @div = iif(@part = 4, 10, 20)
	set @error = 1.0
	set @best = null
	set @iter = 0
	truncate table #best_cg

	drop table if exists #tp
	select ID_CONTACT, COST_DISCOUNT1
			, SEGMENT = ROW_NUMBER() over (order by MIN_DATA, COST_DISCOUNT, LAST_DATA) / @div
	into #tp
	from #t
	where ID_PROMO = @p and PART = @part

	create clustered columnstore index cci_tp on #tp

	while @error >= 0.0005 and @iter < 1000
	begin

	drop table if exists #local_cg

	;with n as (
	select ID_CONTACT
			, R = ROW_NUMBER() over (partition by SEGMENT order by newid())
	from #tp
	)
	select ID_CONTACT
			, CONTROL_GROUP=1
	into #local_cg
	from n
	where R=1

	drop table if exists #stat;
	;with t as (
	select CG = IIF(b.ID_CONTACT is not NULL, 1,0)
			, c.COST_DISCOUNT1
	from	#tp as c
			left join #local_cg as b
			on c.ID_CONTACT=b.ID_CONTACT
	)
	select	CG
			, COST_DISCOUNT= sum(COST_DISCOUNT1)/ count(*)
			, COUNT_CLIENT = count(*)
	into #stat
	from t
	group by CG
	;

	set @error = isnull((select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / nullif(a.COST_DISCOUNT, 0)
			from #stat a
				join #stat b
					on a.CG = 0
					and b.CG = 1
			), 1.0)

	if @best is null or @error < @best
	begin
		set @best = @error
		truncate table #best_cg
		insert #best_cg (ID_CONTACT) select ID_CONTACT from #local_cg
	end

	set @iter += 1
	end

	set @msg = concat(@p, N' часть ', @part, N': КГ ', (select count(*) from #best_cg), N', попыток ', @iter, N', ошибка ', @best)
	raiserror(@msg, 0, 1) with nowait

	insert into #cg (ID_PROMO, ID_CONTACT)
	select @p, ID_CONTACT from #best_cg

	fetch next from parts into @p, @part
end
close parts
deallocate parts


UPDATE d
SET CONTROL_GROUP=1
from	#cg as a
		inner join  #x as d
		on a.ID_CONTACT=d.ID_CONTACT
		and a.ID_PROMO=d.ID_PROMO


-- списки записал другой запуск, пока подбиралась КГ
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO in (101550, 101551, 101552, 101553, 101554, 101555))
begin
	raiserror(N'101550–101555: списки записаны другим запуском — вставка пропущена', 10, 1)
	return
end

INSERT INTO I_PROMO_OFFER (ID_PROMO, ID_CONTACT, CONTROL_GROUP, ID_ORGANIZATION, LOAD_TO_ML, ID_COMPANY, CRM_GUID, LOAD_TO_MC)
SELECT ID_PROMO, ID_CONTACT, CONTROL_GROUP, ID_ORGANIZATION, LOAD_TO_ML, ID_COMPANY, CRM_GUID, LOAD_TO_MC
from #x


-- сводка — только в запуске, который записал списки
select	p.ID_PROMO
		, [Название списка] = p.PROMO_NAME_ML
		, [Клиентов на отправку] = sum(iif(x.CONTROL_GROUP = 0, 1, 0))
		, [КГ] = sum(iif(x.CONTROL_GROUP = 1, 1, 0))
		, [Дата старта] = p.START_DATE
		, [Загрузка в Манзана Онлайн] = N'да'
from #x as x
		inner join I_PROMO as p (nolock)
		on p.ID_PROMO = x.ID_PROMO
		and p.ID_COMPANY = 1
group by p.ID_PROMO, p.PROMO_NAME_ML, p.START_DATE
order by p.ID_PROMO
GO
