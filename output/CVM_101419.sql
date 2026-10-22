-- CVM · одна акция: 101419 · Скидка 20% на бумажные и влажные салфетки · Активные и Новые · вс 25.10.2026
-- Правило из CVM_BUILD_OFFERS.sql (ОК Елены 05.10.2026): все Активные и Новые с пушем (SEGMENT 1, 2, 3), минус I_GLOBAL_CG, КГ 5% (RN/20) по BUDGET.
-- Блок — как CVM_101416.sql. Шапка акции заведена месячным скриптом CVM_october_2026_I_PROMO.sql.
-- LOAD_TO_ML = 0: акция с выгодой, настраивается в Манзана Онлайн (уточнение 18.10.2026). LOAD_TO_MC = 1: PUSH.
--
-- Скрипт для задания CVM_API_nightly на SQL01: раннер выполняет файл при каждом запуске и сам его не удаляет, поэтому:
--   · после дня акции (позже 25.10.2026) → блок пропускается, таблицы не читаются;
--   · список уже собран → блок пропускается, ничего не пишется;
--   · клиентов меньше 80% списка 101416 (то же правило, собран 19.10: 2 839 956 с КГ, числом в скрипте) → ошибка, ничего не пишется;
--   · КГ не подобралась за 200 итераций → ошибка, ничего не пишется.
-- Выгрузки CRM_GUID здесь нет: список на SFTP для Campaign забирает задание cvm-campaign-lists (каждые 15 минут, 101419 согласована в портале).

SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET ANSI_WARNINGS ON
SET ARITHABORT ON
SET NOCOUNT ON
GO

USE mci_model
GO


-- ==================================================================================
-- СБОРКА СПИСКА 101419
-- ==================================================================================

-- 101419_20 бумажные влажные салфетки

-- защита: день акции прошёл — блок пропускается
if cast(getdate() as date) > '2026-10-25'
begin
	raiserror(N'101419: акция прошла (25.10.2026) — блок пропущен, файл можно убрать из папки', 10, 1)
	return
end
-- защита: шапка акции должна быть в I_PROMO (CVM_october_2026_I_PROMO.sql)
if not exists (select 1 from I_PROMO (nolock) where ID_PROMO = 101419 and ID_COMPANY = 1)
begin
	raiserror(N'Нет шапки 101419 в I_PROMO — сначала CVM_october_2026_I_PROMO.sql', 16, 1)
	return
end
-- защита: список уже собран — блок пропускается (повторный прогон не задвоит)
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101419)
begin
	raiserror(N'101419 уже в I_PROMO_OFFER — блок пропущен', 10, 1)
	return
end

drop table if exists #x

;with t as
(
select	a.ID_CONTACT
from I_CVM_CONTACT as a (nolock)
where a.ID_COMPANY=1 and a.ID_ORGANIZATION=1
	and a.ID_CONTACT<>0
	and a.SEGMENT in (1, 2, 3)
group by a.ID_CONTACT
)
select ID_PROMO = 101419
		, t.ID_CONTACT
		, CONTROL_GROUP = IIF(c.ID_CONTACT is null,0,1)
		, ID_ORGANIZATION = 1
		, LOAD_TO_ML = 0
		, ID_COMPANY=1
		, CRM_GUID
		, LOAD_TO_MC = 1
into #x
from	  t
		inner join I_CONTACT as b (nolock)
		on t.ID_CONTACT=b.ID_CONTACT
		and b.ID_COMPANY=1
		left join I_GLOBAL_CG as c (nolock)
		on t.ID_CONTACT=c.ID_CONTACT
where HAS_PUSH=1 and TOKENS=1
and c.ID_CONTACT is null
group by t.ID_CONTACT
		,c.ID_CONTACT
		, CRM_GUID


-- защита: неполное чтение I_CVM_CONTACT — сверка с 101416 (то же правило, неделей раньше)
declare @n bigint = (select count(*) from #x)
declare @n_ref bigint = 2839956	-- 101416 с КГ на 19.10.2026
declare @msg nvarchar(200)
if @n < 0.8 * @n_ref
begin
	set @msg = concat(N'101419: клиентов ', @n, N' — меньше 80%% списка 101416 (', @n_ref, N'), список не записан')
	raiserror(@msg, 16, 1)
	return
end


drop table if exists #t

select	a.ID_CONTACT
		, COST_DISCOUNT= round(sum(BUDGET)/10,0)
		, COST_DISCOUNT1 = sum(BUDGET)
		, MIN_DATA = min(FIRST_DATA)
		, LAST_DATA = max(LAST_DATA)
into #t
from	#x as a (nolock)
	join I_CVM_CONTACT b (nolock)
		on a.ID_CONTACT = b.ID_CONTACT
		and a.ID_COMPANY = b.ID_COMPANY
		and b.ID_ORGANIZATION=1
where a.CONTROL_GROUP=0
and b.ID_CONTACT<>0
group by a.ID_CONTACT


declare @error float = 1.0, @iter int = 0

while @error >= 0.0005 and @iter < 200
begin

drop table if exists #local_cg

;with x as
(
select ID_CONTACT
		, RN = ROW_NUMBER() over (order by MIN_DATA, COST_DISCOUNT, LAST_DATA)
from #t
), a as (
select ID_CONTACT
		, SEGMENT = RN/20
from x
), n as (
select ID_CONTACT
		, R = ROW_NUMBER() over (partition by SEGMENT order by newid())
from a
)
select ID_CONTACT
		, CONTROL_GROUP=1
into #local_cg
from n
where R=1

drop table if exists #stat;
;with t as (
select CG = IIF(b.ID_CONTACT is not NULL, 1,0)
		, COST_DISCOUNT1
		, LAST_DATA
		, a.ID_CONTACT
from	#x as a (nolock)
		left join #local_cg  as b
		on a.ID_CONTACT=b.ID_CONTACT
		inner join #t as c
		on a.ID_CONTACT=c.ID_CONTACT
where  a.CONTROL_GROUP=0
group by COST_DISCOUNT1
		, LAST_DATA
		, a.ID_CONTACT
		, b.ID_CONTACT
)
select	CG
		, COST_DISCOUNT= sum(COST_DISCOUNT1)/ count(ID_CONTACT)
		, COUNT_CLIENT = count(distinct ID_CONTACT)
into #stat
from t
group by CG
;

set @error = (select abs(a.COST_DISCOUNT - b.COST_DISCOUNT) / nullif(a.COST_DISCOUNT, 0)
		from #stat a
			join #stat b
				on a.CG = 0
				and b.CG = 1
		)

set @iter += 1
set @msg = concat(N'101419: итерация подбора КГ ', @iter, N', ошибка ', @error)
raiserror(@msg, 0, 1) with nowait
end

-- защита: КГ не подобралась — ничего не пишем
if @error >= 0.0005 or @error is null
begin
	set @msg = concat(N'101419: КГ не подобралась за ', @iter, N' итераций (ошибка ', @error, N'), список не записан')
	raiserror(@msg, 16, 1)
	return
end

select * from #stat


UPDATE d
SET CONTROL_GROUP=1
from	#local_cg as a
		inner join  #x as d
		on a.ID_CONTACT=d.ID_CONTACT


-- защита: список записал другой запуск, пока подбиралась КГ
if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101419)
begin
	raiserror(N'101419 записан другим запуском — вставка пропущена', 10, 1)
	return
end

INSERT INTO I_PROMO_OFFER (ID_PROMO, ID_CONTACT, CONTROL_GROUP, ID_ORGANIZATION, LOAD_TO_ML, ID_COMPANY, CRM_GUID, LOAD_TO_MC)
SELECT ID_PROMO, ID_CONTACT, CONTROL_GROUP, ID_ORGANIZATION, LOAD_TO_ML, ID_COMPANY, CRM_GUID, LOAD_TO_MC
from #x


-- сводка — только в запуске, который записал список
select ID_PROMO = 101419
		, [Название списка] = N'101419_20 бумажные влажные салфетки'
		, [Клиентов на отправку] = count(distinct ID_CONTACT)
		, [Дата старта] = cast('2026-10-25' as date)
		, [Загрузка в Манзана Онлайн] = N'да'
from I_PROMO_OFFER (nolock)
where ID_PROMO = 101419 and CONTROL_GROUP = 0
	and ID_COMPANY = 1 and ID_ORGANIZATION = 1
GO


-- ==================================================================================
-- УБОРКА ВРЕМЕННЫХ ТАБЛИЦ СЕССИИ
-- ==================================================================================

drop table if exists #x
drop table if exists #t
drop table if exists #local_cg
drop table if exists #stat
GO
