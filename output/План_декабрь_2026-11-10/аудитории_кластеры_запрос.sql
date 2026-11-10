-- Численности аудиторий для плана декабря 2026 ПО КЛАСТЕРАМ (только чтение, агрегаты, без списков клиентов).
-- Отличие от аудитории_декабрь_запрос.sql (ревью sql-senior 10.11): + кластер офлайна CLUSTER_DETAIL последнего готового присвоения
--   (выбор присвоения — как в output/CVM_101538_101549_v2.sql), в агрегате вместо CHK/MAXCHK — CL.
-- Логика подгрупп — как в output/CVM_101550_101555.sql (ноябрь, ОК Елены 01.11): офлайн орг. 1, SEGMENT 1–6;
--   низкий ЦИ = нижняя треть клиентов офлайна по PRICE_INDEX (I_CONTACT_PRICE_SEGMENT), граница считается здесь;
--   «редкие» Активные = VISITS <= 4 или нет покупок 21 день до REFERENCE_DATE.
-- Признаки: пуш (I_CONTACT HAS_PUSH = 1 и TOKENS = 1), ГКГ (I_GLOBAL_CG), фрод (I_FRAUD_CONTACT), канал продаж (1 офлайн, 2 омни),
--   средний чек за 6 недель (AVG_CHECK) — только в SUM_AVG_CHECK; полосы чека CHK/MAXCHK в агрегате нет — здесь не считаются.
-- Нагрузка: каждая таблица MCI читается один раз (шаг 1), дальше — только #c.

set nocount on;

-- 0. последнее готовое присвоение кластеров офлайна (после него нет перезаписи RUN того же окна и версии) — как в CVM_101538_101549_v2.sql
declare @cl_date date, @cl_ver varchar(64)
declare @cl_log_max int = (select max(ID) from I_CLUSTER_ASSIGN_LOG (nolock))
select top (1) @cl_date = l.REFERENCE_DATE, @cl_ver = l.MODEL_VERSION
from I_CLUSTER_ASSIGN_LOG as l (nolock)
where l.ID_ORGANIZATION = 1 and l.STATUS = 'OK'
	and not exists (select 1 from I_CLUSTER_ASSIGN_LOG as r (nolock)
					where r.ID_ORGANIZATION = l.ID_ORGANIZATION and r.REFERENCE_DATE = l.REFERENCE_DATE
						and r.MODEL_VERSION = l.MODEL_VERSION and r.STATUS = 'RUN' and r.ID > l.ID)
order by l.REFERENCE_DATE desc, l.ID desc;
if @cl_date is null
begin
	raiserror(N'Аудитории декабря по кластерам: нет готового присвоения кластеров офлайна', 16, 1);
	return;
end

-- 1. клиенты офлайна с готовыми признаками — один проход по I_CVM_CONTACT, I_CONTACT_PRICE_SEGMENT, I_CONTACT, I_GLOBAL_CG, I_FRAUD_CONTACT, I_CLUSTER_CONTACT;
--    I_CLUSTER_CONTACT (148 млн строк, CCI): группы строк чистые по REFERENCE_DATE и ID_ORGANIZATION — читается только окно @cl_date (~7,5 млн);
--    recompile не нужен: строк на дату почти поровну, оценка по плотности для @cl_date совпадает с фактом;
--    пуш / ГКГ / фрод = есть хоть одна строка (как EXISTS): источники свёрнуты по ID_CONTACT, строки клиента не размножаются
drop table if exists #c;
select	c.ID_CONTACT
		, c.SEGMENT
		, c.ID_SALE_CHANNEL
		, c.AVG_CHECK
		, p.PRICE_INDEX
		, PUSH   = iif(b.ID_CONTACT is null, 0, 1)
		, GCG    = iif(g.ID_CONTACT is null, 0, 1)
		, FRAUD  = iif(f.ID_CONTACT is null, 0, 1)
		, RARE   = iif(c.VISITS <= 4 or c.LAST_DATA <= dateadd(day, -21, c.REFERENCE_DATE), 1, 0)
		, CL       = isnull(k.CL, 0)					-- 0 = клиент без кластера в присвоении
		, CL_MULTI = iif(k.CL < k.CL_MAX, 1, 0)		-- 1 — у клиента несколько кластеров в одном присвоении (проверка 2а)
into #c
from I_CVM_CONTACT as c (nolock)
		left join I_CONTACT_PRICE_SEGMENT as p (nolock)
		on p.ID_COMPANY = c.ID_COMPANY
		and p.ID_ORGANIZATION = c.ID_ORGANIZATION
		and p.ID_CONTACT = c.ID_CONTACT
		left join (select ID_CONTACT from I_CONTACT (nolock) where ID_COMPANY = 1 and HAS_PUSH = 1 and TOKENS = 1 group by ID_CONTACT) as b
		on b.ID_CONTACT = c.ID_CONTACT
		left join (select ID_CONTACT from I_GLOBAL_CG (nolock) group by ID_CONTACT) as g
		on g.ID_CONTACT = c.ID_CONTACT
		left join (select ID_CONTACT from I_FRAUD_CONTACT (nolock) where ID_COMPANY = 1 group by ID_CONTACT) as f
		on f.ID_CONTACT = c.ID_CONTACT
		left join (select ID_CONTACT, CL = min(CLUSTER_DETAIL), CL_MAX = max(CLUSTER_DETAIL)	-- min < max = несколько кластеров; без count(distinct) — один агрегат вместо двух
				   from I_CLUSTER_CONTACT (nolock)
				   where ID_ORGANIZATION = 1 and REFERENCE_DATE = @cl_date and MODEL_VERSION = @cl_ver
				   group by ID_CONTACT) as k
		on k.ID_CONTACT = c.ID_CONTACT
where c.ID_COMPANY = 1 and c.ID_ORGANIZATION = 1
	and c.ID_CONTACT <> 0
	and c.SEGMENT between 1 and 6
option (maxdop 4);

create clustered columnstore index cci_c on #c with (maxdop = 4);

-- 2. как в ноябрьском скрипте: клиент повторяется — численности были бы завышены
if exists (select ID_CONTACT from #c (nolock) group by ID_CONTACT having count(*) > 1)
begin
	raiserror(N'Аудитории декабря: клиент повторяется в I_CVM_CONTACT / I_CONTACT_PRICE_SEGMENT (компания 1, орг. 1) — численности не посчитаны', 16, 1);
	return;
end

-- 2а. присвоение кластеров перезаписывалось во время чтения (NOLOCK мог попасть на удалённые строки) или у клиента несколько кластеров — стоп
if exists (select 1 from I_CLUSTER_ASSIGN_LOG (nolock)
		   where ID_ORGANIZATION = 1 and REFERENCE_DATE = @cl_date and MODEL_VERSION = @cl_ver and ID > @cl_log_max)
	or exists (select 1 from #c (nolock) where CL_MULTI = 1)
begin
	raiserror(N'Аудитории декабря по кластерам: присвоение кластеров перезаписывается или клиент в нескольких кластерах — не посчитано', 16, 1);
	return;
end

-- 3. граница низкого ЦИ — как в ноябрьском скрипте: нижняя треть клиентов #c (SEGMENT 1–6, все каналы) по PRICE_INDEX
declare @n_pi bigint, @b_low decimal(18, 6);
select @n_pi = count(*) from #c (nolock) where PRICE_INDEX is not null option (maxdop 4);
select @b_low = max(PRICE_INDEX)
from (select top (@n_pi / 3) PRICE_INDEX from #c (nolock) where PRICE_INDEX is not null order by PRICE_INDEX) as x
option (maxdop 4, recompile);

-- 4. агрегат по готовым колонкам
select	B_LOW = @b_low, CL_DATE = @cl_date, CL_VER = @cl_ver
		, SEGMENT, ID_SALE_CHANNEL, PUSH, GCG, FRAUD, CI, RARE, CL
		, CLIENTS = count(*)
		, SUM_AVG_CHECK = sum(cast(AVG_CHECK as float))
from (
	select	SEGMENT, ID_SALE_CHANNEL, PUSH, GCG, FRAUD, RARE, CL, AVG_CHECK
			, CI = case when PRICE_INDEX is null then 0 when PRICE_INDEX <= @b_low then 1 else 2 end		-- 0 нет индекса, 1 низкий, 2 высокий+средний
	from #c (nolock)
) as c
group by SEGMENT, ID_SALE_CHANNEL, PUSH, GCG, FRAUD, CI, RARE, CL
option (maxdop 4);
