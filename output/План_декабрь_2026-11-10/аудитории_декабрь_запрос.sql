-- Численности аудиторий для плана декабря 2026 (только чтение, агрегаты, без списков клиентов).
-- Логика подгрупп — как в output/CVM_101550_101555.sql (ноябрь, ОК Елены 01.11): офлайн орг. 1, SEGMENT 1–6;
--   низкий ЦИ = нижняя треть клиентов офлайна по PRICE_INDEX (I_CONTACT_PRICE_SEGMENT), граница считается здесь;
--   «редкие» Активные = VISITS <= 4 или нет покупок 21 день до REFERENCE_DATE.
-- Признаки: пуш (I_CONTACT HAS_PUSH = 1 и TOKENS = 1), ГКГ (I_GLOBAL_CG), фрод (I_FRAUD_CONTACT), канал продаж (1 офлайн, 2 омни),
--   полоса среднего чека за 6 недель (AVG_CHECK), полоса максимального чека (MAX_CHECK).
-- Нагрузка: каждая таблица MCI читается один раз (шаг 1), дальше — только #c.

set nocount on;

-- 1. клиенты офлайна с готовыми признаками — один проход по I_CVM_CONTACT, I_CONTACT_PRICE_SEGMENT, I_CONTACT, I_GLOBAL_CG, I_FRAUD_CONTACT;
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
		, CHK    = case when c.AVG_CHECK is null or c.AVG_CHECK <= 0 then 0
					   when c.AVG_CHECK <= 750 then 1 when c.AVG_CHECK <= 1000 then 2 when c.AVG_CHECK <= 1500 then 3
					   when c.AVG_CHECK <= 2300 then 4 when c.AVG_CHECK <= 3500 then 5 else 6 end
		, MAXCHK = case when c.MAX_CHECK is null or c.MAX_CHECK <= 0 then 0
					   when c.MAX_CHECK <= 1000 then 1 when c.MAX_CHECK <= 1500 then 2 when c.MAX_CHECK <= 2000 then 3
					   when c.MAX_CHECK <= 3000 then 4 when c.MAX_CHECK <= 5000 then 5 else 6 end
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

-- 3. граница низкого ЦИ — как в ноябрьском скрипте: нижняя треть клиентов #c (SEGMENT 1–6, все каналы) по PRICE_INDEX
declare @n_pi bigint, @b_low decimal(18, 6);
select @n_pi = count(*) from #c (nolock) where PRICE_INDEX is not null option (maxdop 4);
select @b_low = max(PRICE_INDEX)
from (select top (@n_pi / 3) PRICE_INDEX from #c (nolock) where PRICE_INDEX is not null order by PRICE_INDEX) as x
option (maxdop 4, recompile);

-- 4. агрегат по готовым колонкам
select	B_LOW = @b_low
		, SEGMENT, ID_SALE_CHANNEL, PUSH, GCG, FRAUD, CI, RARE, CHK, MAXCHK
		, CLIENTS = count(*)
		, SUM_AVG_CHECK = sum(cast(AVG_CHECK as float))
from (
	select	SEGMENT, ID_SALE_CHANNEL, PUSH, GCG, FRAUD, RARE, CHK, MAXCHK, AVG_CHECK
			, CI = case when PRICE_INDEX is null then 0 when PRICE_INDEX <= @b_low then 1 else 2 end		-- 0 нет индекса, 1 низкий, 2 высокий+средний
	from #c (nolock)
) as c
group by SEGMENT, ID_SALE_CHANNEL, PUSH, GCG, FRAUD, CI, RARE, CHK, MAXCHK
option (maxdop 4);
