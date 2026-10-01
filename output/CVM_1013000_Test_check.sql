-- CVM · проверка списка 1013000_Test на следующий день (27.08.2026)
-- Что проверяем: после отработки загрузки поле LOAD_TO_ML в I_PROMO_OFFER должно смениться с 0 на 1.
-- Прогонять целиком. Ожидание: в п.1 одна строка LOAD_TO_ML = 1, в п.3 вердикт «ОК».


-- ==================================================================================
-- 1. РАСКЛАДКА СПИСКА ПО LOAD_TO_ML
-- ==================================================================================

select	LOAD_TO_ML
		, [Клиентов] = count(distinct ID_CONTACT)
		, [Из них на отправку] = count(distinct case when CONTROL_GROUP = 0 then ID_CONTACT end)
		, [Из них КГ] = count(distinct case when CONTROL_GROUP = 1 then ID_CONTACT end)
from I_PROMO_OFFER (nolock)
where ID_PROMO = 1013000
group by LOAD_TO_ML
order by LOAD_TO_ML
GO


-- ==================================================================================
-- 2. ОСТАВШИЕСЯ НУЛИ — ПЕРВЫЕ 100 КОНТАКТОВ, ЕСЛИ ФЛАГ НЕ ПЕРЕКЛЮЧИЛСЯ
-- ==================================================================================

select top 100
		ID_CONTACT
		, CRM_GUID
		, CONTROL_GROUP
		, LOAD_TO_ML
from I_PROMO_OFFER (nolock)
where ID_PROMO = 1013000
	and LOAD_TO_ML <> 1
order by ID_CONTACT
GO


-- ==================================================================================
-- 3. ВЕРДИКТ
-- ==================================================================================

;with s as (
select	[Всего] = count(*)
		, [С LOAD_TO_ML = 1] = sum(case when LOAD_TO_ML = 1 then 1 else 0 end)
		, [С LOAD_TO_ML = 0] = sum(case when LOAD_TO_ML = 0 then 1 else 0 end)
		, [Прочие значения] = sum(case when LOAD_TO_ML not in (0, 1) or LOAD_TO_ML is null then 1 else 0 end)
from I_PROMO_OFFER (nolock)
where ID_PROMO = 1013000
)
select	ID_PROMO = 1013000
		, [Название списка] = N'1013000_Test'
		, [Всего]
		, [С LOAD_TO_ML = 1]
		, [С LOAD_TO_ML = 0]
		, [Прочие значения]
		, [Вердикт] = case
				when [Всего] = 0 then N'НЕТ ДАННЫХ — списка 1013000 в I_PROMO_OFFER нет'
				when [С LOAD_TO_ML = 1] = [Всего] then N'ОК — флаг переключился на 1 по всему списку'
				when [С LOAD_TO_ML = 1] = 0 then N'НЕ ПРОШЛО — флаг остался 0 по всему списку'
				else N'ЧАСТИЧНО — флаг переключился не у всех строк'
			end
from s
GO
