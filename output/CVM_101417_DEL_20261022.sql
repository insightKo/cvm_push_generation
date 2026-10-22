-- CVM · удаление акции 101417 «Активируй 20% кешбэка на готовую еду и кулинарию» из I_PROMO (Елена 22.10.2026: удалена из Google 21.10).
-- Списка в I_PROMO_OFFER и строк эффекта у акции нет (проверено 22.10). I_CAMPAIGN (кампания 1867 из Manzana) не трогается — интеграционная загрузка;
-- кампанию в Manzana Online выключают там. Повторный прогон ничего не делает.

SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET NOCOUNT ON
GO

USE mci_model
GO

if exists (select 1 from I_PROMO_OFFER (nolock) where ID_PROMO = 101417)
begin
	raiserror(N'101417: в I_PROMO_OFFER есть список — шапка не удалена', 16, 1)
	return
end

delete from I_PROMO
where ID_COMPANY = 1
	and ID_PROMO = 101417
	and TYPE_PROMO = N'CVM'
	and PROMO_NAME = N'Активируй 20% кешбэка на готовую еду и кулинарию'

select [Удалено] = @@rowcount
	 , [Осталось в I_PROMO] = (select count(*) from I_PROMO (nolock) where ID_COMPANY = 1 and ID_PROMO = 101417)
GO
