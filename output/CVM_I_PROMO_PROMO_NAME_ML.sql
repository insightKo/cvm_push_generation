-- CVM · I_PROMO: колонка PROMO_NAME_ML — имя списка для ML · 13.10.2026
-- Имя = ID_PROMO + '_' + название акции до 20 символов, без спецсимволов и повторов слов.
-- Запускать целиком (F5) в mci_model.

use mci_model
GO

if col_length('dbo.I_PROMO', 'PROMO_NAME_ML') is null
	alter table dbo.I_PROMO add PROMO_NAME_ML nvarchar(32) null
GO

update dbo.I_PROMO set PROMO_NAME_ML = N'101412_20 мандарины'       where ID_PROMO = 101412 and ID_COMPANY = 1
update dbo.I_PROMO set PROMO_NAME_ML = N'101413_20 кешбэка колбасы' where ID_PROMO = 101413 and ID_COMPANY = 1
update dbo.I_PROMO set PROMO_NAME_ML = N'101415_20 женскую гигиену' where ID_PROMO = 101415 and ID_COMPANY = 1
update dbo.I_PROMO set PROMO_NAME_ML = N'101416_20 пельмени вареники' where ID_PROMO = 101416 and ID_COMPANY = 1
update dbo.I_PROMO set PROMO_NAME_ML = N'101417_20 кешбэка готовую'  where ID_PROMO = 101417 and ID_COMPANY = 1
update dbo.I_PROMO set PROMO_NAME_ML = N'101419_20 бумажные влажные' where ID_PROMO = 101419 and ID_COMPANY = 1
update dbo.I_PROMO set PROMO_NAME_ML = N'101420_20 хурму гранат'     where ID_PROMO = 101420 and ID_COMPANY = 1
update dbo.I_PROMO set PROMO_NAME_ML = N'101432_50р чек 500р'        where ID_PROMO = 101432 and ID_COMPANY = 1
update dbo.I_PROMO set PROMO_NAME_ML = N'101433_100р чек 1000р'      where ID_PROMO = 101433 and ID_COMPANY = 1
update dbo.I_PROMO set PROMO_NAME_ML = N'101434_300р чек 3000р'      where ID_PROMO = 101434 and ID_COMPANY = 1
update dbo.I_PROMO set PROMO_NAME_ML = N'101425_Баланс баллов'       where ID_PROMO = 101425 and ID_COMPANY = 1
GO

select ID_PROMO, PROMO_NAME, PROMO_NAME_ML
from dbo.I_PROMO (nolock)
where ID_PROMO between 101412 and 101434 and ID_COMPANY = 1
order by ID_PROMO
GO
