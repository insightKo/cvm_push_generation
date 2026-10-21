-- CVM · 101445–101474 · название акций в I_PROMO: «Вернём 100 монет с покупки …» → «Вернём 100р. монетами с покупки …»
-- (Елена 21.10.2026; шапки заведены CVM_101445_101474.sql в 12:00). Меняются только строки со старым названием — повторный
-- прогон ничего не делает. PROMO_NAME_ML (имя списка для ML) не меняется.

SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
SET NOCOUNT ON
GO

USE mci_model
GO

update I_PROMO
set PROMO_NAME = N'Вернём 100р. монетами с покупки ' + substring(PROMO_NAME, len(N'Вернём 100 монет с покупки ') + 2, 250)
where ID_COMPANY = 1
	and ID_PROMO between 101445 and 101474
	and TYPE_PROMO = N'CVM'
	and PROMO_NAME like N'Вернём 100 монет с покупки %'

select ID_PROMO, PROMO_NAME, PROMO_NAME_ML
from I_PROMO (nolock)
where ID_COMPANY = 1 and ID_PROMO between 101445 and 101474
order by ID_PROMO
GO
