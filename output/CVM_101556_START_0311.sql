-- 101556 «Дарим 200р. монетами на неделю» (Спящие): старт переносится с 02.11 на 03.11.2026 по слову Елены 01.11.2026
-- («перенеси на 03.11, чтобы могли успеть подготовиться»). Список I_PROMO_OFFER не меняется. Повтор безопасен.
SET NOCOUNT ON
SET XACT_ABORT ON
GO
USE mci_model
GO
update I_PROMO
set START_DATE = '2026-11-03'
	, FINISH_DATE_FROM = '2026-11-02'
	, START_DATE_FROM = dateadd(week, -52*3, cast('2026-11-03' as date))
where ID_PROMO = 101556 and ID_COMPANY = 1 and START_DATE = '2026-11-02'

select ID_PROMO, START_DATE, FINISH_DATE, START_DATE_FROM, FINISH_DATE_FROM, START_DATE_AFTER, FINISH_DATE_AFTER
from I_PROMO (nolock) where ID_PROMO = 101556 and ID_COMPANY = 1
GO
