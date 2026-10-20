-- mci_model: список в файл — скрипт Елены (согласовано 08.10.2026). Параметр: ID_PROMO.
-- 20.10.2026 (решение Елены «1а»): строки без CRM_GUID в файл не пишутся — загрузчик ДИКСИ принимает только строки-GUID,
-- пустая строка делала весь файл невалидным (.error.json). То же условие — в offer_count.sql, иначе число строк не сойдётся.
select CRM_GUID
from dbo.I_PROMO_OFFER (nolock)
where ID_PROMO = cast(? as int)
  and CONTROL_GROUP = 0
  and CRM_GUID is not null
  and CRM_GUID <> '';
