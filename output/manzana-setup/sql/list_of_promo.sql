-- База списков: список одной акции — дата старта, имя кампании в Campaign (null — Campaign не нужен), число CRM_GUID
-- на отправку (ЦГ, различных). Нет строки — списка акции в базе списков нет (кампании ждут).
select id_promo, start_date, campaign_name, guids_send
from lists.promo
where id_promo = %(number)s;
