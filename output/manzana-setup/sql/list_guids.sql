-- База списков: CRM_GUID участников акции без КГ — различные, без пустых (клиент без CRM_GUID), в верхнем регистре.
-- Различные — как guids_send акции, с которым робот сверяет число. Робот читает их серверным курсором порциями.
select distinct upper(g::text) as crm_guid
from lists.offer_chunk c
cross join lateral unnest(c.crm_guid) as g
where c.id_promo = %(number)s
  and c.control_group = 0
  and g is not null
order by 1;
