-- База списков (cvm_lists), роль lists_manzana: списки, которые нужны в Campaign (имя кампании задано; у SLIP его нет),
-- со стартом сегодня или позже по МСК, ещё не заведённые роботом (нет итога done для этой даты старта).
-- Колонки lists.promo — только выданные роли (002_manzana.sql). CRM_GUID здесь нет — их читает list_guids.sql.
select p.id_promo, p.start_date, p.campaign_name, p.guids_send
from lists.promo p
where nullif(p.campaign_name, '') is not null
  and p.start_date >= %(start_from)s
  and not exists (select 1
                  from lists.manzana m
                  where m.id_promo = p.id_promo
                    and m.start_date = p.start_date
                    and m.kind = 'list'
                    and m.status = 'done')
order by p.start_date, p.id_promo;
