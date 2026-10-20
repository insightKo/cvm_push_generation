-- База портала: акции к заведению в Manzana. Условие то же, что у задания cvm-campaign-lists (approved_promos.sql):
-- статус approved (решение человека, кто бы его ни поставил — портал или вкладка «CVM offline», тогда updated_by пуст),
-- старт start_from (сегодня по МСК) или позже. «Настройка» (setup) = «да» — нужны условия и купон в Manzana Online,
-- иначе только кампания и список. Колонки условий — те же, что заливка читает из «CVM offline» (sync/sheets_sync.py,
-- PROMO_COLUMNS). Списков CRM_GUID здесь нет и быть не может.
select number, title, setup, segments_raw, channels, start_date, end_date,
       description, mechanic, mechanic_online, category, bonus_expiry, coupon, coupon_name, slip_text, button,
       discount_sum, restrictions, coalesce(updated_by, 'CVM offline') as approved_by
from cvm.promo
where status = 'approved'
  and start_date >= %(start_from)s
order by start_date, number;
