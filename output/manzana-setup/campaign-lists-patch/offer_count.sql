-- mci_model: число клиентов акции (согласовано с Еленой 08.10.2026). Параметр: ID_PROMO.
-- Задание сравнивает его с тем же числом через 5 минут: не изменилось — загрузка списка в базу закончилась.
-- PORTAL_ROWS — строки списка портала («Сегменты»): файл в обмен передал сам портал (PUSH) или Campaign списку не нужен
-- (SLIP) — такую акцию задание не выгружает (решения Елены 10–11.10.2026). Список портала узнаётся по блоку номеров
-- 500000–599999: с 11.10.2026 портал пишет LOAD_TO_ML = 0 у SLIP и акций с выгодой, LOAD_TO_ML = 2 — у PUSH-коммуникаций.
-- 20.10.2026 (решение Елены «1а»): считаются только строки с CRM_GUID — те же, что уходят в файл (offer_guids.sql).
select count(*) as ROWS_SEND,
       count(case when LOAD_TO_ML = 2 or ID_PROMO between 500000 and 599999 then 1 end) as PORTAL_ROWS
from dbo.I_PROMO_OFFER with (nolock)
where ID_PROMO = cast(? as int)
  and CONTROL_GROUP = 0
  and CRM_GUID is not null
  and CRM_GUID <> '';
