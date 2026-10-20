-- База списков: итог done одного объекта заведения (список, кампания сообщения, купон) — для повтора и для аудитории
-- кампаний (имя МС, под которым робот загрузил список).
select name, external_id, rows, at
from lists.manzana
where id_promo = %(number)s
  and start_date = %(start_date)s
  and kind = %(kind)s
  and item = %(item)s
  and status = 'done';
