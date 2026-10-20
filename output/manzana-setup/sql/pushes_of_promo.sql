-- База портала: согласованные PUSH-сообщения акции из вкладки PUSH (cvm.push). Текст — правка портала, если была
-- (title_edited / body_edited), иначе из вкладки. Снятые из вкладки строки (deleted_at) не берутся.
select id, msg_no, segment_raw, send_date, send_time, client_settings,
       coalesce(nullif(title_edited, ''), title) as title,
       coalesce(nullif(body_edited, ''), body) as body,
       screen_link, button, coupon_text
from cvm.push
where promo_number = %(number)s
  and status = 'approved'
  and deleted_at is null
  and upper(coalesce(channel, 'PUSH')) = 'PUSH'
order by send_date, send_time, msg_no;
