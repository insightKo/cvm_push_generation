-- База списков: итог заведения одного объекта (done / error). error не затирает уже записанный done.
insert into lists.manzana as m (id_promo, start_date, kind, item, name, external_id, guid, rows, status, detail, at)
values (%(number)s, %(start_date)s, %(kind)s, %(item)s, %(name)s, %(external_id)s, %(guid)s::uuid, %(rows)s, %(status)s,
        %(detail)s, now())
on conflict (id_promo, start_date, kind, item) do update
   set name = excluded.name, external_id = excluded.external_id, guid = excluded.guid, rows = excluded.rows,
       status = excluded.status, detail = excluded.detail, at = excluded.at
 where m.status = 'error' or excluded.status = 'done';
