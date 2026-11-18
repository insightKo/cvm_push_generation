\pset format unaligned
\pset fieldsep '\t'
\pset footer off
-- Кандидаты на 18.11 по формам ДЦО и «Тотал Промо» вместе.
-- T — трафик: «ПО КАРТЕ» (в т. ч. пометка КДД в N+1) и «СКИДКА» в недельных/многонедельных акциях;
-- C — на чек: «СКИДКА НА ВТОРОЙ…» (в т. ч. асс. аналог).
-- Не берём: акции лояльности (игрушки, наклейки), стойки, ТУК, промо-наборы — не для пуша о скидке на товар.
with r as (
  select d.sku, d.district, upper(btrim(d.mechanic)) as mech, coalesce(d.n_plus_1, '') as n1, d.discount,
         d.price_regular, d.price_promo, d.start_date, d.end_date, d.name,
         coalesce(d.form, '') as form, coalesce(d.total_no, '') as total_no,
         coalesce(d.promo_type, '') as ptype, coalesce(d.promo_subtype, '') as psub
    from cvm.dco_row d
   where d.start_date <= date '2026-11-18' and d.end_date >= date '2026-11-18'
     and d.district in ('ЦФО', 'СЗФО')
     and coalesce(d.promo_type, '') not ilike 'Акция лояльности%'
     and coalesce(d.promo_type, '') not in ('Стойки', 'Товар У Кассира (ТУК)')
     and (upper(btrim(d.mechanic)) in ('ПО КАРТЕ', 'СКИДКА') or upper(btrim(d.mechanic)) like 'СКИДКА НА ВТОРОЙ%')
)
select case when r.mech like 'СКИДКА НА ВТОРОЙ%' then 'C' else 'T' end as typ, r.sku, max(r.name) as name,
       l.cat, c.name as catname, l.grp, l.shops, l.checks,
       string_agg(r.district || ':' || r.mech || ' ' || r.n1 || ' ' || coalesce(round(r.discount * 100)::text, '?') || '% '
                  || to_char(r.start_date, 'DD.MM') || '-' || to_char(r.end_date, 'DD.MM') || ' '
                  || coalesce(r.price_regular::text, '') || '>' || coalesce(r.price_promo::text, '')
                  || ' [' || case when r.form <> '' then 'ф' else '' end || case when r.total_no <> '' then 'Т' else '' end
                  || ' ' || r.total_no || ' ' || r.ptype || ' ' || r.psub || ']', ' | ' order by r.district, r.start_date) as det
  from r
  left join cvm.live_assort l on l.sku = r.sku
  left join cvm.cvm_category c on c.id = l.cat
 group by 1, 2, 4, 5, 6, 7, 8
 order by 1, l.grp, l.cat, l.checks desc nulls last;
