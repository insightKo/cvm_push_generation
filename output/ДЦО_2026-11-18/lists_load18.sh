set -euo pipefail
# ДЦО 18.11: транспортная таблица раздачи (как lists.dco_20261030) — читает связанный сервер CVM_PORTAL под ролью mci
docker exec -i cvm-lists-db sh -c 'psql -U "$POSTGRES_USER" -d cvm_lists -v ON_ERROR_STOP=1 -q' <<'SQL'
drop table if exists lists.dco_20261118;
create table lists.dco_20261118 (id_promo int not null, id_contact int not null primary key);
alter table lists.dco_20261118 owner to lists_owner;
grant select on lists.dco_20261118 to mci;
comment on table lists.dco_20261118 is 'ДЦО ср 18.11.2026: раздача 101579–101643 (клиент один раз), читает скрипт обменника CVM_DCO_20261118; удалить после записи в I_PROMO_OFFER';
SQL
docker exec cvm-api cat /tmp/dco1811/dco18.csv \
  | docker exec -i cvm-lists-db sh -c 'psql -U "$POSTGRES_USER" -d cvm_lists -v ON_ERROR_STOP=1 -q -c "\copy lists.dco_20261118 (id_promo, id_contact) from stdin with (format csv)"'
docker exec -i cvm-lists-db sh -c 'psql -U "$POSTGRES_USER" -d cvm_lists -At -F " "' <<'SQL'
select 'всего', count(*), count(distinct id_contact), count(distinct id_promo) from lists.dco_20261118;
select id_promo, count(*) from lists.dco_20261118 group by 1 order by 1;
select 'mci select', has_table_privilege('mci', 'lists.dco_20261118', 'SELECT');
SQL
