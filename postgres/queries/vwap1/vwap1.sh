export PGPASSWORD="root"
dbname="vwap1"
dropdb $dbname -U postgres --if-exists
createdb $dbname -U postgres
psql -f vwap1static.sql -d $dbname -U postgres
psql -f vwap1gen.sql -d $dbname -U postgres
psql -f expt1.sql -d $dbname -U postgres
