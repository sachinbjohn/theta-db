export PGPASSWORD="root"
dbname="vwap2"
dropdb $dbname -U postgres --if-exists
createdb $dbname -U postgres
psql -f vwap2static.sql -d $dbname -U postgres
psql -f vwap2gen.sql -d $dbname -U postgres
psql -f expt1.sql -d $dbname -U postgres
