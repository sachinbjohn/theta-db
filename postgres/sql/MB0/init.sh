#!/bin/bash
# Absolute path to this script, e.g. /home/user/bin/foo.sh
SCRIPT=$(readlink -f "$0")
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(dirname "$SCRIPT")

dbname="MB0"
dropdb $dbname -U postgres -h localhost --if-exists
createdb $dbname -U postgres -h localhost 
psql -f $SCRIPTPATH/static.sql -d $dbname -U postgres -h localhost
#psql -f $SCRIPTPATH/gen.sql -d $dbname -U postgres -h localhost
psql -f $SCRIPTPATH/expt1.sql -d $dbname -U postgres -h localhost 
