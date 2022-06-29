#!/bin/bash

set -e

# insert.sh, but the updating process of the changesets is also included,
# that only one entry in cronjobs is necessary. REPDIR has to be created and
# sequence.state added in it. ope and changesetmd.py must either be added to the
# path variable or the path must be added here.

DIRECTORY=$(cd `dirname $0` && pwd)

REPDIR="/var/cache/osmhistory-replication"  # postgresql has private tmp enabled and thus cannot read from our tmp.
CHANGESETMD="$DIRECTORY/ChangesetMD/changesetmd.py"

DB_NAME=${POSTGRESQL_DATABASE:-"osmhistory"}
DB_USER=${POSTGRESQL_POSTGRES_SUPERUSER:-"postgres"}
DB_PASSWORD=${POSTGRESQL_POSTGRES_PASSWORD:-"postgres"}
DB_HOST=${POSTGRESQL_HOST:-"localhost"}
DB_PORT=${POSTGRESQL_PORT:-"5432"}

CHANGESET_OPTIONS="-d ${DB_NAME} -p ${DB_PASSWORD} -H ${DB_HOST} -u ${DB_USER} -P ${DB_PORT}"
PSQL_CONNECTION="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}"

SEQUENCE_STATE=${SEQUENCE_STATE_FILE:-"sequence.state"}

if [ x"${INSIDE_DOCKER}" == "x" ]; then
    exec &> insert_logs.txt
fi

osm_objects() {
    echo "Ensuring $REPDIR exists, creating it of not"
    mkdir -p $REPDIR
    echo "Ensuring changes.osm.gz is not here anymore"
    rm -f $REPDIR/changes.osm.gz

    echo "Saving Changedata in $REPDIR/changes.osm.gz (using sequence.state)"
    pyosmium-get-changes -f ${SEQUENCE_STATE} -o $REPDIR/changes.osm.gz
    echo "Creating Insertfiles from $REPDIR/changes.osm.gz in $REPDIR/"
    ope -H $REPDIR/changes.osm.gz $REPDIR/nodes=n%I.v.d.c.t.i.T.Gp $REPDIR/ways=w%I.v.d.c.t.i.T.N. $REPDIR/relations=r%I.v.d.c.t.i.T.M. $REPDIR/users=u%i.u.
    echo "Writing Data to Database $DB"
    psql ${PSQL_CONNECTION} -c "\\copy \"temp_nodes\" from '$REPDIR/nodes.pgcopy'"
    psql ${PSQL_CONNECTION} -c "\\copy \"temp_ways\" from '$REPDIR/ways.pgcopy'"
    psql ${PSQL_CONNECTION} -c "\\copy \"temp_relations\" from '$REPDIR/relations.pgcopy'"
    psql ${PSQL_CONNECTION} -c "\\copy \"temp_users\" from '$REPDIR/users.pgcopy'"
    psql ${PSQL_CONNECTION} -f import.sql
    echo "Starting osm_pg_db_clipper.py"
    python osm_pg_db_clipper.py ${CHANGESET_OPTIONS} -b borders.geojson -f $REPDIR/changes.osm.gz
    echo "Deleting changefiles in $REPDIR/"
    rm $REPDIR/changes.osm.gz
    rm $REPDIR/ways.*
    rm $REPDIR/nodes.*
    rm $REPDIR/relations.*
    rm $REPDIR/users.*
}

changesets() {
    echo "Save new Changesets into Database"
    python $CHANGESETMD ${CHANGESET_OPTIONS} -r -g
    echo "Deleting Changesets outside Switzerland"
    python osm_changeset_deleter.py ${CHANGESET_OPTIONS} -b borders.geojson
}

main() {
    osm_objects
    changesets
}

main
