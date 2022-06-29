from pathlib import Path
import yaml
import os
from yaml import CLoader as Loader

db = os.environ['POSTGRESQL_DATABASE']
user = os.environ['POSTGRESQL_USERNAME']
host = os.environ['POSTGRESQL_HOST']
pw = os.environ['POSTGRESQL_PASSWORD']
port = os.environ.get('POSTGRESQL_PORT', 5432)

history_db_osh_pbf_url = 'https://minios3.sifs0005.infs.ch/osmhistory-db/history-switzerland.osh.pbf'
state_url = "https://planet.osm.org/replication/changesets/state.yaml"
changesets_latest = "https://planet.osm.org/planet/changesets-latest.osm.bz2"

inital_data_folder = '/var/cache/osmhistory-replication/initial-data'


def _run_command(cmd):
    assert os.system(cmd) == 0


def get_latest_osm_sequence_number():
    if not Path(f'{inital_data_folder}/state.yml').exists():
        _run_command(f"wget -O {inital_data_folder}/state.yml {state_url}")

    with open(f'{inital_data_folder}/state.yml', 'r') as f:
        state = yaml.load(f.read(), Loader=Loader)

    return int(state['sequence'])


def load_initial_changesets_for_switzerland():
    CHANGESET_FILE = f'{inital_data_folder}/changesets-switzerland.osm.bz2'
    
    if not Path(f'{inital_data_folder}/changesets.osm.loaded').exists():
        
        if not Path(f'{inital_data_folder}/changesets.osm.bz2').exists():
            _run_command(f"wget -O {inital_data_folder}/changesets.osm.bz2 {changesets_latest}")
        
        if not Path(f'{CHANGESET_FILE}').exists():
            bbox_switzerland = '8.6462402,47.193709,8.8275146,47.283218'
            _run_command(f"osmium changeset-filter --bbox={bbox_switzerland} {inital_data_folder}/changesets.osm.bz2 --output={CHANGESET_FILE}")

        truncate_tables = Path(f'{inital_data_folder}/changesetmd.ran').exists()
        postgres_admin_user = os.environ.get('POSTGRESQL_POSTGRES_SUPERUSER', 'postgres')
        postgres_admin_pw = os.environ['POSTGRESQL_POSTGRES_PASSWORD']

        flags = f'-c -g'

        if truncate_tables:
            flags+= ' -t'

        _run_command(f'python /code/ChangesetMD/changesetmd.py -d {db} -p {postgres_admin_pw} -H {host} -u {postgres_admin_user} -P {port} {flags} -f {CHANGESET_FILE}')

        seq_nr = get_latest_osm_sequence_number()
        update_sequence_number(seq_nr)

        _run_command(f'touch {inital_data_folder}/changesetmd.ran')
        _run_command(f'touch {inital_data_folder}/changesets.osm.loaded')


def update_sequence_number(sequence_number):
    postgres_admin_user = os.environ.get('POSTGRESQL_POSTGRES_SUPERUSER', 'postgres')
    postgres_admin_pw = os.environ['POSTGRESQL_POSTGRES_PASSWORD']
    _run_command(f"psql postgresql://{postgres_admin_user}:{postgres_admin_pw}@{host}:{port}/{db} -c 'update osm_changeset_state set last_sequence = {sequence_number};'")


def setup_osm_history_db_for_switzerland():
    if not Path(f'{inital_data_folder}/history_db_setup.ran').exists():
        postgres_admin_user = os.environ.get('POSTGRESQL_POSTGRES_SUPERUSER', 'postgres')
        postgres_admin_pw = os.environ['POSTGRESQL_POSTGRES_PASSWORD']
        pg_connection = f"postgresql://{postgres_admin_user}:{postgres_admin_pw}@{host}:{port}/{db}"
        
        if not Path(f'{inital_data_folder}/history-switzerland.osh.pbf').exists():
            _run_command(f"wget -O {inital_data_folder}/history-switzerland.osh.pbf {history_db_osh_pbf_url}")
        
        if not Path(f'{inital_data_folder}/sequence.state').exists():
            _run_command(f"pyosmium-get-changes -O {inital_data_folder}/history-switzerland.osh.pbf -f {inital_data_folder}/sequence.state")
        
        _run_command(f"ope -H {inital_data_folder}/history-switzerland.osh.pbf nodes=n%I.v.d.c.t.i.T.Gp ways=w%I.v.d.c.t.i.T.N. relations=r%I.v.d.c.t.i.T.M. users=u%i.u.")
        _run_command(f"psql {pg_connection} -f users.sql")
        _run_command(f"psql {pg_connection} -f relations.sql")
        _run_command(f"psql {pg_connection} -f ways.sql")
        _run_command(f"psql {pg_connection} -f nodes.sql")
        _run_command(f"psql {pg_connection} -f db_setup.sql")
        _run_command(f"python osm_pg_db_clipper.py --user {postgres_admin_user} --password {postgres_admin_pw} --host {host} --database {db} --port {port} -b borders.geojson -f {inital_data_folder}/history-switzerland.osh.pbf")
        _run_command(f'touch {inital_data_folder}/history_db_setup.ran')


def setup_history_db_view():
    if not Path(f'{inital_data_folder}/history_db_view_setup.ran').exists():
        postgres_admin_user = os.environ.get('POSTGRESQL_POSTGRES_SUPERUSER', 'postgres')
        postgres_admin_pw = os.environ['POSTGRESQL_POSTGRES_PASSWORD']
        pg_connection = f"postgresql://{postgres_admin_user}:{postgres_admin_pw}@{host}:{port}/{db}"

        _run_command(f"psql {pg_connection} -f 00_create_nwr_change_view.sql")
        _run_command(f'touch {inital_data_folder}/history_db_view_setup.ran')


if __name__ == '__main__':
    # ensure inital_data_folder exists
    _run_command(f'mkdir -p {inital_data_folder}')
    changesets_loaded = Path(f'{inital_data_folder}/changesets.osm.loaded').exists()
    data_inserted = Path(f'{inital_data_folder}/history_db_setup.ran').exists()
    view_inserted = Path(f'{inital_data_folder}/history_db_view_setup.ran').exists()

    initialization_done = changesets_loaded and data_inserted and view_inserted

    # force = False
    # TODO: enable forced insert of data
    # if force:
    #     _run_command(f'rm -f {inital_data_folder}/changesetmd.ran {inital_data_folder}/changesets.osm.loaded {inital_data_folder}/changesets-history.osh.bz2 {inital_data_folder}/changesets.osm.bz2 {inital_data_folder}/state.yml')

    if not changesets_loaded:
        get_latest_osm_sequence_number()
        load_initial_changesets_for_switzerland()

    if not data_inserted:
        setup_osm_history_db_for_switzerland()

    if not view_inserted:
        setup_history_db_view()
