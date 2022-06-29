import os
from subprocess import run
from time import sleep

RUN_AFTER_X_SLEEP_MINUTES = os.environ.get('RUN_AFTER_X_SLEEP_MINUTES', 10)

def _run_command(cmd):
    try:
        print(run(cmd, shell=True, capture_output=True, check=True))
    except Exception as exc:
        print(exc)
        raise


def run_forever():
    _run_command('mkdir -p /var/cache/osmhistory-replication')

    while True:
        _run_command('./insert_expanded.sh')
        print(f'run done, sleeping for {RUN_AFTER_X_SLEEP_MINUTES} minutes')
        sleep(RUN_AFTER_X_SLEEP_MINUTES * 60)


if __name__ == '__main__':
    run_forever()
