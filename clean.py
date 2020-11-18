from multiprocessing import Pool
from itertools import repeat
from subprocess import PIPE, Popen, STDOUT, TimeoutExpired
import logging
import time

logger = logging.getLogger("Clean")
logging.basicConfig(level=logging.INFO)


def cmd_bash(cmd,result=False,wait=False):
    timeout=20
    process_machine = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)

    if wait:
        process_machine.wait()

    try:
        stdout, stderr = process_machine.communicate(timeout=timeout)

        if result and stderr == "":
            out=stdout
            logger.debug(stdout.split("\n")[0])
            process_machine.kill()
            return 0, out

        elif stderr == "":
            process_machine.kill()
            return 0

        else:
            err = stderr.split('\n')[0]
            logger.error(f"ECHEC : {err}")
            process_machine.kill()
            return [1,err]


    except TimeoutExpired:
        process_machine.kill()
        logger.debug(f"{cmd} has Timeout")

def test_ssh(machine):
    cmd = f"ssh asenet@{machine} hostname"
    connect_cmd = f"ssh -o \'StrictHostKeyChecking=no\' asenet@{machine} hostname"
    return_code_ssh = cmd_bash(connect_cmd, result=True)
    logger.debug(return_code_ssh)

    if return_code_ssh :
        if return_code_ssh[0] == 0:
            return machine


def clean(machine,folder):
    cmd_clean = f"ssh -o \'StrictHostKeyChecking=no\' asenet@{machine} rm -rf {folder}"
    return_code_clean= cmd_bash(cmd_clean)

    return return_code_clean, machine

def main():
    folder = "/tmp/asenet/"
    start = time.time()

    with open('machines.txt') as f:
        machines_liste = f.read().splitlines()

    logger.info('Testing ssh....')
    with Pool() as p:
        up_machines = p.map(test_ssh,machines_liste)

    logger.info('Cleaning....')
    with Pool() as p:
        clean_files= p.starmap(clean,zip(up_machines,repeat(folder)))
    success_clean = [res[1] for res in clean_files if res[0] == 0]

    logger.info('Editing up machines txt....')
    with open('machines.txt', 'w') as f:
        f.write('\n'.join(f"{m}" for m in success_clean))

    point0 = time.time()
    elapsed = point0 - start
    logger.info(f' Clean took : {elapsed:.5f} seconds')

    print(f"Cleaning successfull on marchines {success_clean}")

if __name__ == '__main__':
   main()
