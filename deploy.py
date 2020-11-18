import subprocess
from itertools import repeat
from multiprocessing import Pool
import logging

logger = logging.getLogger("Deploy")
logging.basicConfig(level=logging.INFO)

def cmd_bash(cmd):
    try:
        proc = subprocess.run(cmd, shell = True, timeout=10)

    except subprocess.TimeoutExpired :
        logger.info(f"Timeout Expired for {cmd}")
    else:
        return proc.returncode

# def test_ssh(machine):
#     connect_cmd = f"ssh -o \'StrictHostKeyChecking=no\' asenet@{machine} hostname"
#     return_code = cmd_bash(connect_cmd)
#     if return_code == 0:
#         return machine

def deploy(machine,folder):
    cmd_dir = f"ssh asenet@{machine} mkdir -p {folder}"
    return_code_dir = cmd_bash(cmd_dir)

    if return_code_dir == 0:
        cmd_scp = f"scp -r -p slave.py asenet@{machine}:{folder} "
        return_code_scp = cmd_bash(cmd_scp)

    else:
        return_code_scp=1

    return return_code_scp, machine

def main():
    folder = "/tmp/asenet/"

    with open('machines.txt') as f:
        up_machines = f.read().splitlines()

    logger.info("Deploying files ...")
    with Pool() as p:
        deploy_files= p.starmap(deploy,zip(up_machines,repeat(folder)))
    success_deploy = [res[1] for res in deploy_files if res[0] == 0]

    with open(f'/tmp/asenet/machines.txt', "w") as f:
        [f.write(machine+"\n") for machine in success_deploy]


    print(f"Deploy ok on marchines {success_deploy}")


if __name__ == '__main__':
    main()