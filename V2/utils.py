from subprocess import PIPE, Popen, STDOUT, TimeoutExpired
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def cmd_bash(cmd,result=False,wait=False, timeout=60):
    process_machine = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)

    if wait:
        process_machine.wait()

    try:
        stdout, stderr = process_machine.communicate(timeout=timeout)

        if result and stderr == "":
            out = stdout
            process_machine.kill()
            return 0, out

        elif stderr == "":
            process_machine.kill()
            return 0

        else:
            logger.debug(f"echec {cmd}")
            logger.debug(f"stderr {stderr}")
            process_machine.kill()
            out = stdout
            return 1, out

    except TimeoutExpired:
        process_machine.kill()
        print(cmd, "Timeout")


def make_dir(machine, folder):
    cmd_dir = f"ssh asenet@{machine} mkdir -p {folder}"
    return_code_dir = cmd_bash(cmd_dir)

    return return_code_dir, machine

def scp_file(folder, machine, file):
    cmd_scp = f"scp -r -p {folder}{file} asenet@{machine}:{folder} "
    return_code_scp = cmd_bash(cmd_scp, timeout=300)

    return return_code_scp, machine
