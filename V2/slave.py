import sys
import re
import os
import socket
import glob
import hashlib
from subprocess import PIPE, Popen, STDOUT, TimeoutExpired
from multiprocessing import Pool
import logging
import time

logger = logging.getLogger(f"Slave-{socket.gethostname()}")
logging.basicConfig(level=logging.INFO)

#GLOBAL VARIABLES
wc_dict = {}
shuffle_dir = "/tmp/asenet/shuffles/"
shuffle_received_dir = "/tmp/asenet/shufflesreceived/"
maps_folder= "/tmp/asenet/maps/"
reduce_folder = '/tmp/asenet/reduces/'


def hash_function(word):
    word_b = bytes(word,'utf-8')
    hashed_word = int(hashlib.sha1(word_b).hexdigest(), 16) % (10 ** 10)
    return hashed_word

def shuffle(filename, machines):
    with open(filename, encoding='utf8') as f:
        words = f.read().split()

    with Pool() as p:
        p.map(create_shuffle, words)

    shuffle_files = list(map(os.path.basename,glob.glob(shuffle_dir + "*.txt")))

    with Pool() as p:
        p.map(scp_shuffle_file, shuffle_files)

def create_shuffle(word):
    receiver = machines[hash_function(word) % len(machines)]
    with open(f'/tmp/asenet/shuffles/{receiver}_{socket.gethostname()}.txt', 'a') as f:
        f.write(f'{word} ')
    f.close()

def cmd_bash(cmd, timeout=10):
    process_machine = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, text=True)
    # process_machine.wait()

    try:
        _, stderr = process_machine.communicate(timeout=timeout)
        if stderr == "":
            process_machine.kill()
            return 0

        else:
            process_machine.kill()
            logger.error(f"echec {cmd}")

    except TimeoutExpired:
        process_machine.kill()
        logger.error(f"Timeout {cmd}")

def map_txt_words(filename):
    with open(filename) as file:
        data=[[word] for line in file for word in line.split()]

    return data

def scp_shuffle_file(filepath):
    filename = os.path.basename(filepath)
    cmd_scp = f"scp /tmp/asenet/shuffles/{filename} asenet@{filename.split('_')[0]}:/tmp/asenet/shufflesreceived"

    return_code_scp = cmd_bash(cmd_scp)

    return return_code_scp

def reduce():
    files = list(map(os.path.basename,glob.glob(shuffle_received_dir + "*.txt")))
    reduced_count = {}
    for file in files :
        with open(f'{shuffle_received_dir}/{file}', encoding='utf8') as f:
            read_shuffle = f.read()
        for word in read_shuffle.split(' '):
            reduced_count[word] = reduced_count.get(word, 0) + 1
    return reduced_count


def main():

    ##### MAP #####
    if sys.argv[1] == "0" :
        logger.debug("Map Start")

        #Create all necessary directories
        cmd_bash(f"mkdir -p {maps_folder} | mkdir -p {shuffle_dir} | mkdir -p {shuffle_received_dir} | mkdir -p {reduce_folder} ")
        logger.debug("All folders created")


        filename = sys.argv[2]
        filepath = "/tmp/asenet/splits/" + filename
        words_map =  map_txt_words(filepath)

        split_nb = re.search(r'\d+',filename).group(0)

        with open(f'/tmp/asenet/maps/UM{split_nb}.txt', "w+") as f:
            for word_map in words_map:
                [f.write(word+"\n") for word in word_map]

                                                                                                                                                                                                                                                                                                                                                                                                    
    ##### Shuffle #####
    if sys.argv[1] == "1":
        start = time.time()
        logger.debug("Shuffle Start")
        filename = glob.glob(maps_folder + "*.txt")

        global machines
        machines = [m.replace('\'', '') for m in list(sys.argv[2].strip('[]').split(', '))]

        #Open Split Files
        logger.debug("proccessing Split files...")
        shuffle(filename[0], machines)
        point1 = time.time()
        elapsed = point1 - start
        logging.info(f' Shuffle files created and sent in : {elapsed:.5f} secondes')


    ##### REDUCE #####
    if sys.argv[1] == "2":
        logger.debug("Reduce Start")

        results_reduce=reduce()

        print(results_reduce)


if __name__ == '__main__':
    main()
