import glob
import os
from multiprocessing import Pool
from itertools import repeat
import time
from utils import cmd_bash, make_dir, scp_file
import sys
import ast
import logging
from math import *

logging.basicConfig(level=logging.INFO)

##Gloal variables
root_folder = "/tmp/asenet/"
splits_folder = "/tmp/asenet/splits/"
maps_folder = "/tmp/asenet/maps/"
shuffle_received_dir = "/tmp/asenet/shufflesreceived/"
reduces_folder = "/tmp/asenet/reduces/"
char_chunk = 20
result_file = '/tmp/asenet/results.txt'


def get_txt_words(filename):

    with open(filename) as file:
        number_of_characters = len(file.read())

    with open(filename) as file:
        words=[]
        for line in file:
            for word in line.split():
                words.append(word)

    unique_words = len(set(words))
    txt_len = len(words)

    return txt_len, unique_words, number_of_characters

def create_splits(filename, char_chunk, folder):

    with open(filename, encoding='utf8') as f:
        raw_file = f.read()

    splits_list = []
    file_nb_char = len(raw_file)
    split_start = 0

    #Make list of chunk size char
    while file_nb_char > char_chunk:
        split_end = split_start + char_chunk

        #When split end in a middle of word look for last space
        while raw_file[split_end] != " ":
            split_end -= 1
        splits_list.append(raw_file[split_start:split_end])
        file_nb_char -= split_end - split_start
        split_start = split_end

    splits_list.append(raw_file[split_start:len(raw_file)])

    #Create split files
    for i, split in enumerate(splits_list):
        with open(f'{folder}/S{str(i)}.txt', 'w') as f:
            f.write(split)
            f.close()

    return splits_list


def launch_map(machine, file):
    cmd_map=f"ssh asenet@{machine} python3 /tmp/asenet/slave.py 0 {file}"
    return_code_slave = cmd_bash(cmd_map)

    return return_code_slave, machine, file


def launch_shuffle(machine, machines):
    cmd_shuffle=f"ssh asenet@{machine} 'python3 /tmp/asenet/slave.py 1 \"{machines}\"'"
    logging.debug(cmd_shuffle)
    return_code_slave = cmd_bash(cmd_shuffle,wait=True)

    return return_code_slave, machine

def distribute_shuffle(machine):
    connect_cmd = f"ssh -o \'StrictHostKeyChecking=no\' asenet@{machine}"
    send_shuffle_cmd = f' python3 /tmp/asenet/slave.py 3'
    return_code_shuffle = cmd_bash(connect_cmd + send_shuffle_cmd,wait=False, timeout=1200)

    return return_code_shuffle, machine

def launch_reduce(machine):
    cmd_reduce=f"ssh asenet@{machine} python3 /tmp/asenet/slave.py 2"
    logging.debug(cmd_reduce)
    return_cmd = cmd_bash(cmd_reduce, result=True)
    return_code_slave = return_cmd[0]
    return_result = return_cmd[1]
    logging.debug(f"reduce done on machine {machine}  RETURN, {return_cmd}")

    return return_code_slave, machine, return_result

def show_results(reduce_results):
    # word_count_final=[]
    # for return_data in reduce_results[1]:
    #     result = ast.literal_eval(return_data[2])
    #     word_count_final.append(result)
    #
    # flat_list = [item for sublist in word_count_final for item in sublist]
    # dict_agg={}
    # for i in flat_list:
    #     for k, v in i.items():
    #         if dict_agg.get(k,0) != 0:
    #             dict_agg[k] += v
    #         else:
    #             dict_agg[k] = v

    dict_agg={}
    for return_data in reduce_results[1]:
        result = ast.literal_eval(return_data[2])
        dict_agg.update(result)

    print("=== TOP 10 wordcount ===")
    for key, value in sorted(dict_agg.items(), key=lambda item: item[1], reverse=True)[0:10]:
        print(key, value)

    return dict_agg

def multiprocess(func, *arg, action=None):

    with Pool() as p:
        cmd = p.starmap(func,zip(*arg))
    p.join()
    success = [res[1] for res in cmd if res[0] == 0]
    logging.debug(f"{action} ok on marchines {success}")

    return success, cmd


def main():
    start = time.time()

    if len(sys.argv) > 3:
        print('usage: ./master.py {--deploy} file')
        sys.exit(1)

    logging.debug("test debug")

    ## Launch clean and Deploy (optionnal)
    if sys.argv[1] == '--deploy':
        logging.info(" START Clean and deploy")
        logging.debug(" launch Clean")
        cmd_clean="python3 ../clean.py"
        return_clean = cmd_bash(cmd_clean, result=True, wait=False, timeout=100)
        logging.debug(return_clean)

        logging.debug(" launch deploy")
        cmd_deploy="python3 ../deploy.py"
        return_deploy = cmd_bash(cmd_deploy,result=True, wait=False, timeout=100)
        logging.debug(return_deploy)

        #if return_clean[0] == 0 and return_deploy[0] == 0 :
        filename = sys.argv[2]
        point0 = time.time()
        elapsed = point0 - start
        start = point0
        logging.info(f' Clean and deploy took : {elapsed:.5f} seconds')
    else:
        filename = sys.argv[1]

    filepath= f"../input/{filename}"

    ## Count how many words
    txt_len, unique_words, number_of_characters = get_txt_words(filepath)
    logging.info(f" Text has {number_of_characters} caracters, {txt_len} words, {unique_words} different words")

    ## Remove existing splits
    cmd_del_splits=f"rm -rf /tmp/asenet/splits/*"
    return_code_del_splits = cmd_bash(cmd_del_splits)
    if return_code_del_splits == 0:
        logging.info(f" Removed splits from local folder")

    ## 1 split per up machine
    with open('/tmp/asenet/machines.txt') as f:
        machines_list = f.read().splitlines()

    logging.info(f' NB machines used : {len(machines_list)}')
    logging.debug('machines liste: '+str(machines_list))

    # Making as much chunk as machines (adding 1.5 due to chunk func that can go back if chunck in the middle of a word)
    char_chunk = ceil(number_of_characters / len(machines_list)*1.5)

    ## Create Splits Files:
    splits = create_splits(filepath, char_chunk, splits_folder)
    logging.debug(f'splits {splits}')
    logging.info(f" Created {len(splits)} splits of {char_chunk} char size from text")
    point1 = time.time()
    elapsed = point1 - start
    logging.info(f' Splits Creation took : {elapsed:.5f} secondes')

    ## Assign each split to a machine
    split_files = list(map(os.path.basename, glob.glob(splits_folder + "*.txt")))
    file_machine_assign = {file : machines_list[split_files.index(file) % len(machines_list)] for file in split_files}

    ## Create split Dir before scp
    multiprocess(make_dir, machines_list, repeat(splits_folder), action="Create Splits Folder")
    point2 = time.time()
    elapsed = point2 - point1
    logging.info(f' Folder Created in: {elapsed:.5f} secondes')

    ## Send Machine file
    multiprocess(scp_file, repeat(root_folder), file_machine_assign.values(), repeat('machines.txt') , action="Send machines.txt")
    point2b = time.time()
    elapsed = point2b - point2
    logging.info(f' Send machine list: {elapsed:.5f} secondes')

    ## SCP split file
    return_scp = multiprocess(scp_file, repeat(splits_folder), file_machine_assign.values(), file_machine_assign.keys(), action="Splits upload")
    machines_working = [machine[1] for machine in return_scp[1] if machine[0] == 0]
    logging.debug(f"Return scp {return_scp}")
    logging.info(f"Sent split {file_machine_assign}")
    logging.debug(f"marchines working {machines_working}")
    point3 = time.time()
    elapsed = point3 - point2b
    logging.info(f' Splits files sent in: {elapsed:.5f} secondes')

    ## Launch map
    multiprocess(launch_map, machines_working, split_files, action="Launch map")
    point4 = time.time()
    elapsed = point4 - point3
    logging.info(f' MAPS FINISHED in: {elapsed:.5f} secondes')

    ##Make and distribute Shuffle files
    multiprocess(launch_shuffle, machines_working, repeat(machines_working), action="Shuffling")
    point5 = time.time()
    elapsed = point5 - point4
    logging.info(f' SHUFFLE FINISHED in : {elapsed:.5f} secondes')

    ##Launch Reduce
    reduce_results = multiprocess(launch_reduce, machines_working, action="Reduce")
    logging.debug(f"Reduce results {reduce_results}")
    point7 = time.time()
    elapsed = point7 - point5
    logging.info(f' REDUCE FINISHED in : {elapsed:.5f} secondes')

    ##Gather results
    word_count_final = show_results(reduce_results)
    logging.debug(f"word final {word_count_final}")

    end = time.time()
    logging.info(f"MAP/REDUCE in {(end - start)} s")
    logging.debug(f"END")


if __name__ == '__main__':
   main()
