"""Taller evaluable"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os.path
import string
import time
from itertools import groupby

def copy_raw_files_to_input_folder(n):
    """Funcion copy_files"""


    if os.path.exists("files/input"):
        for file in glob.glob("files/input/*"):
            os.remove(file)
        os.rmdir("files/input")
    os.makedirs("files/input")

    for file in glob.glob("files/raw/*"):
        for i in range(1, n + 1):
            with open(file, "r", encoding="utf-8") as f:
                with open(
                    f"files/input/{os.path.basename(file).split('.')[0]}_{i}.txt",
                    "w",
                    encoding="utf-8",
                ) as f2:
                    f2.write(f.read()) 

def load_input(input_directory):
    """Funcion load_input"""

    sequence = []
    files = glob.glob(f"{input_directory}/*")
    with fileinput.input(files=files) as f:
        for line in f:
            sequence.append((fileinput.filename(), line))
    return sequence

def line_preprocessing(sequence):
    """Line Preprocessing"""
    sequence = [
        (key, value.translate(str.maketrans("", "", string.punctuation)).lower())
        for key, value in sequence
    ]
    return sequence
  

def mapper(sequence):
    """Mapper"""
    return [(word, 1) for _, value in sequence for word in value.split()]

def shuffle_and_sort(sequence):
    """Shuffle and Sort"""
    return sorted(sequence, key=lambda x: x[0])

def reducer(sequence):
    """Reducer"""
    result = []
    for key, group in groupby(sequence, lambda x: x[0]):
        result.append((key, sum(value for _, value in group)))
    return result


def create_ouptput_directory(output_directory):
    """Create Output Directory"""

    if os.path.exists(output_directory):
        for file in glob.glob(f"{output_directory}/*"):
            os.remove(file)
        os.rmdir(output_directory)
    os.makedirs(output_directory)


def save_output(output_directory, sequence):
    """Save Output"""
    with open(f"{output_directory}/part-00000", "w", encoding="utf-8") as f:
        for key, value in sequence:
            f.write(f"{key}\t{value}\n")

def create_marker(output_directory):
    """Create Marker"""
    with open(f"{output_directory}/_SUCCESS", "w", encoding="utf-8") as f:
        f.write("")

def run_job(input_directory, output_directory):
    """Job"""
    sequence = load_input(input_directory)
    sequence = line_preprocessing(sequence)
    sequence = mapper(sequence)
    sequence = shuffle_and_sort(sequence)
    sequence = reducer(sequence)
    create_ouptput_directory(output_directory)
    save_output(output_directory, sequence)
    create_marker(output_directory)
    return

if __name__ == "__main__":

    copy_raw_files_to_input_folder(n=1000)

    start_time = time.time()

    run_job(
        "files/input",
        "files/output",
    )

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")