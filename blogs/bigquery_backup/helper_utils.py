#!/bin/env python3

import logging
import subprocess
import os
import json
import tempfile


def exec_shell_command(command):
    """
    Execute shell command and return output as a string
    :param command:  1D array of strings e.g. ['ls', '-l']
    :return: output of command
    """
    logging.info(command)
    process = subprocess.run(command, stdout=subprocess.PIPE, universal_newlines=True)
    return process.stdout


def exec_shell_pipeline(pipeline):
    """
    Execute shell pipeline and return output as a string
    :param pipeline:  2D array of strings e.g. [ ['ls', '-l'], ['grep', 'xyz'] ]
    :return: output of command
    """
    if len(pipeline) == 1:
        return exec_shell_command(pipeline[0])

    prev = subprocess.Popen(pipeline[0], stdout=subprocess.PIPE)
    for i in range(1, len(pipeline)):
        logging.info('   '*i + ' '.join(pipeline[i]))
        curr = subprocess.Popen(pipeline[i], stdin=prev.stdout, stdout=subprocess.PIPE, universal_newlines=True)
        prev.stdout.close() # allow curr to receive a SIGPIPE if prev exists
        prev = curr
    return curr.communicate()[0]


def write_json_string(json_string, output_gcsfile):
    """
    Write a json string to a file in Google Cloud Storage in pretty format
    :json_string: string to write out
    :output_gcsfile:  URL starting with gs://
    """
    schema = json.loads(json_string)
    logging.info(schema)
    fd, fname = tempfile.mkstemp()
    with open(fname, 'w') as ofp:
        json.dump(schema, ofp, sort_keys=False, indent=2)
    os.close(fd)
    exec_shell_command(['gsutil', 'cp', fname, output_gcsfile])

def read_json_string(gcsfile):
    """
    Read a json string to a file from Google Cloud Storage
    :gcsfile:  URL starting with gs://
    """
    json_string = exec_shell_command(['gsutil', 'cat', gcsfile])
    return json.loads(json_string)

