#!/bin/env python3

import logging
import argparse
import tempfile
import json
import os

from helper_utils import exec_shell_command, exec_shell_pipeline, read_json_string


def restore_table(fromdir, todataset):
    """
    Restore schema & data from GCS to BigQuery table
    :param todir: GCS input directory gs://..../dataset/tablename/
    :param todataset: BigQuery dataset name
    """

    # start to create load command
    load_command = [
        'bq', 'load',
        '--source_format', 'AVRO',
    ]

    # get table definition
    tbldef = read_json_string(os.path.join(fromdir, 'tbldef.json'))

    if 'timePartitioning' in tbldef:
        load_command += [
            '--time_partitioning_expiration', tbldef['timePartitioning']['expirationMs'],
            '--time_partitioning_field', tbldef['timePartitioning']['field'],
            '--time_partitioning_type', tbldef['timePartitioning']['type'],
        ]

    if 'rangePartitioning' in tbldef:
        load_command += [
            '--range_partitioning',
            '{},{},{},{}'.format(
                tbldef['rangePartitioning']['field'],
                tbldef['rangePartitioning']['range']['start'],
                tbldef['rangePartitioning']['range']['end'],
                tbldef['rangePartitioning']['range']['interval']
            )
        ]

    if 'clustering' in tbldef:
        load_command += [
            '--clustering_fields',
            ','.join(tbldef['clustering']['fields'])
        ]

    # write schema to a temporary file
    schema = tbldef['schema']['fields']  # array of dicts
    fd, schema_file = tempfile.mkstemp()
    with open(schema_file, 'w') as ofp:
        json.dump(schema, ofp, sort_keys=False, indent=2)
    os.close(fd)
    load_command += [
        '--schema', schema_file,
    ]

    # load the data into BigQuery
    table_name = tbldef['tableReference']['tableId']
    load_command += [
        '{}.{}'.format(todataset, table_name),
        os.path.join(fromdir, 'data_*.avro')
    ]

    exec_shell_command(load_command)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Restore a BigQuery dataset from Google Cloud Storage'
    )
    parser.add_argument('--output', required=True, help='destination dataset')
    parser.add_argument('--input', required=True, help='GCS URL gs://..../dataset/tablename/ ')
    parser.add_argument('--quiet', action='store_true', help='Turn off verbose logging')

    args = parser.parse_args()
    if not args.quiet:
        logging.basicConfig(level=logging.INFO)

    restore_table(args.input, args.output)

