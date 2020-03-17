#!/bin/env python3

import logging
import subprocess
import argparse
import json
import tempfile
import os

from helper_utils import exec_shell_command, exec_shell_pipeline

def backup_table(dataset, tablename, todir, schemaonly):
    """
    Store schema & data in table to GCS
    :param dataset: BigQuery dataset name
    :param tablename: BigQuery table name
    :param todir: GCS output prefix
    :param schemaonly: don't export data, just the schema
    :return: None
    """

    # write schema to GCS
    schema = exec_shell_command(['bq', 'show', '--schema', '{}.{}'.format(dataset, tablename)])
    schema = json.loads(schema)
    logging.info(schema)
    fd, fname = tempfile.mkstemp()
    with open(fname, 'w') as ofp:
        json.dump(schema, ofp, sort_keys=False, indent=2)
    os.close(fd)
    output_schema_name = os.path.join(todir,
                                      '{}/{}_schema.json'.format(dataset, tablename)
                                      )
    exec_shell_command(['gsutil', 'cp', fname, output_schema_name])

    if not schemaonly:
        # also back up the data
        output_data_name = os.path.join(todir,
                                        '{}/{}_data.avro'.format(dataset, tablename))
        _ = exec_shell_command([
            'bq', 'extract',
            '--destination_format=AVRO',
            '{}.{}'.format(dataset, tablename),
            output_data_name
        ])





if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Backup a BigQuery dataset (schema + data) to Google Cloud Storage'
    )
    parser.add_argument('--output', required=True, help='Specify output location in GCS')
    parser.add_argument('--input', required=True, help='dataset or dataset.table in BigQuery')
    parser.add_argument('--schema', action='store_true', help='Write out only the schema, no data')
    parser.add_argument('--quiet', action='store_true', help='Turn off verbose logging')

    args = parser.parse_args()
    if not args.quiet:
        logging.basicConfig(level=logging.INFO)


    if '.' in args.input:
        dataset, table = args.input.split('.')
        tables = [table]
    else:
        dataset = args.input
        tables = exec_shell_pipeline([
            ['bq', 'ls', 'advdata'],
            ['awk', "{print $1}"],
            ['tail', '+3']
        ]).split()


    for table in tables:
        backup_table(dataset, table, args.output, args.schema)
