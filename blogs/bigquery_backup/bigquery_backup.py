#!/usr/bin/env python3

# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import argparse
import json
import os

from helper_utils import exec_shell_command, exec_shell_pipeline, write_json_string

def backup_table(dataset, tablename, todir, schemaonly):
    """
    Store schema & data in table to GCS
    :param dataset: BigQuery dataset name
    :param tablename: BigQuery table name
    :param todir: GCS output prefix
    :param schemaonly: don't export data, just the schema
    :return: None
    """

    full_table_name = '{}.{}'.format(dataset, tablename)

    # write schema to GCS
    schema = exec_shell_command(['bq', 'show', '--schema', full_table_name])
    write_json_string(schema,
                      os.path.join(todir, dataset, tablename, 'schema.json')
                      )

    if not schemaonly:
        # back up the table definition
        tbldef = exec_shell_command(['bq', '--format=json', 'show', full_table_name])
        write_json_string(tbldef,
                          os.path.join(todir, dataset, tablename, 'tbldef.json')
                          )

        tbldef = json.loads(tbldef)  # array of dicts
        if tbldef['type'] == 'VIEW':
            return  # no need to extract data

        # read the data
        output_data_name = os.path.join(todir, dataset, tablename, 'data_*.avro')
        _ = exec_shell_command([
            'bq', 'extract',
            '--destination_format=AVRO',
            '--use_avro_logical_types', # for DATE, TIME, NUMERIC
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
        dataset_contents = exec_shell_command(
            ['bq', '--format=json', 'ls', dataset]
        )
        dataset_contents = json.loads(dataset_contents) # array of dicts
        tables = []
        for entry in dataset_contents:
            if entry['type'] == 'TABLE' or entry['type'] == 'VIEW':
                tables.append(entry['tableReference']['tableId'])
            else:
                logging.warning('Not backing up {} because it is a {}'.format(entry['id'], entry['type']))
        print(tables)

    for table in tables:
        backup_table(dataset, table, args.output, args.schema)

