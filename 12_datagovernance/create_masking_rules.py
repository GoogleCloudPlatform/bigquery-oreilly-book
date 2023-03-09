#!/usr/bin/python
#
# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import yaml

from google.cloud.bigquery import datapolicies
from google.iam.v1 import iam_policy_pb2
#from google.cloud import bigquery


def process_request(project, region, yaml_file):
        
    with open(yaml_file) as f:
        file_contents = yaml.full_load(f)
        subtree = file_contents.get("masking_rules")
        #print(masking_rules)
        
        for masking_rule in subtree:
            create_update_masking_rule(project, region, masking_rule)
        
                    
def create_update_masking_rule(project, region, masking_rule):
    
    print('masking_rule:', masking_rule)
    
    parent = 'projects/{0}/locations/{1}'.format(project, region)
    masking_policy = masking_rule.get("masking_policy")
    data_policy = 'projects/{0}/locations/{1}/dataPolicies/{2}'.format(project, region, masking_policy)
    masking_type = masking_rule.get("masking_type")
    masked_readers = masking_rule.get("masked_readers")
    policy_tag = masking_rule.get("policy_tag")

    dpsc = datapolicies.DataPolicyServiceClient()
    
    list_req = datapolicies.ListDataPoliciesRequest(parent=parent)
    list_res = dpsc.list_data_policies(request=list_req)
    
    masking_rule_exists = False
    
    for res in list_res:
        # data policies must be unique under a project and location
        if res.name == data_policy:
            print('masking rule', data_policy, 'already exists')
            masking_rule_exists = True
            policy_tag = res.policy_tag
            break
    
    if 'hash' in masking_type.lower() or 'SHA256' in masking_type:
        predefined_expression = 'SHA256'
    elif 'nullify' in masking_type.lower() or 'null' in masking_type.lower():
        predefined_expression = 'ALWAYS_NULL'
    elif 'last_four' in masking_type.lower() or 'last_four_characters' in masking_type.lower():
        predefined_expression = 'LAST_FOUR_CHARACTERS'
    elif 'first_four' in masking_type.lower() or 'first_four_characters' in masking_type.lower():
        predefined_expression = 'FIRST_FOUR_CHARACTERS'
    elif 'email' in masking_type.lower() or 'email_mask' in masking_type.lower():
        predefined_expression = 'EMAIL_MASK'
    elif 'date_year' in masking_type.lower() or 'date_year_mask' in masking_type.lower():
        predefined_expression = 'DATE_YEAR_MASK'
    else:
        predefined_expression = 'DEFAULT_MASKING_VALUE'

    dp = datapolicies.DataPolicy()
    dp.name = data_policy
    dp.data_policy_id = masking_policy
    dp.data_policy_type = 'DATA_MASKING_POLICY'
    dp.policy_tag = policy_tag
    dp.data_masking_policy.predefined_expression = predefined_expression
    
    if masking_rule_exists:
        
        dp_req = datapolicies.UpdateDataPolicyRequest(data_policy=dp)
        #print('dp_req:', dp_req)
    
        dp_res = dpsc.update_data_policy(request=dp_req)
        #print('dp_res:', dp_res)
        
        print('updated data policy ', masking_policy)
        
    else:  

        dp_req = datapolicies.CreateDataPolicyRequest(
            parent=parent,
            data_policy=dp)
        #print('dp_req:', dp_req)
    
        dp_res = dpsc.create_data_policy(request=dp_req)
        #print('dp_res:', dp_res)
        
        print('created data policy ', masking_policy)
        
    # set permissions on the created or updated policy
    set_masked_readers(dp_res.name, masked_readers)


def set_masked_readers(data_policy, masked_readers):
    
    dpsc = datapolicies.DataPolicyServiceClient()
    formatted_readers = []
    
    for reader in masked_readers:
        
        if reader.strip().startswith('user:') or reader.strip().startswith('group:') or reader.strip().startswith('serviceAccount:'):
            formatted_readers.append(reader)
        else:
            print('Please prefix the email address with user: or group: or serviceAccount:')    
            return
    
    print('formatted_readers:', formatted_readers)
    
    iam_req = iam_policy_pb2.GetIamPolicyRequest(resource=data_policy)
    iam_resp = dpsc.get_iam_policy(request=iam_req)
    #print('iam_resp:', iam_resp)
        
    policy = {
     "bindings": [
     {
      "role": "roles/bigquerydatapolicy.maskedReader",
      "members": formatted_readers
      }],
      "etag": iam_resp.etag
    }

    #print('policy:', policy)
    
    iam_req = iam_policy_pb2.SetIamPolicyRequest(resource=data_policy, policy=policy)
    iam_resp = dpsc.set_iam_policy(iam_req)
    print('set masked readers')
    
    #print('iam_resp:', iam_resp)

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Creates masking rules based on a yaml file specification")
    parser.add_argument('project', help='The project id in which to create the masking rules')
    parser.add_argument('region', help='The region in which to create the masking rules')
    parser.add_argument('yaml_file', help='The yaml file containing the specification')
    args = parser.parse_args()
    process_request(args.project, args.region, args.yaml_file)
    