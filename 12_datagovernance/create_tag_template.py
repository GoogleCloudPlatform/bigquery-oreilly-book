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

from google.cloud import datacatalog

dc_client = datacatalog.DataCatalogClient()

def create_overwrite_template(project_id, region, template_id):
    
    project_location_path = 'projects/{}/locations/{}'.format(project_id, region)
    tag_template_path = 'projects/{}/locations/{}/tagTemplates/{}'.format(project_id, region, template_id)
    
    get_req = datacatalog.GetTagTemplateRequest(
        name=tag_template_path,
    )
    
    try:
        resp = dc_client.get_tag_template(request=get_req)
        
        if resp != None:
            template_exists = True
            print('Tag template exists')
        
    except Exception as e:
        if '403' in str(e):
            template_exists = False
            print('Tag template does not exist')
        else:
            print('Error getting tag template:', e)

    if template_exists == True:
        delete_req = datacatalog.DeleteTagTemplateRequest(
                name=tag_template_path,
                force=True,
        )

        try:
            dc_client.delete_tag_template(request=delete_req)
            print('Deleted tag template')
        except Exception as e:
            print('Error deleting tag template:', e)
            
    template = datacatalog.TagTemplate()
    template.display_name = 'Data Classification Template'
    
    # Bool field
    bool_field = datacatalog.TagTemplateField()
    bool_field.type_.primitive_type = datacatalog.FieldType.PrimitiveType.BOOL
    bool_field.display_name = 'Is Sensitive'
    bool_field.is_required = True
    bool_field.description = 'This resource stores sensitive data. True or False.'
    bool_field.order = 1 
    template.fields['is_sensitive'] = bool_field
    
    # Enum field
    sensitive_categories = ['Category1', 'Category2', 'Category3', 'Category4']
    enum_field = datacatalog.TagTemplateField()
    
    for category in sensitive_categories:
        
        enum_value = datacatalog.FieldType.EnumType.EnumValue()
        enum_value.display_name = category
        enum_field.type_.enum_type.allowed_values.append(enum_value)
                            
    enum_field.display_name = 'Sensitive Category'
    enum_field.is_required = False                       
    enum_field.description = 'Sensitive category of the data. Must conform to standard classification'
    enum_field.order = 0
    template.fields['sensitive_category'] = enum_field

    try:
        dc_client.create_tag_template(parent=project_location_path, tag_template_id=template_id, tag_template=template)
        print('Created tag template')
    except Exception as e:
        print('Error creating tag template', e)
    

if __name__ == '__main__':
    project_id = 'bigquerybook2e' 
    region = 'us-central1'
    template_id = 'data_classification'
    create_overwrite_template(project_id, region, template_id)       