from google.cloud import datacatalog

dc_client = datacatalog.DataCatalogClient()

def lookup_entry(project_id, dataset, table): 
    
    entry_name = None
    
    fq_table = '//bigquery.googleapis.com/projects/{}/datasets/{}/tables/{}'.format(project_id, dataset, table)
    
    request = datacatalog.LookupEntryRequest()
    request.linked_resource=fq_table
    
    try:
        entry = dc_client.lookup_entry(request)
        entry_name = entry.name      
    except Exception as e:
        print('Unable to lookup entry', e)
        
    return entry_name
    
    
def create_update_tag(project_id, region, template_id, entry_name):
   
    tag_exists = False
    template_path = dc_client.tag_template_path(project_id, region, template_id)
    
    try:
        tag_list = dc_client.list_tags(parent=entry_name)
    
        for current_tag in tag_list:
            if current_tag.template == template_path:
                tag_exists = True
                break
    
    except Exception as e:
        print('Error occurred during list:', e)
    
    tag = datacatalog.Tag()
    tag.template = template_path
    
    bool_field = datacatalog.TagField()
    bool_field.bool_value = True
    tag.fields['is_sensitive'] = bool_field
    
    enum_field = datacatalog.TagField()
    enum_field.enum_value.display_name = 'Category1'
    tag.fields['sensitive_category'] = enum_field
    
    try:
        if tag_exists != True:
            dc_client.create_tag(parent=entry_name, tag=tag)
        else:
            tag.name = current_tag.name
            dc_client.update_tag(tag=tag)
    except Exception as e:
        print('Error occurred during create or update:', e)

    
if __name__ == '__main__':
    project_id = 'bigquerybook2e'
    region = 'us-central1'
    dataset = 'fitdw'               
    table = 'User'              
    template_id = 'data_classification'

    entry_name = lookup_entry(project_id, dataset, table)
    
    if entry_name:
        create_update_tag(project_id, region, template_id, entry_name)    
    