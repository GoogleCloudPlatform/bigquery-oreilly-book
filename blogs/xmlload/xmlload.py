def parse_into_dict(xmlfile):
    import xmltodict
    with open(xmlfile) as ifp:
        doc = xmltodict.parse(ifp.read())
        return doc
table_schema = {
    'fields': [
        {'name' : 'CustomerID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name' : 'EmployeeID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name' : 'OrderDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name' : 'RequiredDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name' : 'ShipInfo', 'type': 'RECORD', 'mode': 'NULLABLE', 'fields': [
            {'name' : 'ShippedDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipVia', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipAddress', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipCity', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipRegion', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipPostalCode', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipCountry', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]},
    ]
}

# The @ symbol is not allowed as a column name in BigQuery
def cleanup(x):
    import copy
    y = copy.deepcopy(x)
    if '@ShippedDate' in x['ShipInfo']: # optional attribute
        y['ShipInfo']['ShippedDate'] = x['ShipInfo']['@ShippedDate']
        del y['ShipInfo']['@ShippedDate']
    print(y)
    return y

def get_orders(doc):
    for order in doc['Root']['Orders']['Order']:
        yield cleanup(order)

def run(to_bq):
    import apache_beam as beam
    p = beam.Pipeline()
    orders = (p 
         | 'files' >> beam.Create(['orders.xml'])
         | 'parse' >> beam.Map(lambda filename: parse_into_dict(filename))
         | 'orders' >> beam.FlatMap(lambda doc: get_orders(doc)))
    
    if to_bq:
         orders | 'tobq' >> beam.io.WriteToBigQuery('ai-analytics-solutions:advdata.fromxml',
                                   schema=table_schema,
                                   write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, #WRITE_TRUNCATE
                                   create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    else:
        orders | 'totxt' >> beam.io.WriteToText('orders.txt')
        
    p.run().wait_until_finish()
    
if __name__ == '__main__':
    run(False)
