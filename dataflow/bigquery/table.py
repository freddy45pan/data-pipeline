TABLE_ID = 'craft_beers'

# SCHEMA = ','.join([
#     'abv:FLOAT',
#     'ibu:INTEGER',
#     'id:STRING',
#     'name:STRING',
#     'style:STRING',
#     'brewery_id:STRING',
#     'ounces:FLOAT',
#     'brewery_name:STRING',
#     'brewery_city:STRING',
#     'brewery_state:STRING'
# ])

SCHEMA = [
    {
        'name' : 'abv',
        'type' : 'FLOAT',
        'mode' : 'NULLABLE'
    },
    {
        'name' : 'ibu',
        'type' : 'INTEGER',
        'mode' : 'NULLABLE'
    },
    {
        'name' : 'id',
        'type' : 'STRING',
        'mode' : 'NULLABLE'
    },
    {
        'name' : 'name',
        'type' : 'STRING',
        'mode' : 'NULLABLE'
    },
    {
        'name' : 'style',
        'type' : 'STRING',
        'mode' : 'NULLABLE'
    },
    {
        'name' : 'brewery_id',
        'type' : 'STRING',
        'mode' : 'NULLABLE'
    },
    {
        'name' : 'ounces',
        'type' : 'FLOAT',
        'mode' : 'NULLABLE'
    },
    {
        'name' : 'brewery_name',
        'type' : 'STRING',
        'mode' : 'NULLABLE'
    },
    {
        'name' : 'brewery_city',
        'type' : 'STRING',
        'mode' : 'NULLABLE'
    },
    {
        'name' : 'brewery_state',
        'type' : 'STRING',
        'mode' : 'NULLABLE'
    },
]
