===========
bloat_my_db
===========


Add a short description here!


Description
===========

A longer description of your project goes here...


.. _pyscaffold-notes:

Note
====

This project has been set up using PyScaffold 4.0.2. For details and usage
information on PyScaffold see https://pyscaffold.org/.

schema:
{
   "table1":{
        "id": {
            "data_type":"uuid"
            "column_default": "None",
            "is_nullable":false,
            "constraint": {
                "pk_capsids" : {
                    "type": "PRIMARY KEY",
                    "referenced_table":"capsids",
                    "referenced_column":"id"
                }
            },
        },
        ...,
        "column_count": 0,
        "has_foreign_keys": false,
        "foreign_constraint_tables" : [
            "capsids", "molecules"
        ]
   },
    ...
}
