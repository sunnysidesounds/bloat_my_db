
# bloat_my_db

```bash
usage: bloatdb [-h] [-buildSchema] [-buildAnalyzedSchema] [-buildCSVSchema] [-populateRandomData] [-populateCSVData] [-config CONFIG] [-workSpacePath WORKSPACEPATH] [-importCSVFile IMPORTCSVFILE] [-rows ROWS] [-purge] [-force] [-disablePrompt] [-openSchema]
[-openAnalyzedSchema] [-truncateDb]

Utility tool that populates random or CSV data to your database for development purposes

optional arguments:
-h, --help            show this help message and exit
-buildSchema          builds the database schema file
-buildAnalyzedSchema  builds the analyzed database schema file
-buildCSVSchema       builds CSV files of all the table schemas (To had your own data)
-populateRandomData   Takes the analyzed schema and bloats the database with random data
-populateCSVData      Takes the analyzed schema and bloats the database with specified CSV data
-config CONFIG        configuration file or set BLOAT_CONFIG=<path> env variable
-workSpacePath WORKSPACEPATH
The path on were to import/export files (zips, csv), can also set it in the bloat_config.json
-importCSVFile IMPORTCSVFILE
The CSV or zip file (of CSV's) you want to import into the database
-rows ROWS            How may rows do you want to bloat the database, default is 25
-purge                purges all generated files (both the schema, analyzed schema and CSV export files)
-force                force rebuilds both schemas (used with -populateRandomData flag)
-disablePrompt        Disables the user CLI input prompt
-openSchema           Opens the built schema in Chrome browser
-openAnalyzedSchema   Opens the built analyzed schema in Chrome browser
-truncateDb           Truncates all the values in the database

```

## Prerequisite
- python 3.9.1

## Install
```bash
git clone https://github.com/sunnysidesounds/bloat_my_db
cd bloat_my_db
python setup.py install
bloatdb -h
```

## Usage
1. Create a `bloat_config.json` file with these values and set `BLOAT_CONFIG=<path>` env variable or using `-config=<path>`  parameter
```bash
{
   "db":{
      "host":"localhost",
      "database":"<db_name>",
      "user":"<db_user>",
      "password":"<db_password>"
   },
   "paths":{
      "workspace_path":"<path/to/workspace>" #The path on were to import/export files (zips, csv)
   }
}
```

2. Build your json schema
    - This build a json schema of your database 
```bash
bloatdb -buildSchema
```

3. Build your analysis json schema
    - This build the table insertion order, determines constraints and column types
```bash
bloatdb -buildAnalyzedSchema
```

4. Populate data in the database
    - We can either randomly populate data or use CSV files to populate the database.
    
```bash
# Populating database with random data
bloatdb -populateRandomData
# Populate database with zip file with CSV table data
bloatdb -populateCSVData -importCSVFile=<path/to/zip>

```

## Schema Details

### buildSchema produces
```bash
{
    "table_name": {
        "columns": [
            {
                "name": "column_name_1",
                "data_type": "character varying",
                "column_default": null,
                "is_nullable": false,
                "constraint": {
                    "pk___ef_example_constraint": {
                        "type": "PRIMARY KEY",
                        "referenced_table": "table_name",
                        "referenced_column": "table_name_id"
                    }
                }
            },
            {
                "name": "column_name_2",
                "data_type": "character varying",
                "column_default": null,
                "is_nullable": false
            }
        ],
        "@table_metadata": {
            "column_count": 2,
            "has_foreign_keys": false,
            "has_user_defined_keys": false,
            "foreign_constraint_tables": []
        }
    },
    ...
}
```
### buildAnalyzedSchema produces
```bash
{
    "1": {
        "table_name_1": {
            "columns": [
                {
                    "name": "table_name_id",
                    "data_type": "character varying",
                    "column_default": null,
                    "is_nullable": false,
                    "constraint": {
                        "pk___ef_migrations_history": {
                            "type": "PRIMARY KEY",
                            "referenced_table": "table_name_1",
                            "referenced_column": "table_name"
                        }
                    }
                },
                {
                    "name": "table_name",
                    "data_type": "character varying",
                    "column_default": null,
                    "is_nullable": false
                }
            ],
            "@table_metadata": {
                "column_count": 2,
                "has_foreign_keys": false,
                "has_user_defined_keys": false,
                "foreign_constraint_tables": []
            }
        }
    },
    "2": {
        "table_name_2": {
            "columns": [
                {
                    "name": "id",
                    "data_type": "uuid",
                    "column_default": null,
                    "is_nullable": false,
                    "constraint": {
                        "pk_table_name_2": {
                            "type": "PRIMARY KEY",
                            "referenced_table": "table_name_2",
                            "referenced_column": "id"
                        }
                    }
                },
                {
                    "name": "email",
                    "data_type": "character varying",
                    "column_default": null,
                    "is_nullable": false
                },
                {
                    "name": "first_name",
                    "data_type": "character varying",
                    "column_default": null,
                    "is_nullable": false
                }
            ],
            "@table_metadata": {
                "column_count": 3,
                "has_foreign_keys": false,
                "has_user_defined_keys": false,
                "foreign_constraint_tables": []
            }
        }
    },

```

## Notes

This project has been set up using PyScaffold 4.0.2. For details and usage
information on PyScaffold see https://pyscaffold.org/.
