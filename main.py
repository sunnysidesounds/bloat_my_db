from bloat_my_db.schema_builders.pg_schema_builder import PgSchemaBuilder
from bloat_my_db.schema_analyzers.pg_schema_analyzer import PgSchemaAnalyzer
import json

conn_info = {
    "host": "localhost",
    "database": "ToolsAndReagents",
    "user": "postgres",
    "password": "postgres"
}


if __name__ == '__main__':
    builder = PgSchemaBuilder(conn_info)
    schema = builder.build_schema()
    analyzer = PgSchemaAnalyzer(schema, conn_info)
    #print(schema)
    analyzed_schema = analyzer.analyze()


    json_schema = json.dumps(analyzed_schema, indent=4)
    print(json_schema)
