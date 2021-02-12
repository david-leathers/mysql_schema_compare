import mysql.connector
from collections import defaultdict
servers = ["127.0.0.1", "localhost"]
db_user = ""
db_password = ""
server_count = len(servers)
schema_dict = defaultdict(int)
table_dict = defaultdict(int)
skip_schemas = ""
# Loop through every server and verify the schemas and tables exist on every server
for my_server in servers:
    db = mysql.connector.connect(
        host=my_server,
        user=db_user,
        password=db_password,
        database="information_schema",
        autocommit=True
    )
    db_cursor = db.cursor()
    # verify that the schemas exist on every server
    db_cursor.execute("SELECT DISTINCT TABLE_SCHEMA FROM TABLES;")
    result_set = db_cursor.fetchall()
    for record in result_set:
        if record[0] not in schema_dict:
            schema_dict[record[0]] = 1
        else:
            schema_dict[record[0]] += 1
    # Verify that the tables exist on every server
    db_cursor.execute("SELECT DISTINCT CONCAT(TABLE_SCHEMA,'.',TABLE_NAME) FROM TABLES;")
    result_set = db_cursor.fetchall()
    for record in result_set:
        if record[0] not in table_dict:
            table_dict[record[0]] = 1
        else:
            table_dict[record[0]] += 1
# Print all schemas and tables that are not on all servers.
for schema in schema_dict:
    if schema_dict[schema] != server_count:
        print(schema + ":" + str(schema_dict[schema]))
for table in table_dict:
    if table_dict[table] != server_count:
        print(table + ":" + str(table_dict[schema]))