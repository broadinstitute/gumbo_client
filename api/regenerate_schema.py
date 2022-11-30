import gumbo_client
from sqlpath import read_schema_from_postgresql

def main():
    client = gumbo_client.Client()
    schema = read_schema_from_postgresql(client.connection)
    with open("gumbo_schema.py", "wt") as fd:
        fd.write("from sqlpath import Schema, ForeignKey, Table\n")
        fd.write("schema=")
        fd.write(repr(schema))
        fd.write("\n")

if __name__ == "__main__":
    main()