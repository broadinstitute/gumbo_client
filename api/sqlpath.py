from collections import defaultdict
from typing import List, Tuple, Dict
import re
from dataclasses import dataclass
from gumbo_client.client import _get_pk_column

IDENTIFIER_RE = re.compile("[A-Za-z_]+[A-Za-z_0-9]*")

@dataclass
class Query:
    "A query which can be translated to SQL via query_to_sql()"
    table: str
    column_expressions : List[Tuple[str]]

@dataclass
class ForeignKey:
    "Used to define FK in Schema"
    column_name : str
    referenced_table : str

@dataclass
class Table:
    "Used to define a Table in Schema"
    name: str
    primary_key: str
    foreign_keys : List[ForeignKey]

class Schema:
    "Metadata about schema needed to traverse FK relationships when translating queries to SQL"
    def __init__(self, tables: List[Table]):
        self.referenced_tables_by_fk = {}
        self.tables_by_name = {}

        for table in tables:
            self.tables_by_name[table.name] = table
            for fk in table.foreign_keys:
                self.referenced_tables_by_fk[(table.name, fk.column_name)] = fk.referenced_table

    def get_referenced_table(self, table: str, field: str) -> str:
        "give a table and a field within that table, returns the table that column references or raises an exception if column doesn't reference table."
        key = (table, field)
        assert key in self.referenced_tables_by_fk, f"{table}.{field} does not appear to reference any table"
            
        return self.referenced_tables_by_fk[key]

    def get_pk(self, table):
        return self.tables_by_name[table].primary_key


def _parse_path(path):
    "Given a path of the form 'table1.table2.field' turn that into ('table1', 'table2', 'field') handling whitespace and validating that path have no funny characters"
    # break by "."
    parts = path.split(".")

    # and strip any whitespace
    parts = [x.strip() for x in parts]

    for part in parts:
        assert IDENTIFIER_RE.match(part), f"Not a valid identifier: {part}"
    
    return tuple(parts)



def _construct_join_clause(starting_table : str, 
                    schema: Schema,
                    join_path : List[List[str]], 
                    aliases_by_path: Dict):
    prev_table = starting_table

    for field in join_path[:-1]:
        prev_table = schema.get_referenced_table(prev_table, field)

    field = join_path[-1]
    referenced_table = schema.get_referenced_table(prev_table, field)

    if len(join_path) == 1:
        prev_alias = starting_table
    else:
        prev_alias = aliases_by_path[join_path[:-1]]

    alias = aliases_by_path[join_path]
    referenced_table_pk = schema.get_pk(referenced_table)
    return f"LEFT OUTER JOIN {referenced_table} {alias} ON {alias}.{referenced_table_pk} = {prev_alias}.{field}"



def _construct_alias_dict(column_expressions):
    aliases_by_path = {}
    def add_alias_for(join_path):
        if join_path not in aliases_by_path:
            aliases_by_path[join_path] = f"{join_path[-1]}_{len(aliases_by_path)}"

    def add_aliases_for(join_path):
        # call add_alias_for on each sub-path (to ensure we have joins for intermediate tables)
        for i in range(1, len(join_path)+1):
            add_alias_for(join_path[:i])

    # build map of the "join path" to the alias that we'll use to refer to that joined table
    for expr in (column_expressions):
        if len(expr) > 1:
            add_aliases_for(expr[:-1])
    return aliases_by_path

def query_to_sql(query : Query, schema : Schema):
    column_expressions =  query.column_expressions
    aliases_by_path = _construct_alias_dict(column_expressions)

    # convert all the join paths into sql join clauses
    joins = []
    for join_path in sorted(aliases_by_path.keys()):
        joins.append(_construct_join_clause(query.table, schema, join_path, aliases_by_path))

    def to_sql_expr(expr: Tuple[str]):
        if len(expr) == 1:
            table_alias = query.table
        else:
            table_alias = aliases_by_path[expr[:-1]]
        return f"{table_alias}.{expr[-1]}"

    columns = [to_sql_expr(expr) for expr in column_expressions]

    return f"SELECT {', '.join(columns)} FROM {query.table} {' '.join(joins)}"

def read_schema_from_postgresql(connection):
    cursor = connection.cursor()
    cursor.execute("""SELECT
            tc.table_schema, 
            tc.constraint_name, 
            tc.table_name, 
            kcu.column_name, 
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name 
        FROM 
            information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY';
        """)
    rows = cursor.fetchall()

    columns_referrenced = set()
    fks_per_table = defaultdict(lambda: [])
    for _, _, table_name, column_name, _, foreign_table_name, foreign_column_name in rows:
        fks_per_table[table_name].append(ForeignKey(column_name, foreign_table_name))

        key = f"{table_name}.{column_name}"
        assert key not in columns_referrenced, "Composite FKs not supported but {key} had more than one fk record"
        columns_referrenced.add(key)

    tables = []
    for table_name in fks_per_table.keys():
        tables.append(Table(table_name, _get_pk_column(cursor, table_name), fks_per_table[table_name]))

    return Schema(tables)

def make_query(table : str, fields : str):
    return Query(table, [_parse_path(x) for x in fields.split(",")])

def test_construct_join_clause():
    schema = Schema([Table("order", "order_id", [ForeignKey("customer_id", "customer")]),
        Table("item","item_id", [ForeignKey("order_id", "order")]),
        Table("customer", "customer_id", [])
    ])

    # 1. test one level of joins 

    column_expressions = [ ("order_id","date"), ("product_name",), ("quantity",) ]

    aliases_by_path = _construct_alias_dict(column_expressions)

    clause = _construct_join_clause("item",
                    schema,
                    ("order_id",), 
                    aliases_by_path)
    assert clause == "LEFT OUTER JOIN order order_id_0 ON order_id_0.order_id = item.order_id"

    # 2. test two levels of joins

    column_expressions = [ ("order_id","date"), ("order_id", "customer_id", "name"), ("quantity",) ]

    aliases_by_path = _construct_alias_dict(column_expressions)

    clause = _construct_join_clause("item",
                    schema,
                    ("order_id",), 
                    aliases_by_path)
    assert clause == "LEFT OUTER JOIN order order_id_0 ON order_id_0.order_id = item.order_id"

    clause = _construct_join_clause("item",
                    schema,
                    ("order_id","customer_id"), 
                    aliases_by_path)
    assert clause == "LEFT OUTER JOIN customer customer_id_1 ON customer_id_1.customer_id = order_id_0.customer_id"

def test_query_to_sql():
    query = make_query("item", "order_id.date,order_id.customer_id.name, quantity")
    schema = Schema([Table("order", "order_id", [ForeignKey("customer_id", "customer")]),
        Table("item","item_id", [ForeignKey("order_id", "order")]),
        Table("customer", "customer_id", [])
    ])

    sql =  query_to_sql(query , schema )
    assert sql == ("SELECT order_id_0.date, customer_id_1.name, item.quantity "
                   "FROM item "
                   "LEFT OUTER JOIN order order_id_0 ON order_id_0.order_id = item.order_id "
                   "LEFT OUTER JOIN customer customer_id_1 ON customer_id_1.customer_id = order_id_0.customer_id")
