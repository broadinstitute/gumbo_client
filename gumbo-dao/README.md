gumbo-dao is a python module which implements a data-access-object class for querying and updating the gumbo database.

At this level, it accepts a db connection and provides methods for reading a table, and updating a table. Does not concern itself with transaction management or how connection was established.

Examples:

```
dao = GumboDAO(connection)
df = dao.get(table_name)
dao.update(username, table_name, new_df, reason=reason)
dao.insert_only(username, table_name, new_rows_df, reason=reason)
dao.update_only(username, table_name, updated_rows_df, reason=reason)
dao.get_model_condition_status_summaries(peddep_only=peddep_only)
```

tests are in the `tests` subdirectory and use an in-memory sqlite database for testing. To run:

```
pytest tests
```
