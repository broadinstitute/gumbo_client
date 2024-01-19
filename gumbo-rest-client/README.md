The tests for the client are written as end-to-end functional tests. They
run an instance of the service connected to a real database, and then try 
to use an instance of the client to communicate with the service. As such,
tests need to be run with a local postgres database that we can connect to.

Set the `POSTGRES_TEST_DB` with the connect string to use for testing. The
database should ideally be empty but expect tests may add/remove tables as 
needed.

For example on can run the following to use the local database named
'testdb':

```
POSTGRES_TEST_DB='dbname=testdb' pytest
```
