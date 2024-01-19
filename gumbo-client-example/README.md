This file contains a sample (`sample.py`) which demonstrates how to
authenticate the client using current google credentials (assuming it is
being run as a service account. If default credentials are a user account, 
this will throw an error)

This instantation is intended for the case where this client runs from a
google cloud function or similar google managed environment.
