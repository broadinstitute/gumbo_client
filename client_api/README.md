# Gumbo Client API

### For local development:

```pip install -r requirements.txt```

To Authenticate to this API: 

run on bash: `gcloud auth application-default login`
Then in python:
```
import google.auth
credentials, project = google.auth.default()
... (make a request using the credentials)
```