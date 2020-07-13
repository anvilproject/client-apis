Setup:
```
python3 -m pip install -r requirements.txt
```

Run:
```
# get the billing project from your terra profile
export USER_PROJECT=terra-test-bwalsh
python3 apps/make_graph.py --user_project $USER_PROJECT
```

Cleanup:
To optimize, the make_graph app will cache data in *.sqlite tables.  Delete them to refresh all.