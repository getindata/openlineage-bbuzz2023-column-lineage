# openlineage-bdtw-column-lineage

```
docker-compose up
```

Verify if all components are up and running
```
http://localhost:5000/api/v1/namespaces
```
check to verify if Marquez api is up and connection do DB is configured. 
```
http://localhost:3000
```
to see Marquez UI. 

Browse `openlineage-bdtw-column-lineage-notebook-1` docker logs to see Jupyter URL printed:
```
http://127.0.0.1:18888/lab?token=<<>>
```
Click the link with a token to access notebooks.
