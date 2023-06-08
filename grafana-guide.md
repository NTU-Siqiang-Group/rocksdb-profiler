# Create the prometheus data source
1. select Connection on the side pannel

![step 1](examples/guide/p1.png)

2. select data source

![step 2](examples/guide/p2.png)

3. click Add new data source

![step 3](examples/guide/p3.png)

4. select prometheus

![step 4](examples/guide/p4.png)

5. set the url to `http://127.0.0.1:9090` and the scrape interval to `1s`

![step 5](examples/guide/p5.png)

6. click save and test

![step 6](examples/guide/p6.png)

# Import grafana.json

1. select Dashboard from the side pannel

![step 1](examples/guide/step1.png)

2. click the import button

![step 2](examples/guide/step2.png)

3. upload the `grafana.json` file in on the root directory

![step 3](examples/guide/step3.png)

4. change the name, the uid, and the data source (that you create on the previous step)

![step 4](examples/guide/step4.png)

5. click import(overwrite)

![step 5](examples/guide/step5.png)
