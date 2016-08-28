#/bin/bash
cd ..
docker run -v `pwd`:/i2p-transform-oracle -p 8080:8080 -d --name airflow airflow
