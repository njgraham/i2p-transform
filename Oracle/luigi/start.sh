#/bin/bash
cd ..
docker run -v `pwd`:/i2p-transform-oracle -p 8082:8082 -d --name luigi luigi
