#!/bin/bash -e
docker run -m 4G --memory-reservation 4G --memory-swap 4G --hostname=cmu.db --privileged=true -t -i -v $(pwd):$(pwd) --publish-all=true -p9091:9091 $1 bash
