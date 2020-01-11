#!/bin/bash -e
docker run -m 4G --memory-reservation 4G --memory-swap 4G --hostname=cmu.db --name=cmudb -t -i -v $(pwd):/src $1 bash
