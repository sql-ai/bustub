REM !/bin/bash -e
docker run -m 8G --memory-reservation 6G --memory-swap 8G --hostname=cmu.db --privileged=true -t -i -v %cd%:/src --publish-all=true -p9091:9091 %1 bash