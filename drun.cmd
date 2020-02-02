#!/bin/bash -e
docker run -m 4G --privileged=true -ti --name cmudb -v C:\Users\doant\code\bustub:/bustub --mount source=vscode-server,target=/root 15601d1fee00 bash