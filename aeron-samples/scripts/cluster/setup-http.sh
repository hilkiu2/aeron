#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

cd /users/hilkiu2/aeron

echo "[`date`]" > /users/hilkiu2/aeron/httpServer.log
echo "[`date`]" > /users/hilkiu2/aeron/httpServer.err

nohup java --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED -Daeron.cluster.tutorial.hostnames=10.200.1.2,10.200.1.2,10.200.1.2 -cp '/users/hilkiu2/aeron/libs/*' io.aeron.samples.cluster.tutorial.AuctionHttpServer >> /users/hilkiu2/aeron/httpServer.log 2>> /users/hilkiu2/aeron/httpServer.err & disown
