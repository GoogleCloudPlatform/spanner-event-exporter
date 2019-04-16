FROM gradle:latest AS build-env
ADD . /app
WORKDIR /app
USER root
RUN gradle wrapper
RUN ./gradlew clean shadowJar && \
  mv build/libs/Spez-0.1.0-SNAPSHOT.jar spez-poller.jar

FROM gcr.io/distroless/java
COPY --from=build-env /app/spez-poller.jar /app/spez-poller.jar
WORKDIR /app
ENTRYPOINT ["java", \
  "-ea", \
  "-Djava.net.preferIPv4Stack=true", \
  "-Dio.netty.allocator.type=pooled", \
  "-XX:+UseStringDeduplication", \
  "-XX:+UseTLAB", \
  "-XX:+AggressiveOpts",\
  "-XX:+UseConcMarkSweepGC", \
  "-XX:+CMSParallelRemarkEnabled",\
  "-XX:+CMSClassUnloadingEnabled", \
  "-XX:ReservedCodeCacheSize=128m", \
  "-XX:SurvivorRatio=128", \
  "-XX:MaxTenuringThreshold=0", \
  "-XX:MaxDirectMemorySize=8G", \
  "-Xss8M", \
  "-Xms512M", \
  "-Xmx4G", \
  "-server", \
  "-Dcom.sun.management.jmxremote", \
  "-Dcom.sun.management.jmxremote.port=9010", \
  "-Dcom.sun.management.jmxremote.rmi.port=9010", \
  "-Dcom.sun.management.jmxremote.local.only=false",\
  "-Dcom.sun.management.jmxremote.authenticate=false",\
  "-Dcom.sun.management.jmxremote.ssl=false",\
  "-Djava.rmi.server.hostname=127.0.0.1",\
  "-Dspez.avroNamespace=$AVRO_NAMESPACE", \
  "-Dspez.instanceName=$INSTANCE_NAME", \
  "-Dspez.daName=$DB_NAME", \
  "-Dspez.tableName=$TABLE_NAME", \
  "-Dspez.pollRate=$POLL_RATE", \
  "-Dspez.recordLimit=$RECORD_LIMIT", \
  "-Dspez.startingTimestamp=$STARTING_TIMESTAMP", \
  "-Dspez.publishToPubSub=$PUBLISH_TO_PUBSUB", \
  "-Dspez.poll=$POLL", \
  "-Dspez.replayToPubSub=$REPLAY_TO_PUBSUB", \
  "-Dspez.replayToPubSubStartTime=$REPLAY_TO_PUBSUB_STARTTIME", \
  "-Dspez.replayToPubSubEndTime=$REPLAY_TO_PUBSUB_ENDTIME", \
  "-Dspez.replayToQueue=$REPLAY_TO_QUEUE", \
  "-Dspez.replayToQueueStartTime=$REPLAY_TO_QUEUE_STARTTIME", \
  "-Dspez.replayToQueueEndTime=$REPLAY_TO_QUEUE_ENDTIME", \
  "-Dspez.replayToSpanner=$REPLAY_TO_SPANNER", \
  "-Dspez.replayToSpannerTableName=$REPLAY_TO_SPANNER_TABLENAME", \
  "-Dspez.replayToSpannerTimestamp=$REPLAY_TO_SPANNER_TIMESTAMP", \
  "-jar",\
  "spez-poller.jar"]
