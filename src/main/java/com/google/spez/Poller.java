/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.spez;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.paging.Page;
import com.google.cloud.ByteArray;
import com.google.cloud.ServiceOptions;
import com.google.cloud.Timestamp;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Blob.BlobSourceOption;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.opencensus.common.Scope;
import io.opencensus.contrib.grpc.metrics.RpcViews;
import io.opencensus.contrib.zpages.ZPageHandlers;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import io.opencensus.exporter.trace.stackdriver.StackdriverExporter;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class creates and schedules the poller
 *
 * <p>The {@code Poller} class creates your poller and schedules it to poll the configured spanner
 * table based on your configuration options. For each row the poller receives, it will create a
 * corresponding avro record and publish that record to the configured pub / sub topic. This class
 * assumes you have your Google Cloud credentials set up as described in the {@code README.md} for
 * this git repo.
 *
 * <p>As the poller polls on a fixed interval, there is no mechanism for retry as the poller will
 * just process the records on its next poll attempt based on the last processed timestamp.
 *
 * <p>For the first run, the poller will try to configure the lastTimestamp based on the last record
 * published to the configured pub / sub topic. If it cannot find one, it will default to the
 * timestamp that is configured in spez.
 */
class Poller {
  private static final Logger log = LoggerFactory.getLogger(Poller.class);
  private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
  private static final String SAMPLE_SPAN = "SPEZ";

  private final int pollRate;
  private final String recordLimit;
  private final String avroNamespace;
  private final String instanceName;
  private final String dbName;
  private final String tableName;
  private final String startingTimestamp;
  private final boolean publishToPubSub;
  private final Statement schemaQuery;
  private final DatabaseClient dbClient;
  private final Publisher publisher;
  private final LinkedHashMap<String, String> spannerSchema = Maps.newLinkedHashMap();
  private final SpezConfig config;
  private Spanner spanner;
  private String lastTimestamp;
  private Schema avroSchema;

  public Poller(SpezConfig config) {
    this.config = config;
    this.avroNamespace = config.avroNamespace;
    this.instanceName = config.instanceName;
    this.dbName = config.dbName;
    this.tableName = config.tableName;
    this.pollRate = config.pollRate;
    this.recordLimit = config.recordLimit;
    this.startingTimestamp = config.startingTimestamp;
    this.publishToPubSub = config.publishToPubSub;
    this.lastTimestamp = this.startingTimestamp;
    this.dbClient = configureDb();
    this.publisher = configurePubSub();
    this.schemaQuery =
        Statement.newBuilder(
                "SELECT COLUMN_NAME, SPANNER_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=@tablename ORDER BY ORDINAL_POSITION")
            .bind("tablename")
            .to(tableName)
            .build();
    configureTracing();
  }

  private void configureTracing() {
    try {
      // Installs a handler for /tracez page.
      ZPageHandlers.startHttpServerAndRegisterAll(8080);
      // Installs an exporter for stack driver traces.
      StackdriverExporter.createAndRegister();
      Tracing.getExportComponent()
          .getSampledSpanStore()
          .registerSpanNamesForCollection(Arrays.asList(SAMPLE_SPAN));

      // Installs an exporter for stack driver stats.
      StackdriverStatsExporter.createAndRegister();
      RpcViews.registerAllCumulativeViews();
    } catch (IOException e) {
      log.error("Could not start the tracing server", e);
    }
  }

  private DatabaseClient configureDb() {
    final SpannerOptions options = SpannerOptions.newBuilder().build();
    spanner = options.getService();
    final DatabaseId db = DatabaseId.of(PROJECT_ID, instanceName, dbName);
    final String clientProject = spanner.getOptions().getProjectId();

    if (!db.getInstanceId().getProject().equals(clientProject)) {
      log.error(
          "Invalid project specified. Project in the database id should match"
              + "the project name set in the environment variable GCLOUD_PROJECT. Expected: "
              + clientProject);

      stop();
      System.exit(1);
    }

    final DatabaseClient dbClient = spanner.getDatabaseClient(db);

    return dbClient;
  }

  private Publisher configurePubSub() {
    if (publishToPubSub) {
      ProjectTopicName topicName =
          ProjectTopicName.of(PROJECT_ID, tableName); // Topic name will always be the Table Name
      try {
        Publisher publisher = Publisher.newBuilder(topicName).build();
        return publisher;
      } catch (IOException e) {
        log.error("Was not able to create a publisher for topic: " + topicName, e);

        stop();
        System.exit(1);
      }
    }

    // If configured to publishToPubSub, this function will return a publisher or throw
    return null;
  }

  /**
   * Starts the poller and schedules subsequent polls based on the configuration settings presented
   * at startup.
   */
  public void start() {
    if (config.poll) {
      // Add hook to gracefully shutdown the spanner lib
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread() {
                @Override
                public void run() {
                  // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                  System.err.println("*** shutting down Poller server since JVM is shutting down");
                  Poller.this.stop();
                  System.err.println("*** service shut down");
                }
              });

      // Schedule our poller on a fixed rate to make sure we have a constant delay
      ScheduledExecutorService scheduler =
          Executors.newScheduledThreadPool(
              2,
              new ThreadFactoryBuilder()
                  .setNameFormat("poller")
                  .build()); // Run in 2 threads so we can poll even if we can't finish before the
      // next poll starts. This will introduce the error case of a record being
      // processed multiple times. The archiver should de-dupe the records
      scheduler.scheduleAtFixedRate(
          () -> {
            try {
              poll();
            } catch (Exception e) {
              log.error("poller failed", e);

              stop();
              System.exit(1);
            }
          },
          0,
          pollRate,
          TimeUnit.MILLISECONDS);
    }

    // TODO(XJDR): Should we allow for a custom GCS prefix?
    if (config.replayToPubSub) {
      replayToPubSub("", config.replayToPubSubStartTime, config.replayToPubSubEndTime);
    }

    if (config.replayToQueue) {
      replayToQueue("", config.replayToQueueStartTime, config.replayToQueueEndTime);
    }

    if (config.replayToSpanner) {
      replayToSpanner("", config.replayToSpannerTableName, config.replayToSpannerTimestamp);
    }
  }

  private void getSchema() {
    ResultSet resultSet;
    try (Scope ss =
        Tracing.getTracer()
            .spanBuilderWithExplicitParent(SAMPLE_SPAN, null)
            .setSampler(Samplers.alwaysSample())
            .startScopedSpan()) {
      resultSet = dbClient.readOnlyTransaction().executeQuery(schemaQuery);
    }

    final SchemaBuilder.FieldAssembler<Schema> avroSchemaBuilder =
        SchemaBuilder.record(tableName).namespace(avroNamespace).fields();
    log.debug("Getting Schema");

    log.debug("Processing  Schema");
    while (resultSet.next()) {
      log.debug("Making Avro Schema");
      final Struct currentRow = resultSet.getCurrentRowAsStruct();

      final String name = currentRow.getString(0);
      final String type = currentRow.getString(1);
      spannerSchema.put(name, type);
      log.debug("Binding Avro Schema");
      // TODO(XJDR): Need to strip out size from the type object and add it to the avro datatype i.e
      // STRING(MAX) vs STRING(1024)
      // Fixed length strings will be unsupported in the first release.
      switch (type) {
        case "ARRAY":
          log.debug("Made ARRAY");
          avroSchemaBuilder.name(name).type().array();
          break;
        case "BOOL":
          log.debug("Made BOOL");
          avroSchemaBuilder.name(name).type().booleanType().noDefault();
          break;
        case "BYTES":
          log.debug("Made BYTES");
          avroSchemaBuilder.name(name).type().bytesType().noDefault();
          break;
        case "DATE":
          // Date handled as String type
          log.debug("Made DATE");
          avroSchemaBuilder.name(name).type().stringType().noDefault();
          break;
        case "FLOAT64":
          log.debug("Made FLOAT64");
          avroSchemaBuilder.name(name).type().doubleType().noDefault();
          break;
        case "INT64":
          log.debug("Made INT64");
          avroSchemaBuilder.name(name).type().longType().noDefault();
          break;
        case "STRING(MAX)":
          log.debug("Made STRING");
          avroSchemaBuilder.name(name).type().stringType().noDefault();
          break;
        case "TIMESTAMP":
          log.debug("Made TIMESTAMP");
          avroSchemaBuilder.name(name).type().stringType().noDefault();
          break;
        default:
          log.error("Unknown Schema type when generating Avro Schema: " + type);
          stop();
          System.exit(1);
          break;
      }
    }

    log.debug("Ending Avro Record");
    avroSchema = avroSchemaBuilder.endRecord();

    log.debug("Made Avro Schema");

    final Set<String> keySet = spannerSchema.keySet();

    for (String k : keySet) {
      log.debug("-------------------------- ColName: " + k + " Type: " + spannerSchema.get(k));
    }

    log.debug("--------------------------- " + avroSchema.toString());
  }

  private void poll() throws Exception {
    log.info("polling ....");

    final ByteBuf bb = Unpooled.directBuffer();
    final String[] ts = new String[1];
    final Statement pollQuery =
        Statement.newBuilder(
                "SELECT * FROM "
                    + tableName
                    + " WHERE Timestamp > '"
                    + lastTimestamp
                    + "'"
                    + " ORDER BY Timestamp ASC LIMIT "
                    + recordLimit)
            .build();

    final Set<String> keySet = spannerSchema.keySet();
    final List<ApiFuture<String>> pubSubFutureList = new ArrayList<>();

    ResultSet resultSet;
    try (Scope ss =
        Tracing.getTracer()
            .spanBuilderWithExplicitParent(SAMPLE_SPAN, null)
            .setSampler(Samplers.alwaysSample())
            .startScopedSpan()) {
      resultSet = dbClient.readOnlyTransaction().executeQuery(pollQuery);
    }

    ts[0] = lastTimestamp;
    boolean firstRun = false;

    while (resultSet.next()) {
      if (firstRun == false) {
        getSchema();
        if (publishToPubSub) {
          lastTimestamp = getLastProcessedTimestamp();
        } else {
          lastTimestamp = getLastProcessedTimestampFromSpanner();
        }
        firstRun = true;
      }

      final GenericRecord record = new GenericData.Record(avroSchema);
      keySet.forEach(
          x -> {
            switch (spannerSchema.get(x)) {
              case "ARRAY":
                log.debug("Put ARRAY");

                final Type columnType = resultSet.getColumnType(x);
                final String arrayTypeString =
                    columnType.getArrayElementType().getCode().toString();

                switch (arrayTypeString) {
                  case "BOOL":
                    log.debug("Put BOOL");
                    record.put(x, resultSet.getBooleanList(x));
                    break;
                  case "BYTES":
                    log.debug("Put BYTES");
                    record.put(x, resultSet.getBytesList(x));
                    break;
                  case "DATE":
                    log.debug("Put DATE");
                    record.put(x, resultSet.getStringList(x));
                    break;
                  case "FLOAT64":
                    log.debug("Put FLOAT64");
                    record.put(x, resultSet.getDoubleList(x));
                    break;
                  case "INT64":
                    log.debug("Put INT64");
                    record.put(x, resultSet.getLongList(x));
                    break;
                  case "STRING(MAX)":
                    log.debug("Put STRING");
                    record.put(x, resultSet.getStringList(x));
                    break;
                  case "TIMESTAMP":
                    // Timestamp lists are not supported as of now
                    log.error("Cannot add Timestamp array list to avro record: " + arrayTypeString);
                    break;
                  default:
                    log.error("Unknown Data type when generating Array Schema: " + arrayTypeString);
                    break;
                }

                break;
              case "BOOL":
                log.debug("Put BOOL");
                record.put(x, resultSet.getBoolean(x));
                break;
              case "BYTES":
                log.debug("Put BYTES");
                record.put(x, resultSet.getBytes(x));
                break;
              case "DATE":
                log.debug("Put DATE");
                record.put(x, resultSet.getString(x));
                break;
              case "FLOAT64":
                log.debug("Put FLOAT64");
                record.put(x, resultSet.getDouble(x));
                break;
              case "INT64":
                log.debug("Put INT64");
                record.put(x, resultSet.getLong(x));
                break;
              case "STRING(MAX)":
                log.debug("Put STRING");
                record.put(x, resultSet.getString(x));
                break;
              case "TIMESTAMP":
                log.debug("Put TIMESTAMP");
                ts[0] = resultSet.getTimestamp(x).toString();
                record.put(x, ts[0]);
                break;
              default:
                log.error("Unknown Data type when generating Avro Record: " + spannerSchema.get(x));
                break;
            }
          });

      log.debug("Made Record");
      log.debug(record.toString());

      try (final ByteBufOutputStream outputStream = new ByteBufOutputStream(bb)) {

        final JsonEncoder encoder =
            EncoderFactory.get().jsonEncoder(avroSchema, outputStream, true);
        final DatumWriter<Object> writer = new GenericDatumWriter<>(avroSchema);

        log.debug("Serializing Record");
        writer.write(record, encoder);
        encoder.flush();
        outputStream.flush();
        log.debug("Adding serialized record to list");
        final byte[] ba = new byte[bb.readableBytes()];
        log.debug("--------------------------------- readableBytes " + bb.readableBytes());
        log.debug("--------------------------------- readerIndex " + bb.readerIndex());
        log.debug("--------------------------------- writerIndex " + bb.writerIndex());
        bb.getBytes(bb.readerIndex(), ba);
        final ByteString message = ByteString.copyFrom(ba);

        if (publishToPubSub) {
          final PubsubMessage pubSubMessage =
              PubsubMessage.newBuilder()
                  .setData(message)
                  .putAttributes("Topic", tableName)
                  .putAttributes("Timestamp", record.get("Timestamp").toString())
                  .build();
          final ApiFuture<String> pubSubFuture = publisher.publish(pubSubMessage);
          pubSubFutureList.add(pubSubFuture);
        } else {
          Queue.send(
              dbClient,
              tableName + "_queue",
              record.get("Timestamp").toString(),
              message.toByteArray());
        }

      } catch (IOException e) {
        log.error(
            "IOException while Serializing Spanner Stuct to Avro Record: " + record.toString(), e);
      } finally {
        final List<String> messageIds = ApiFutures.allAsList(pubSubFutureList).get();

        for (String messageId : messageIds) {
          log.info("Published Message: " + messageId);
        }
      }
    }

    lastTimestamp = ts[0];
    log.debug("polled ....");
  }

  private String getLastProcessedTimestampFromSpanner() {
    final Statement pollQuery =
        Statement.newBuilder(
                "SELECT * FROM "
                    + tableName
                    + "_queue"
                    + " WHERE Ack IS TRUE"
                    + " ORDER BY Timestamp DESC LIMIT 1")
            .build();

    final ResultSet resultSet = dbClient.readOnlyTransaction().executeQuery(pollQuery);
    final List<QueueMessage> messages = new ArrayList<>();

    String timestamp = "";
    while (resultSet.next()) {
      timestamp = resultSet.getTimestamp("Timestamp").toString();
    }

    if (timestamp.isEmpty()) {
      log.error("Could not get last processed timestamp from Spanner Queue");

      // If we cannot find a previously processed timestamp, we will default
      // to the one present in the config file.
      return startingTimestamp;
    }

    return timestamp;
  }

  private String getLastProcessedTimestamp() {

    String timestamp = "";
    try {
      final SubscriberStubSettings subscriberStubSettings =
          SubscriberStubSettings.newBuilder()
              .setTransportChannelProvider(
                  SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                      .setMaxInboundMessageSize(20 << 20) // 20MB
                      .build())
              .build();

      try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
        final String subscriptionName = ProjectSubscriptionName.format(PROJECT_ID, tableName);
        final PullRequest pullRequest =
            PullRequest.newBuilder()
                .setMaxMessages(1)
                .setReturnImmediately(true)
                .setSubscription(subscriptionName)
                .build();

        final PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
        final DatumReader<GenericRecord> datumReader =
            new GenericDatumReader<GenericRecord>(avroSchema);

        for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
          final JsonDecoder decoder =
              DecoderFactory.get()
                  .jsonDecoder(avroSchema, message.getMessage().getData().newInput());

          final GenericRecord record = datumReader.read(null, decoder);
          timestamp = record.get("Timestamp").toString();

          log.debug("---------------- Got Timestamp: " + timestamp);
        }
      }
    } catch (IOException e) {
      log.error("Could not get last processed timestamp from pub / sub", e);

      // If we cannot find a previously processed timestamp, we will default
      // to the one present in the config file.
      return startingTimestamp;
    }

    return timestamp;
  }

  private List<Blob> getListOfRecords(
      String prefix, String startingTimestampString, String endingTimestampString) {
    Timestamp startingTimestamp = Timestamp.parseTimestamp(startingTimestampString);
    Timestamp endingTimestamp = Timestamp.parseTimestamp(endingTimestampString);
    Storage storage = StorageOptions.getDefaultInstance().getService();
    Page<Blob> blobs =
        storage.list(prefix, BlobListOption.currentDirectory(), BlobListOption.prefix(tableName));
    List<Blob> records =
        StreamSupport.stream(blobs.iterateAll().spliterator(), false)
            .parallel()
            .filter(
                blob ->
                    Timestamp.parseTimestamp(blob.getMetadata().get("Timestamp"))
                            .compareTo(startingTimestamp)
                        >= 0)
            .filter(
                blob ->
                    Timestamp.parseTimestamp(blob.getMetadata().get("Timestamp"))
                            .compareTo(endingTimestamp)
                        <= 0)
            .collect(Collectors.toList());

    records.sort(
        new Comparator<Blob>() {

          @Override
          public int compare(Blob a, Blob b) {
            int comp =
                Timestamp.parseTimestamp(a.getMetadata().get("Timestamp"))
                    .compareTo(Timestamp.parseTimestamp(b.getMetadata().get("Timestamp")));

            return comp;
          }
        });

    return records;
  }

  private void replayToPubSub(String prefix, String beginning, String end) {
    final List<ApiFuture<String>> pubSubFutureList = new ArrayList<>();
    final List<Blob> records = getListOfRecords(prefix, beginning, end);
    records.forEach(
        record -> {
          final PubsubMessage pubSubMessage =
              PubsubMessage.newBuilder()
                  .setData(
                      ByteString.copyFrom(record.getContent(BlobSourceOption.generationMatch())))
                  .putAttributes("Topic", tableName)
                  .putAttributes("Timestamp", record.getMetadata().get("Timestamp").toString())
                  .build();
          final ApiFuture<String> pubSubFuture = publisher.publish(pubSubMessage);
          pubSubFutureList.add(pubSubFuture);
        });

    try {
      final List<String> messageIds = ApiFutures.allAsList(pubSubFutureList).get();

      for (String messageId : messageIds) {
        log.debug("Published record " + messageId + " to pubsub");
      }
    } catch (InterruptedException | ExecutionException e) {
      log.error(
          "Unable to get pubSub publish future and cannot verify that the record was properly published",
          e);
    }
  }

  private void replayToQueue(String prefix, String beginning, String end) {
    List<Blob> records = getListOfRecords(prefix, beginning, end);
    records.forEach(
        record -> {
          Queue.send(
              dbClient,
              tableName + "_queue",
              record.getMetadata().get("Timestamp").toString(),
              record.getContent(BlobSourceOption.generationMatch()));
        });
  }

  private Timestamp replayToSpanner(String prefix, String newTableName, String timestamp) {
    // When writing relatively large rows, a commit size of 1 MB to 5 MB usually provides the best
    // performance. When writing small values, or values that are indexed, it is generally best to
    // write at most a few hundred rows in a single commit. Independently from the commit size and
    // number of rows, be aware that there is a limitation of 20,000 mutations per commit.
    getSchema();
    final List<Schema.Field> fields = avroSchema.getFields();
    final List<Mutation> mutations = new ArrayList<>();
    final List<Blob> records = getListOfRecords(prefix, startingTimestamp, timestamp);
    final DatumReader<GenericRecord> datumReader =
        new GenericDatumReader<GenericRecord>(avroSchema);

    records.forEach(
        gcsRecord -> {
          try {
            final Mutation.WriteBuilder mutationBuilder =
                Mutation.newInsertOrUpdateBuilder(newTableName);
            final InputStream stream =
                ByteSource.wrap(gcsRecord.getContent(BlobSourceOption.generationMatch()))
                    .openStream();
            final JsonDecoder decoder = DecoderFactory.get().jsonDecoder(avroSchema, stream);
            final GenericRecord record = datumReader.read(null, decoder);

            fields.forEach(
                field -> {
                  final String name = field.schema().getName();
                  final Schema.Type type = field.schema().getType();
                  switch (type) {
                    case ARRAY:
                      // TODO(XJDR): Not sure if this is the best option here ...
                      mutationBuilder.set(name).to((Struct) record.get(name));
                      break;
                    case BOOLEAN:
                      mutationBuilder.set(name).to((boolean) record.get(name));
                      break;
                    case BYTES:
                      mutationBuilder.set(name).to(ByteArray.copyFrom((byte[]) record.get(name)));
                      break;
                    case DOUBLE:
                      mutationBuilder.set(name).to((double) record.get(name));
                      break;
                    case LONG:
                      mutationBuilder.set(name).to((long) record.get(name));
                      break;
                    case STRING:
                      if (name.equals("Timestamp")) {
                        mutationBuilder.set(name).to((Timestamp) record.get(name));
                      } else {
                        mutationBuilder.set(name).to((String) record.get(name));
                      }
                      break;
                    default:
                      log.error(
                          "Was not able to replay record. Did not recognize the Type: "
                              + type
                              + "for Column: "
                              + name);
                      break;
                  }
                });

            mutations.add(mutationBuilder.build());

            if (mutations.size() > 15000) {
              dbClient.write(mutations);
              mutations.clear();
            }

          } catch (IOException e) {
            log.error("Replaying archive to Spanner failed: ", e);
          }
        });

    return dbClient.write(mutations);
  }

  private void stop() {
    spanner.close();
  }
}
