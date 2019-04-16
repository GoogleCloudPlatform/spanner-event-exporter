# Spez

Spez == Spanner Poller Event-sourced Z

`Spez` is a library that will register a listner that will publish any record
you write to a particular Spanner table as an Avro record on a pub / sub stream.
This is a great foundation for creating Event Sourced Systems. `Spez` also
provides a Cloud Function called "Archiver" that will be triggered by any write
to a specific pub / sub topic and automatically write that record to a Google
Cloud Storage bucket.

This is an example of how you might work with a `spez` record once it is on the
pub / sub queue.

### Example:

```java

 try {
      final SubscriberStubSettings subscriberStubSettings =
          SubscriberStubSettings.newBuilder()
              .setTransportChannelProvider(
                  SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                      .setMaxInboundMessageSize(20 << 20) // 20MB
                      .build())
              .build();

      try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
        final String subscriptionName = ProjectSubscriptionName.format(PROJECT_ID, topicName);
        final PullRequest pullRequest =
            PullRequest.newBuilder()
                .setMaxMessages(20)
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

          // ... Process Record
        }
      }
    } catch (IOException e) {
      log.error("Could not get record", e);

    }
```

## Configuration

### Application Config

`Spez` uses a config library that allows you to specify your config variables
via a yaml(like) config file, or override them with command line options. The
following variables are defined as the default configuration in the `spez`.conf
file. The proper way to override these variables for this application is via the
k8s configmap located in the kubernetes directory.

```yaml
spez {
  avroNamespace="spez"
  instanceName="spez-poller-demo"
  dbName="spez_poller_db"
  tableName="spez_poller_table"
  pollRate=1000
  recordLimit="200"
  startingTimestamp="2019-03-06T01:29:25.500000Z"
  publishToPubSub=true
  poll=true
  replayToPubSub=false
  replayToPubSubStartTime="2019-03-06T01:29:25.500000Z"
  replayToPubSubEndTime="2019-03-06T01:29:25.500000Z"
  replayToQueue=false
  replayToQueueStartTime="2019-03-06T01:29:25.500000Z"
  replayToQueueEndTime="2019-03-06T01:29:25.500000Z"
  replayToSpanner=false
  replayToSpannerTableName="spez_poller_table_replay"
  replayToSpannerTimestamp="2019-03-06T01:29:25.500000Z"
}
```

### K8s configmap

These values are located in the kubernetes/configmap.yaml. You will need to
modify these values to match your system configuration in order to properly use
spez.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: spez-config
  namespace: default
data:
  GOOGLE_APPLICATION_CREDENTIALS: "/var/run/secret/cloud.google.com/service-account.json"
  AVRO_NAMESPACE: "spez"
  INSTANCE_NAME: "spez-poller-demo"
  DB_NAME: "spez_poller_db"
  TABLE_NAME: "spez_poller_table"
  POLL_RATE: "1000"
  RECORD_LIMIT: "200"
  STARTING_TIMESTAMP: "2019-03-06T01:29:25.500000Z"
  PUBLISH_TO_PUBSUB: true
  FUNCTION: "archiver"
  BUCKET_REGION: "us-west-1"
  BUCKET_NAME: "gs://spez_archive/spez_poller_table"
  POLL: true
  REPLAY_TO_PUBSUB: false
  REPLAY_TO_PUBSUB_STARTTIME: "2019-03-06T01:29:25.500000Z"
  REPLAY_TO_PUBSUB_ENDTIME: "2019-03-06T01:29:25.500000Z"
  REPLAY_TO_QUEUE: false
  REPLAY_TO_QUEUE_STARTTIME: "2019-03-06T01:29:25.500000Z"
  REPLAY_TO_QUEUE_ENDTIME: "2019-03-06T01:29:25.500000Z"
  REPLAY_TO_SPANNER: false
  REPLAY_TO_SPANNER_TABLENAME: "spez_poller_table_replay"
  REPLAY_TO_SPANNER_TIMESTAMP: "2019-03-06T01:29:25.500000Z"

```

## Create a Spanner Table

In order to use this poller you must have a column named Timestamp that is not
null and contains the Spanner CommitTimestamp.

The poller will perform a full table scan on each poll interval. This will
consume resources on your db instance. Typically to help with this, you would
create a secondary index with the timestamp as the primary key. Do not do that
as it will cause hotspots. In this case, you may want to instead increase the
polling interval in order to address any excessive resource consumption on your
instance.

Do not use a commit timestamp column as the first part of the primary key of a
table or the first part of the primary key of a secondary index. Using a commit
timestamp column as the first part of a primary key creates hotspots and reduces
data performance, but performance issues may occur even with low write rates.
There is no performance overhead to enable the commit timestamps on non-key
columns that are not indexed.

[Review this document for more information on sharding Spanner CommitTimestamps](https://cloud.google.com/blog/products/gcp/sharding-of-timestamp-ordered-data-in-cloud-spanner)

Example:

```sql
CREATE TABLE spez_poller_table (
    ID INT64 NOT NULL,
    Color STRING(MAX),
    Name STRING(MAX),
    Timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (ID)

```

## Create a Service Account

In order to allow `spez` to interact with the necessary Google Cloud resources,
you must create a service account for `spez` and give it the following
permissions:

The `spez` application requires the following permissions:

*   Tha ability to read data from you spanner instance.
*   The ability to publish messages to a pub/sub topic.
*   The ability to write trace data to Stackdriver Trace.

Create a service account for the `spez` application:

```bash
export PROJECT_ID=$(gcloud config get-value core/project)
export SERVICE_ACCOUNT_NAME="spez-service-account"

## Create the service account
gcloud iam service-accounts create ${SERVICE_ACCOUNT_NAME} \
  --display-name "spez service account"

### Add the `spanner.databaseReader`, `pubsub.editor` and `cloudtrace.agent` IAM permissions to the spez service account:
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role='roles/spanner.databaseReader'

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role='roles/pubsub.editor'

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role='roles/cloudtrace.agent'

### Generate and download the `spez` service account:
gcloud iam service-accounts keys create \
  --iam-account "${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  service-account.json
```

## Deploying Spez

Spez is intended to be run on kubernetes. The configmap, deployment and service
yaml are provided in the kubernetes/ directory.

```bash
# Deploy spez with the appropriate kubectl context
kubectl apply -f kubernetes/
```

## Monitoring

Spez comes preconfigured to send the polling traces and metrics to Stackdriver
(make sure you have the APIs enabled). For application logs, make sure you have
your kubernetes cluster set up to recieve and publish logs sent to stdout and
stderr.

## Queues

In addition to the event poller, spez comes with a library to help process other
types of asynchronous work called Queues. Queues is an integrated transactional
messaging system that provides users with an easy way to perform asynchronous
work at scale and even across multiple regions (when and where spanner is
configured to do so). A Queue will continue to deliver a message until that
message has been Ack'd and therefore is at least once delivery guarentee if
there are multiple readers of the Queue.

#### Queues have the following properties:

-   Transactional: Applications are able to send and receive messages atomically
    within a Spanner transaction with other writes: queue operations are
    executed if and only if the entire transaction succeeds.
-   Durable: Messages successfully sent are not lost or dropped, and are
    guaranteed to be delivered.
-   Asynchronous: Messages are sent and received ("acked") in different
    transactions.
-   Triggering: Messages usually cause some receiver to immediately wake up for
    processing.

The unit of transmission and communication within a queue is called a Message.
Each message represents a unit of work that the application needs to complete,
and Spanner ensures that the work eventually gets successfully done. Messages
are identified by a sender-chosen key (which must be unique) and contain a
payload. Messages are sent within a transaction, and after commit, are delivered
asynchronously to a Receiver. The Receiver then handles message processing and
acks messages in a separate transaction.

### When to use a Queue

#### Queues are appropriate whenever you have asynchronous work that needs to get done. For example you might need to...

-   Integrate with an external system. (e.g., ecommerce checkout, inventory
    management)
-   Defer expensive work such as large recomputation or generating a report.
-   Fan out updates across your database. (e.g., deliver calendar invites to
    many invitees)

#### Queues are not a particularly good fit if:

-   You have multiple Receivers / Subscribers that you need notified. (Use
    pubsub instead.)
-   You want to build long-lived, in-order change logs. (Use regular Spanner
    tables instead.)
-   You depend on strict realtime notifications. (Use a custom gRPC solution for
    this.)

### Example:

#### Create a Queue

```java
try {
    Queue.createQueue(dbAdminClient, dbId, queueName);
} catch (SpannerException e) {
    log.error("Could not create Queue table in spanner", e);
}

```

#### Send a Message

```java

MyProto myProto = MyProto.newBuilder().setMessage("My-Message").build();

try {
  Queue.send(dbClient, queueName, "myKey", ByteArray.copyFrom(myProto.toByteArray()));
} catch (SpannerException e) {
  log.error("Could not write message to Queue", e);
}
```

#### Receive a Message

```java
// Create the Async processing callback
QueueMessageCallback cb = messages -> {
    messages.forEach(m -> {
    Optional<MyProto> myProto = Optional.of(MyProto.parseFrom(m.value().asInputStream()));
      // ... Work with message
      Queue.ack(dbClient, m);
    });
}

// Register the receiver
Queue.recieve(dbClient, queueName, 20, 500, cb);

```

## Publishing Options

By default, `spez` is configured to publish polled data to pub / sub. This
allows you to take advantage of cloud function triggers in your event sourced
ledger. This comes at the expense of additional cost and expense. If you do not
need that functionality, you can set the "publishToPubSub" config option to
`false`, and spez will publish all of your records to a queue named tableName +
"_queue". This is simpler and more performant but could cause additional
resource consumption on your spanner instance.

## Replay from Google Cloud Storage

If you leverage the archiver.go cloud function and / or provide your own archive
to GCS (and follow the same guidelines, specifically the Timestamp metadata 
tag attached to the Blob) you can replay your events from a point in time to 
another point in time. These options are configurable via the config maps or 
the environment variables. There are 3 options for replay:
 - Replay To PubSub
 - Replay To Spez Queue
 - Replay to Spanner Table

Replaying to PubSub and Spanner Queue will allow you to ingest events just like
you would when they were written.

Replaying to a new Spanner Table will enable you to get a point in time snapshot
of the state of the system at a given point in time. If you set the point in
time timestamp to the current time or some point in the future, you can leverage
the replay to Spanner Table to back up and restore your Table in a different
region or for staging env, etc. 

## JMX Monitoring

For monitoring and debugging of the Spez poller, forward the JMX port (9010) to
your local PC via kubectl and then open jconsole or jVisualVM:

```bash
kubectl port-forward <your-app-pod> 9010

## Open jconsole connection to your local port 9010:
jconsole 127.0.0.1:9010
```
