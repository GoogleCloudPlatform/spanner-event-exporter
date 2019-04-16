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

import com.typesafe.config.Config;

/** Creates the wrapper by passing in a typesafe Config object. */
public class SpezConfig {
  public final int pollRate;
  public final String recordLimit;
  public final String instanceName;
  public final String dbName;
  public final String tableName;
  public final String startingTimestamp;
  public final String avroNamespace;
  public final boolean publishToPubSub;
  public final boolean poll;
  public final boolean replayToPubSub;
  public final String replayToPubSubStartTime;
  public final String replayToPubSubEndTime;
  public final boolean replayToQueue;
  public final String replayToQueueStartTime;
  public final String replayToQueueEndTime;
  public final boolean replayToSpanner;
  public final String replayToSpannerTableName;
  public final String replayToSpannerTimestamp;

  /**
   * Wraps the typesafe config file.
   *
   * @param config the parsed typesafe Config object
   */
  public SpezConfig(Config config) {
    this.avroNamespace = config.getString("spez.avroNamespace");
    this.instanceName = config.getString("spez.instanceName");
    this.dbName = config.getString("spez.dbName");
    this.tableName = config.getString("spez.tableName");
    this.pollRate = config.getInt("spez.pollRate");
    this.recordLimit = config.getString("spez.recordLimit");
    this.startingTimestamp = config.getString("spez.startingTimestamp");
    this.publishToPubSub = config.getBoolean("spez.publishToPubSub");
    this.poll = config.getBoolean("spez.poll");
    this.replayToPubSub = config.getBoolean("spez.replayToPubSub");
    this.replayToPubSubStartTime = config.getString("spez.replayToPubSubStartTime");
    this.replayToPubSubEndTime = config.getString("spez.replayToPubSubEndTime");
    this.replayToQueue = config.getBoolean("spez.replayToQueue");
    this.replayToQueueStartTime = config.getString("spez.replayToQueueStartTime");
    this.replayToQueueEndTime = config.getString("spez.replayToQueueEndTime");
    this.replayToSpanner = config.getBoolean("spez.replayToSpanner");
    this.replayToSpannerTableName = config.getString("spez.replayToSpannerTableName");
    this.replayToSpannerTimestamp = config.getString("spez.replayToSpannerTimestamp");
  }
}
