/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared;

public class Event {
  public String key;
  public String value;
  public long epoch;

  public Event(String key, String value, long epoch) {
    this.key = key;
    this.value = value;
    this.epoch = epoch;
  }

  @Override
  public String toString() {
    return String.format("%s:%d::%s", key, epoch, value);
  }
}
