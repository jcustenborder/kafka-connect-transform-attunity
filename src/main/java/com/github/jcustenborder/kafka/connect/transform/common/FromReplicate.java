/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.transform.common;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class FromReplicate<R extends ConnectRecord<R>> implements Transformation<R> {
  private FromReplicateConfig config;

  private static final String DATA_FIELD = "data";
  private static final String HEADER_FIELD = "headers";
  private static final String OPERATION_FIELD = "operation";
  private static final String DELETE_OPERATION = "DELETE";

  @Override
  public R apply(R input) {
    if (input.value() instanceof Struct) {
      Struct dataRecord = (Struct) input.value();
      Struct headers = dataRecord.getStruct(HEADER_FIELD);

      final String operation = headers.getString(OPERATION_FIELD);

      if (DELETE_OPERATION.equals(operation)) {
        if (this.config.filterDeletedEnabled) {
          return null;
        } else {
          return input.newRecord(
              input.topic(),
              input.kafkaPartition(),
              input.keySchema(),
              input.key(),
              null,
              null,
              input.timestamp(),
              input.headers()
          );
        }
      } else {
        Struct data = dataRecord.getStruct(DATA_FIELD);
        return input.newRecord(
            input.topic(),
            input.kafkaPartition(),
            input.keySchema(),
            input.key(),
            data.schema(),
            data,
            input.timestamp(),
            input.headers()
        );
      }
    } else {
      return input;
    }
  }

  @Override
  public ConfigDef config() {
    return FromReplicateConfig.config();
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> settings) {
    this.config = new FromReplicateConfig(settings);
  }
}
