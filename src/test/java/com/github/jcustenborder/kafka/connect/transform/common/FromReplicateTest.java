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

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.github.jcustenborder.kafka.connect.utils.AssertStruct.assertStruct;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class FromReplicateTest {


  FromReplicate<SinkRecord> transform;

  @BeforeEach
  public void before() {
    this.transform = new FromReplicate<>();
    this.transform.configure(ImmutableMap.of());
  }


  final Schema keySchema = SchemaBuilder.struct()
      .name("key")
      .field("emp_no", Schema.OPTIONAL_INT32_SCHEMA)
      .build();

  final Schema headerSchema = SchemaBuilder.struct()
      .name("Headers")
      .optional()
      .field("operation", Schema.STRING_SCHEMA)
      .field("changeSequence", Schema.STRING_SCHEMA)
      .field("timestamp", Schema.STRING_SCHEMA)
      .field("streamPosition", Schema.STRING_SCHEMA)
      .field("transactionId", Schema.STRING_SCHEMA)
      .field("changeMask", Schema.OPTIONAL_BYTES_SCHEMA)
      .field("columnMask", Schema.OPTIONAL_BYTES_SCHEMA)
      .build();

  final Schema dataSchema = SchemaBuilder.struct()
      .name("Data")
      .optional()
      .field("emp_no", Schema.OPTIONAL_INT32_SCHEMA)
      .field("birth_date", Date.builder().optional().build())
      .field("first_name", Schema.OPTIONAL_STRING_SCHEMA)
      .field("last_name", Schema.OPTIONAL_STRING_SCHEMA)
      .field("gender", Schema.OPTIONAL_STRING_SCHEMA)
      .field("hire_date", Date.builder().optional().build())
      .build();

  final Schema dataRecordSchema = SchemaBuilder.struct()
      .name("DataRecord")
      .field("data", dataSchema)
      .field("headers", headerSchema)
      .build();


  @Test
  public void refresh() {
    Struct key = new Struct(keySchema)
        .put("emp_no", 12355);
    Struct headers = new Struct(headerSchema)
        .put("operation", "REFRESH")
        .put("changeSequence", "")
        .put("timestamp", "")
        .put("streamPosition", "")
        .put("transactionId", "")
        .put("changeMask", null)
        .put("columnMask", null);
    Struct data = new Struct(dataSchema)
        .put("emp_no", 12355)
        .put("birth_date", new java.util.Date(-39484800000L))
        .put("first_name", "LiMin")
        .put("last_name", "Yoshizawa")
        .put("gender", "M")
        .put("hire_date", new java.util.Date(1547242667123L));
    Struct value = new Struct(dataRecordSchema)
        .put("data", data)
        .put("headers", headers);

    SinkRecord input = new SinkRecord(
        "employee.employee",
        0,
        key.schema(),
        key,
        value.schema(),
        value,
        1234L
    );

    SinkRecord actual = this.transform.apply(input);
    assertNotNull(actual);
    assertStruct(key, (Struct) actual.key());
    assertStruct(data, (Struct) actual.value());
  }

  @Test
  public void delete() {
    Struct key = new Struct(keySchema)
        .put("emp_no", 12355);
    Struct headers = new Struct(headerSchema)
        .put("operation", "DELETE")
        .put("changeSequence", "")
        .put("timestamp", "")
        .put("streamPosition", "")
        .put("transactionId", "")
        .put("changeMask", null)
        .put("columnMask", null);

    Struct value = new Struct(dataRecordSchema)
        .put("data", null)
        .put("headers", headers);

    SinkRecord input = new SinkRecord(
        "employee.employee",
        0,
        key.schema(),
        key,
        value.schema(),
        value,
        1234L
    );

    SinkRecord actual = this.transform.apply(input);
    assertNotNull(actual);
    assertStruct(key, (Struct) actual.key());
    assertNull(actual.valueSchema());
    assertNull(actual.value());
  }

  @Test
  public void deleteFiltered() {
    this.transform.configure(
        ImmutableMap.of(
            FromReplicateConfig.FILTER_DELETES_CONFIG, "true"
        )
    );
    Struct key = new Struct(keySchema)
        .put("emp_no", 12355);
    Struct headers = new Struct(headerSchema)
        .put("operation", "DELETE")
        .put("changeSequence", "")
        .put("timestamp", "")
        .put("streamPosition", "")
        .put("transactionId", "")
        .put("changeMask", null)
        .put("columnMask", null);

    Struct value = new Struct(dataRecordSchema)
        .put("data", null)
        .put("headers", headers);

    SinkRecord input = new SinkRecord(
        "employee.employee",
        0,
        key.schema(),
        key,
        value.schema(),
        value,
        1234L
    );

    SinkRecord actual = this.transform.apply(input);
    assertNull(actual);
  }
}
