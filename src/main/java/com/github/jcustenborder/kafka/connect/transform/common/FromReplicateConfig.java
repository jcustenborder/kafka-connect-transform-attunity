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

import com.github.jcustenborder.kafka.connect.utils.config.ConfigKeyBuilder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

class FromReplicateConfig extends AbstractConfig {
  public static final String FILTER_DELETES_CONFIG = "filter.deletes.enabled";
  static final String FILTER_DELETES_DOC = "When set to true all delete operations will be " +
      "filtered from downstream connectors.";

  public final boolean filterDeletedEnabled;

  public FromReplicateConfig(Map<?, ?> originals) {
    super(config(), originals);
    this.filterDeletedEnabled = getBoolean(FILTER_DELETES_CONFIG);
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(ConfigKeyBuilder.of(FILTER_DELETES_CONFIG, ConfigDef.Type.BOOLEAN)
            .importance(ConfigDef.Importance.HIGH)
            .documentation(FILTER_DELETES_DOC)
            .defaultValue(false)
            .build()
        );
  }
}
