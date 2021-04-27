/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema;

import java.util.UUID;
import org.apache.ignite.configuration.internal.ConfigurationManager;

/**
 * Schema Manager.
 */
// TODO: IGNITE-14586 Remove @SuppressWarnings when implementation provided.
@SuppressWarnings({"FieldCanBeLocal", "unused"}) public class SchemaManager {
    /** Configuration manager in order to handle and listen schema specific configuration.*/
    private final ConfigurationManager configurationMgr;

    /** Schema. */
    private final SchemaDescriptor schema;

    /**
     * The constructor.
     *
     * @param configurationMgr Configuration manager.
     */
    public SchemaManager(ConfigurationManager configurationMgr) {
        this.configurationMgr = configurationMgr;

        this.schema = new SchemaDescriptor(1,
            new Column[] {
                new Column("key", NativeType.LONG, false)
            },
            new Column[] {
                new Column("value", NativeType.LONG, false)
            }
        );
    }

    /**
     * Gets a current schema for the table specified.
     *
     * @param tableId Table id.
     * @return Schema.
     */
    public SchemaDescriptor schema(UUID tableId) {
        return schema;
    }

    /**
     * Gets a schema for specific version.
     *
     * @param tableId Table id.
     * @param ver Schema version.
     * @return Schema.
     */
    public SchemaDescriptor schema(UUID tableId, long ver) {
        assert ver >= 0;

        assert schema.version() == ver;

        return schema;
    }
}
