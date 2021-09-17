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

package org.apache.ignite.internal.configuration.schema;

import org.apache.ignite.configuration.annotation.InternalConfiguration;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.schemas.table.TableConfigurationSchema;
import org.apache.ignite.configuration.validation.Immutable;

/**
 * Extended table configuration schema class.
 */
@InternalConfiguration
// TODO: IGNITE-15480 Add id's to columns in order to properly process column renaming withing index context.
public class ExtendedTableConfigurationSchema extends TableConfigurationSchema {
    /** Table id. String representation of {@link org.apache.ignite.lang.IgniteUuid}. */
    @Value
    @Immutable
    public String id;

    /**
     * Serialized version of an affinity assignments. Currently configuration doesn't support neither collections
     * nor array of arrays, so that serialization was chosen.
     */
    @Value
    public byte[] assignments;

    /** Schemas history as named list where name is schema version and value is serialized version of schema itself. */
    @NamedConfigValue
    public SchemaConfigurationSchema schemas;
}

