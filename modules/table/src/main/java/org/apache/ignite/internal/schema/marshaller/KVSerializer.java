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

package org.apache.ignite.internal.schema.marshaller;

import org.apache.ignite.internal.schema.Row;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Key-value marshaller interface.
 */
public interface KVSerializer<K, V> {
    /**
     * @param obj Object to serialize.
     * @return Table row with columns set from given object.
     */
    Row serialize(@NotNull K obj, V val);

    /**
     * @param row Table row.
     * @return Deserialized key object.
     */
    @NotNull K deserializeKey(@NotNull Row row);

    /**
     * @param row Table row.
     * @return Deserialized value object.
     */
    @Nullable V deserializeValue(@NotNull Row row);
}
