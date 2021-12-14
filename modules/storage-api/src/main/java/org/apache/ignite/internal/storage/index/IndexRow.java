/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.index;

import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.SearchRow;

/**
 * Represents an Index Row - a set of indexed columns and Primary Key columns (for key uniqueness).
 */
public interface IndexRow {
    /**
     * Returns the serialized presentation of this row as a byte array.
     *
     * @return Serialized byte array value.
     */
    byte[] rowBytes();

    /**
     * Returns the Primary Key that is a part of this row.
     *
     * <p>This is a convenience method for easier extraction of the Primary Key to use it for accessing the {@link PartitionStorage}.
     *
     * @return Primary key of the associated {@link PartitionStorage}.
     */
    SearchRow primaryKey();
}
