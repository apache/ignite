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

package org.apache.ignite.table;

import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;

/**
 * Tuple represents arbitrary set of columns whose values is accessible by column name.
 * <p>
 * Provides specialized method for some value-types to avoid boxing/unboxing.
 */
public interface Tuple {
    /**
     * Returns {@code true} if this tuple contains a column with the specified name.
     *
     * @param colName Column name.
     * @param def Default value.
     * @param <T> Column default value type.
     * @return Column value if this tuple contains a column with the specified name. Otherwise returns {@code default}.
     */
    <T> T valueOrDefault(String colName, T def);

    /**
     * Gets column value for given column name.
     *
     * @param colName Column name.
     * @param <T> Value type.
     * @return Column value.
     */
    <T> T value(String colName);

    /**
     * Gets binary object column.
     *
     * @param colName Column name.
     * @return Column value.
     */
    BinaryObject binaryObjectField(String colName);

    /**
     * Gets {@code byte} column value.
     *
     * @param colName Column name.
     * @return Column value.
     */
    byte byteValue(String colName);

    /**
     * Gets {@code short} column value.
     *
     * @param colName Column name.
     * @return Column value.
     */
    short shortValue(String colName);

    /**
     * Gets {@code int} column value.
     *
     * @param colName Column name.
     * @return Column value.
     */
    int intValue(String colName);

    /**
     * Gets {@code long} column value.
     *
     * @param colName Column name.
     * @return Column value.
     */
    long longValue(String colName);

    /**
     * Gets {@code float} column value.
     *
     * @param colName Column name.
     * @return Column value.
     */
    float floatValue(String colName);

    /**
     * Gets {@code double} column value.
     *
     * @param colName Column name.
     * @return Column value.
     */
    double doubleValue(String colName);

    /**
     * Gets {@code String} column value.
     *
     * @param colName Column name.
     * @return Column value.
     */
    String stringValue(String colName);

    /**
     * Gets {@code UUID} column value.
     *
     * @param colName Column name.
     * @return Column value.
     */
    UUID uuidValue(String colName);

    /**
     * Gets {@code BitSet} column value.
     *
     * @param colName Column name.
     * @return Column value.
     */
    BitSet bitmaskValue(String colName);
}
