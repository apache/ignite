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

package org.apache.ignite.internal.client.table;

import java.util.BitSet;
import java.util.HashMap;
import java.util.UUID;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;

/**
 * Client tuple builder.
 */
public final class ClientTupleBuilder implements TupleBuilder, Tuple {
    /** Columns values. */
    private final HashMap<String, Object> map = new HashMap<>();

    /**
     * Gets the underlying map.
     *
     * @return Underlying map
     */
    public HashMap<String, Object> map() {
        return map;
    }

    /** {@inheritDoc} */
    @Override public TupleBuilder set(String colName, Object value) {
        map.put(colName, value);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Tuple build() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public <T> T valueOrDefault(String colName, T def) {
        return (T)map.getOrDefault(colName, def);
    }

    /** {@inheritDoc} */
    @Override public <T> T value(String colName) {
        return (T)map.get(colName);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject binaryObjectField(String colName) {
        throw new IgniteException("Not supported");
    }

    /** {@inheritDoc} */
    @Override public byte byteValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public short shortValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public int intValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public long longValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public float floatValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public double doubleValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public String stringValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public UUID uuidValue(String colName) {
        return value(colName);
    }

    /** {@inheritDoc} */
    @Override public BitSet bitmaskValue(String colName) {
        return value(colName);
    }
}
