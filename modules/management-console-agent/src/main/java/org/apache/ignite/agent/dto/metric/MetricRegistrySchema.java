/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.dto.metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.copyMemory;
import static org.apache.ignite.internal.util.GridUnsafe.getByte;
import static org.apache.ignite.internal.util.GridUnsafe.getInt;
import static org.apache.ignite.internal.util.GridUnsafe.putByte;
import static org.apache.ignite.internal.util.GridUnsafe.putInt;

/**
 * Metric registry schema defines format of data for particular
 * {@link org.apache.ignite.internal.processors.metric.MetricRegistry} subclass.
 *
 * It is high-level metric registry schema representation and can be converted to compact byte array form
 * (binary representation).
 *
 * On high-level metric registry schema is list of {@link MetricRegistrySchemaItem} instances.
 * Each instance describes one particular metric's value.
 *
 * Format:
 * 0 - int - schema size in bytes.
 *
 * 4 - byte - value type
 * 5 - int - name size (k)
 * 9 - byte[k] - name bytes
 * ... repeat ...
 */
//TODO GG-25070: add version to the metric registry schema (should also contain node version for mixed clusters)
public class MetricRegistrySchema {
    /** Schema length in bytes. */
    static final int SCHEMA_LEN_SIZE = Integer.BYTES;

    /** Size of value type field in bytes. */
    static final int VALUE_TYPE_SIZE = Byte.BYTES;

    /** Size of name size field in bytes. */
    static final int NAME_LEN_SIZE = Integer.BYTES;

    /** Metric registry schema items. */
    private final List<MetricRegistrySchemaItem> items;

    /** Size of schema in binary representation. */
    private final int len;

    /**
     * @param items List of schema items.
     * @param len Size of schema in binary representation.
     */
    private MetricRegistrySchema(List<MetricRegistrySchemaItem> items, int len) {
        this.items = items;
        this.len = len;
    }

    /**
     * @return List of schema items.
     */
    public List<MetricRegistrySchemaItem> items() {
        return Collections.unmodifiableList(items);
    }

    /**
     * @param arr Binary representation of the registry schema.
     * @param off Offset in the {@code arr}.
     * @param len Size of schema in binary representation.
     * @return Schema representation.
     */
    public static MetricRegistrySchema fromBytes(byte[] arr, int off, int len) {
        if (len > arr.length - off) {
            throw new IllegalArgumentException("Schema can't be converted from byte array. " +
                    "Schema size is greater then size of array [len=" + len +
                    ", arr.length=" + arr.length + ", off=" + off + ']');
        }

        List<MetricRegistrySchemaItem> items = new ArrayList<>();

        off += SCHEMA_LEN_SIZE;

        for (int lim = off + len - SCHEMA_LEN_SIZE; off < lim;) {
            byte type = getByte(arr, BYTE_ARR_OFF + off);

            off += VALUE_TYPE_SIZE;

            int nameSize = getInt(arr, BYTE_ARR_OFF + off);

            off += NAME_LEN_SIZE;

            String name = new String(arr, off, nameSize, UTF_8);

            off += nameSize;

            MetricType metricType = MetricType.findByType(type);

            MetricRegistrySchemaItem item = new MetricRegistrySchemaItem(name, metricType);

            items.add(item);
        }

        return new MetricRegistrySchema(items, len);
    }

    /**
     * Converts byte representation of the scheme to the high-level representation.
     *
     * @param arr Byte array with compact schema representation.
     * @return High-level schema representation.
     */
    public static MetricRegistrySchema fromBytes(byte[] arr) {
        return fromBytes(arr, 0, arr.length);
    }

    /**
     * Converts high-level schema representation to byte array.
     *
     * @return Compact schema representation.
     */
    public byte[] toBytes() {
        byte[] arr = new byte[len];

        toBytes(arr, 0);

        return arr;
    }

    /**
     * Writes the schema directly to the given array.
     *
     * @param arr Array to write the schema to.
     * @param off Offset from which the schema will be written.
     */
    public void toBytes(byte[] arr, int off) {
        if (len > arr.length - off) {
            throw new IllegalArgumentException("Schema can't be converted to byte array. " +
                    "Schema size is greater then size of array [estimatedLen=" + len +
                    ", arr.length=" + arr.length + ", off=" + off + ']');
        }

        putInt(arr, BYTE_ARR_OFF + off, len);

        off += SCHEMA_LEN_SIZE;

        for (int i = 0; i < items.size(); i++) {
            MetricRegistrySchemaItem item = items.get(i);

            putByte(arr, BYTE_ARR_OFF + off, item.metricType().type());

            off += VALUE_TYPE_SIZE;

            byte[] keyBytes = item.name().getBytes(UTF_8);

            putInt(arr, BYTE_ARR_OFF + off, keyBytes.length);

            off += NAME_LEN_SIZE;

            copyMemory(keyBytes, BYTE_ARR_OFF, arr, BYTE_ARR_OFF + off, keyBytes.length);

            off += keyBytes.length;
        }
    }

    /**
     * Adds a metric with the given metric type.
     *
     * @param key Metric name.
     * @param metricType Metric type.
     */
    private void add(String key, MetricType metricType) {
        items.add(new MetricRegistrySchemaItem(key, metricType));
    }

    /**
     * @return Size of schema in binary representation.
     */
    public int length() {
        return len;
    }

    /**
     * Metrics registry builder.
     */
    public static class Builder {
        /** Collected registry schema items. */
        private List<MetricRegistrySchemaItem> items = new ArrayList<>();

        /** Size of schema in binary representation. */
        private int len;

        /**
         * @return Builder.
         */
        public static Builder newInstance() {
            return new Builder();
        }

        /**
         * @param name Metric name.
         * @param metricType Metric type.
         */
        public void add(String name, MetricType metricType) {
            if (items == null)
                throw new IllegalStateException("Builder can't be used twice.");

            MetricRegistrySchemaItem item = new MetricRegistrySchemaItem(name, metricType);

            items.add(item);

            byte[] nameBytes = name.getBytes(UTF_8);

            len += VALUE_TYPE_SIZE + NAME_LEN_SIZE + nameBytes.length;
        }

        /**
         * @return Built schema.
         */
        public MetricRegistrySchema build() {
            if (items == null)
                throw new IllegalStateException("Builder can't be used twice.");

            len += SCHEMA_LEN_SIZE;

            MetricRegistrySchema schema = new MetricRegistrySchema(items, len);

            items = null;

            return schema;
        }
    }
}
