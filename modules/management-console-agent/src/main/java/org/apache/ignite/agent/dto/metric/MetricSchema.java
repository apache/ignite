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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.processors.metric.MetricRegistry;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_INT_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.BYTE_ARR_OFF;
import static org.apache.ignite.internal.util.GridUnsafe.copyMemory;
import static org.apache.ignite.internal.util.GridUnsafe.getInt;
import static org.apache.ignite.internal.util.GridUnsafe.getShort;
import static org.apache.ignite.internal.util.GridUnsafe.putInt;
import static org.apache.ignite.internal.util.GridUnsafe.putShort;

/**
 * <p>
 * <ul>
 * Metric schema describes metric data set as:
 *     <li>
 *         Items - references to {@link MetricRegistrySchema} instance with metric prefix. Each item represents
 *         one data block that corresponds to the particular {@link MetricRegistry}.
 *     </li>
 *     <li>
 *         List of {@link MetricRegistrySchema} instances that could be referenced by index. Each list item corresponds
 *         to one metric source type.
 *     </li>
 * </ul>
 * </p>
 *
 * <p>Metric schema is immutable and can be constructed using {@link Builder}.</p>
 *
 * <p>
 *     Metric schema can be converted to binary representation. See {@link #toBytes()} and {@link #toBytes(byte[], int)}
 * methods. And, of course, it can be deserialized from binary representation. See {@link #fromBytes(byte[])} and
 * {@link #fromBytes(byte[], int, int)} methods.
 * </p>
 */
public class MetricSchema {
    /** Size of registry schema index field. */
    static final int REG_SCHEMA_IDX_SIZE = Short.BYTES;

    /** Size of schema items count field. */
    static final int SCHEMA_ITEM_CNT_SIZE = Short.BYTES;

    /** Size of prefix length field. */
    static final int PREF_BYTES_LEN_SIZE = Integer.BYTES;

    /** Size of registry schemas count field. */
    static final int REG_SCHEMA_CNT_SIZE = Short.BYTES;

    /** Size of registry schema offset field. */
    static final int REG_SCHEMA_OFF_SIZE = Integer.BYTES;

    /** List of items. */
    private final List<MetricSchemaItem> items;

    /** List of used registry schemas. */
    private final List<MetricRegistrySchema> regSchemas;

    /** Size of metric schema in bytes. */
    private final int len;

    /**
     * Private constructor.
     *
     * @param items Items.
     * @param regSchemas Registry schemas.
     * @param len Size of metric schema in bytes.
     */
    private MetricSchema(List<MetricSchemaItem> items, List<MetricRegistrySchema> regSchemas, int len) {
        this.items = items;
        this.regSchemas = regSchemas;
        this.len = len;
    }

    /**
     * Returns immutable list of metric schema items.
     *
     * @return Metric schema items.
     */
    public List<MetricSchemaItem> items() {
        return Collections.unmodifiableList(items);
    }

    /**
     * Returns size of metric schema in bytes.
     *
     * @return Metric schema size in bytes.
     */
    public int length() {
        return len;
    }

    /**
     * Returns metric registry schema for given index.
     *
     * @param idx Index.
     * @return Metric registry schema.
     */
    public MetricRegistrySchema registrySchema(short idx) {
        return regSchemas.get(idx);
    }

    /**
     * Converts metric registry schema to binary representation.
     *
     * @return Byte array with serialized metric schema.
     */
    public byte[] toBytes() {
        byte[] arr = new byte[len];

        toBytes(arr, 0);

        return arr;
    }

    /**
     * Converts metrics registry schema to binary representation which will be placed in given byte array.
     *
     * @param arr Target array.
     * @param off Target array offset.
     */
    public void toBytes(byte[] arr, int off) {
        if (len > arr.length - off) {
            throw new IllegalArgumentException("Schema can't be converted to byte array. " +
                    "Schema size is greater then size of array [estimatedLen=" + len +
                    ", arr.length=" + arr.length + ", off=" + off + ']');
        }

        putShort(arr, BYTE_ARR_OFF + off, (short)regSchemas.size());

        off += REG_SCHEMA_CNT_SIZE;

        int regSchemasOff = off;

        off += REG_SCHEMA_OFF_SIZE * regSchemas.size();

        putShort(arr, BYTE_ARR_OFF + off, (short)items.size());

        off += SCHEMA_ITEM_CNT_SIZE;

        for (int i = 0; i < items.size(); i++) {
            MetricSchemaItem item = items.get(i);

            putShort(arr, BYTE_ARR_OFF + off, item.index());

            off += Short.BYTES;

            byte[] prefBytes = item.prefix().getBytes(UTF_8);

            putInt(arr, BYTE_ARR_OFF + off, prefBytes.length);

            off += Integer.BYTES;

            copyMemory(prefBytes, BYTE_ARR_OFF, arr, BYTE_ARR_OFF + off, prefBytes.length);

            off += prefBytes.length;
        }

        for (int i = 0; i < regSchemas.size(); i++) {
            putInt(arr, BYTE_ARR_INT_OFF + regSchemasOff + REG_SCHEMA_OFF_SIZE * i, off - regSchemasOff);

            MetricRegistrySchema regSchema = regSchemas.get(i);

            regSchema.toBytes(arr, off);

            off += regSchema.length();
        }
    }

    /**
     * Converts byte representation of the scheme to the high-level representation.
     *
     * @param arr Source byte array.
     * @param off Source byte array offset.
     * @param len Metric schema size in bytes.
     * @return {@link MetricSchema} instance.
     */
    public static MetricSchema fromBytes(byte[] arr, int off, int len) {
        if (len > arr.length - off) {
            throw new IllegalArgumentException("Schema can't be converted from byte array. " +
                    "Schema size is greater then size of array [len=" + len +
                    ", arr.length=" + arr.length + ", off=" + off + ']');
        }

        short regSchemasCnt = getShort(arr, BYTE_ARR_OFF + off);

        off += REG_SCHEMA_CNT_SIZE;

        int regSchemasOff = off;

        off += REG_SCHEMA_OFF_SIZE * regSchemasCnt;

        List<MetricRegistrySchema> regSchemas = new ArrayList<>(regSchemasCnt);

        int regSchemasSize = 0;

        for (int i = 0; i < regSchemasCnt; i++) {
            int regSchemaOff = getInt(arr, BYTE_ARR_OFF + regSchemasOff + REG_SCHEMA_OFF_SIZE * i) + regSchemasOff;

            int regSchemaLen = getInt(arr, BYTE_ARR_OFF + regSchemaOff);

            MetricRegistrySchema regSchema = MetricRegistrySchema.fromBytes(arr, regSchemaOff, regSchemaLen);

            regSchemas.add(regSchema);

            regSchemasSize += regSchema.length();
        }

        List<MetricSchemaItem> items = new ArrayList<>();

        off += SCHEMA_ITEM_CNT_SIZE;

        for (int lim = off + len - REG_SCHEMA_CNT_SIZE - REG_SCHEMA_OFF_SIZE * regSchemasCnt - SCHEMA_ITEM_CNT_SIZE - regSchemasSize; off < lim;) {
            short idx = getShort(arr, BYTE_ARR_OFF + off);

            off += REG_SCHEMA_IDX_SIZE;

            int prefSize = getInt(arr, BYTE_ARR_OFF + off);

            off += PREF_BYTES_LEN_SIZE;

            String pref = new String(arr, off, prefSize, UTF_8);

            off += prefSize;

            MetricSchemaItem item = new MetricSchemaItem(idx, pref);

            items.add(item);
        }

        return new MetricSchema(items, regSchemas, len);
    }

    /**
     * Converts byte representation of the scheme to the high-level representation.
     *
     * @param arr Byte array with binary schema representation.
     * @return {@link MetricSchema} instance.
     */
    public static MetricSchema fromBytes(byte[] arr) {
        return fromBytes(arr, 0, arr.length);
    }

    /**
     * Builder for metric schema creation.
     */
    public static class Builder {
        /** Items. */
        private List<MetricSchemaItem> items = new ArrayList<>();

        /** Registry schemas. */
        private List<MetricRegistrySchema> regSchemas = new ArrayList<>();

        /** Index map. */
        private Map<String, Short> idxMap = new LinkedHashMap<>();

        /** Length. */
        private int len;

        /**
         * Creates new builder instance.
         *
         * @return Builder instance.
         */
        public static Builder newInstance() {
            return new Builder();
        }

        /**
         * Adds item corresponding to particular {@link MetricRegistry}.
         *
         * @param type Type of {@link MetricRegistry}.
         * @param pref Prefix.
         * @param regSchema Corresponding registry schema.
         */
        public void add(String type, String pref, MetricRegistrySchema regSchema) {
            if (items == null)
                throw new IllegalStateException("Builder can't be used twice.");

            Short idx = idxMap.get(type);

            if (idx == null) {
                idx = (short)regSchemas.size();

                idxMap.put(type, idx);

                regSchemas.add(regSchema);

                len += regSchema.length();
            }

            MetricSchemaItem item = new MetricSchemaItem(idx, pref);

            items.add(item);

            byte[] prefBytes = pref.getBytes(UTF_8);

            len += REG_SCHEMA_IDX_SIZE + PREF_BYTES_LEN_SIZE + prefBytes.length;
        }

        /**
         * Builds metric schema.
         *
         * @return Metric schema.
         */
        public MetricSchema build() {
            if (items == null)
                throw new IllegalStateException("Builder can't be used twice.");

            len += SCHEMA_ITEM_CNT_SIZE + REG_SCHEMA_CNT_SIZE + REG_SCHEMA_OFF_SIZE * regSchemas.size();

            MetricSchema schema = new MetricSchema(items, regSchemas, len);

            items = null;

            return schema;
        }
    }
}
