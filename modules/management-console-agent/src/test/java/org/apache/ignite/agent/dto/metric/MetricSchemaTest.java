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

import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.agent.dto.metric.MetricSchema.PREF_BYTES_LEN_SIZE;
import static org.apache.ignite.agent.dto.metric.MetricSchema.REG_SCHEMA_CNT_SIZE;
import static org.apache.ignite.agent.dto.metric.MetricSchema.REG_SCHEMA_IDX_SIZE;
import static org.apache.ignite.agent.dto.metric.MetricSchema.REG_SCHEMA_OFF_SIZE;
import static org.apache.ignite.agent.dto.metric.MetricSchema.SCHEMA_ITEM_CNT_SIZE;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class MetricSchemaTest {
    /** */
    private static final String ITEM_NAME_PREF = "item.name.";

    /** */
    private static final String REG_PREF = "registry.";

    /** */
    private static final int CNT = 4;

    /** */
    private static final String DISALLOWED = "disallowed";

    /** */
    private static final int ARR_EXPANDED_DELTA = 128;

    /** */
    private static final int SCHEMA_OFF = 64;

    /** */
    @Test
    public void testBuild() {
        MetricSchema.Builder bldr = MetricSchema.Builder.newInstance();

        int regSchemasSize = 0;

        int namesSize = 0;

        for (int i = 0; i < CNT; i++) {
            MetricRegistrySchema.Builder regBuilder = MetricRegistrySchema.Builder.newInstance();

            for (byte j = 0; j < CNT; j++) {
                String name = ITEM_NAME_PREF + j;

                regBuilder.add(name, MetricType.findByType(j));
            }

            MetricRegistrySchema regSchema = regBuilder.build();

            String pref = REG_PREF + i;

            //TODO: actually need sum length of all unique registry schemas
            regSchemasSize = regSchema.length();

            namesSize += pref.getBytes(UTF_8).length;

            bldr.add("regType", pref, regSchema);
        }

        MetricSchema schema = bldr.build();

        int exp = REG_SCHEMA_CNT_SIZE + REG_SCHEMA_OFF_SIZE + SCHEMA_ITEM_CNT_SIZE + REG_SCHEMA_IDX_SIZE * CNT + PREF_BYTES_LEN_SIZE * CNT +
                namesSize + regSchemasSize;

        assertEquals(exp, schema.length());

        assertEquals(CNT, schema.items().size());
    }

    /** */
    @Test(expected = IllegalStateException.class)
    public void testAddAfterBuild() {
        MetricSchema.Builder bldr = MetricSchema.Builder.newInstance();

        for (int i = 0; i < CNT; i++) {
            MetricRegistrySchema.Builder regBldr = MetricRegistrySchema.Builder.newInstance();

            for (byte j = 0; j < CNT; j++)
                regBldr.add(ITEM_NAME_PREF + j, MetricType.findByType(j));

            MetricRegistrySchema regSchema = regBldr.build();

            bldr.add("regType", REG_PREF + i, regSchema);
        }

        bldr.build();

        bldr.add("regType", DISALLOWED, MetricRegistrySchema.Builder.newInstance().build());
    }

    /** */
    @Test(expected = IllegalStateException.class)
    public void testBuildAfterBuild() {
        MetricSchema.Builder bldr = MetricSchema.Builder.newInstance();

        for (int i = 0; i < CNT; i++) {
            MetricRegistrySchema.Builder regBldr = MetricRegistrySchema.Builder.newInstance();

            for (byte j = 0; j < CNT; j++)
                regBldr.add(ITEM_NAME_PREF + j, MetricType.findByType(j));

            MetricRegistrySchema regSchema = regBldr.build();

            bldr.add("regType", REG_PREF + i, regSchema);
        }

        bldr.build();

        bldr.build();
    }

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void testSchemaItemsImmutable() {
        MetricSchema.Builder bldr = MetricSchema.Builder.newInstance();

        for (int i = 0; i < CNT; i++) {
            MetricRegistrySchema.Builder regBldr = MetricRegistrySchema.Builder.newInstance();

            for (byte j = 0; j < CNT; j++)
                regBldr.add(ITEM_NAME_PREF + j, MetricType.findByType(j));

            MetricRegistrySchema regSchema = regBldr.build();

            bldr.add("regType", REG_PREF + i, regSchema);
        }

        MetricSchema schema = bldr.build();

        schema.items().add(new MetricSchemaItem((short)0, DISALLOWED));
    }

    /** */
    @Test
    public void testSchemaToBytesFromBytes() {
        MetricSchema.Builder bldr = MetricSchema.Builder.newInstance();

        for (int i = 0; i < CNT; i++) {
            MetricRegistrySchema.Builder regBldr = MetricRegistrySchema.Builder.newInstance();

            for (byte j = 0; j < CNT; j++)
                regBldr.add(ITEM_NAME_PREF + j, MetricType.findByType(j));

            MetricRegistrySchema regSchema = regBldr.build();

            bldr.add("regType", REG_PREF + i, regSchema);
        }

        MetricSchema schema = bldr.build();

        byte[] arr = schema.toBytes();

        assertEquals(schema.length(), arr.length);

        MetricSchema schema1 = MetricSchema.fromBytes(arr);

        assertEquals(schema.length(), schema1.length());

        assertEquals(schema.items(), schema1.items());
    }

    /** */
    @Test
    public void testSchemaToBytesFromBytesInPlace() {
        MetricSchema.Builder bldr = MetricSchema.Builder.newInstance();

        for (int i = 0; i < CNT; i++) {
            MetricRegistrySchema.Builder regBldr = MetricRegistrySchema.Builder.newInstance();

            for (byte j = 0; j < CNT; j++)
                regBldr.add(ITEM_NAME_PREF + j, MetricType.findByType(j));

            MetricRegistrySchema regSchema = regBldr.build();

            bldr.add("regType", REG_PREF + i, regSchema);
        }

        MetricSchema schema = bldr.build();

        byte[] arr = new byte[schema.length() + ARR_EXPANDED_DELTA];

        schema.toBytes(arr, SCHEMA_OFF);


        MetricSchema schema1 = MetricSchema.fromBytes(arr, SCHEMA_OFF, schema.length());

        assertEquals(schema.length(), schema1.length());

        assertEquals(schema.items(), schema1.items());

    }
}
