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
import static org.apache.ignite.agent.dto.metric.MetricRegistrySchema.NAME_LEN_SIZE;
import static org.apache.ignite.agent.dto.metric.MetricRegistrySchema.SCHEMA_LEN_SIZE;
import static org.apache.ignite.agent.dto.metric.MetricRegistrySchema.VALUE_TYPE_SIZE;
import static org.junit.Assert.assertEquals;

/**
 *
 */
public class MetricRegistrySchemaTest {
    /** */
    private static final String ITEM_NAME_PREF = "item.name.";

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
        MetricRegistrySchema.Builder bldr = MetricRegistrySchema.Builder.newInstance();

        int namesSize = 0;

        for (byte i = 0; i < CNT; i++) {
            String name = ITEM_NAME_PREF + i;

            bldr.add(name, MetricType.findByType(i));

            namesSize += name.getBytes(UTF_8).length;
        }

        MetricRegistrySchema schema = bldr.build();

        int exp = SCHEMA_LEN_SIZE + VALUE_TYPE_SIZE * CNT + NAME_LEN_SIZE * CNT + namesSize;

        assertEquals(exp, schema.length());

        assertEquals(CNT, schema.items().size());
    }

    /** */
    @Test(expected = IllegalStateException.class)
    public void testAddAfterBuild() {
        MetricRegistrySchema.Builder bldr = MetricRegistrySchema.Builder.newInstance();

        for (byte i = 0; i < CNT; i++)
            bldr.add(ITEM_NAME_PREF + i, MetricType.findByType(i));

        bldr.build();

        bldr.add(DISALLOWED, MetricType.findByType((byte)0));
    }

    /** */
    @Test(expected = IllegalStateException.class)
    public void testBuildAfterBuild() {
        MetricRegistrySchema.Builder bldr = MetricRegistrySchema.Builder.newInstance();

        for (byte i = 0; i < CNT; i++)
            bldr.add(ITEM_NAME_PREF + i, MetricType.findByType(i));

        bldr.build();

        bldr.build();
    }

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void testSchemaItemsImmutable() {
        MetricRegistrySchema.Builder bldr = MetricRegistrySchema.Builder.newInstance();

        for (byte i = 0; i < CNT; i++)
            bldr.add(ITEM_NAME_PREF + i, MetricType.findByType(i));

        MetricRegistrySchema schema = bldr.build();

        schema.items().add(new MetricRegistrySchemaItem(DISALLOWED, MetricType.findByType((byte)0)));
    }

    /** */
    @Test
    public void testSchemaToBytesFromBytes() {
        MetricRegistrySchema.Builder bldr = MetricRegistrySchema.Builder.newInstance();

        for (byte i = 0; i < CNT; i++)
            bldr.add(ITEM_NAME_PREF + i, MetricType.findByType(i));

        MetricRegistrySchema schema = bldr.build();

        byte[] arr = schema.toBytes();

        assertEquals(schema.length(), arr.length);

        MetricRegistrySchema schema1 = MetricRegistrySchema.fromBytes(arr);

        assertEquals(schema.length(), schema1.length());
        assertEquals(schema.items(), schema1.items());
    }

    /** */
    @Test
    public void testSchemaToBytesFromBytesInPlace() {
        MetricRegistrySchema.Builder bldr = MetricRegistrySchema.Builder.newInstance();

        for (byte i = 0; i < CNT; i++)
            bldr.add(ITEM_NAME_PREF + i, MetricType.findByType(i));

        MetricRegistrySchema schema = bldr.build();

        byte[] arr = new byte[schema.length() + ARR_EXPANDED_DELTA];

        schema.toBytes(arr, SCHEMA_OFF);

        MetricRegistrySchema schema1 = MetricRegistrySchema.fromBytes(arr, SCHEMA_OFF, schema.length());

        assertEquals(schema.length(), schema1.length());
        assertEquals(schema.items(), schema1.items());
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testSchemaToBytesInPlaceBoundsViolated() {
        MetricRegistrySchema.Builder bldr = MetricRegistrySchema.Builder.newInstance();

        for (byte i = 0; i < CNT; i++)
            bldr.add(ITEM_NAME_PREF + i, MetricType.findByType(i));

        MetricRegistrySchema schema = bldr.build();

        byte[] arr = new byte[schema.length() + ARR_EXPANDED_DELTA];

        schema.toBytes(arr, ARR_EXPANDED_DELTA + SCHEMA_OFF / 2);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testSchemaFromBytesInPlaceBoundsViolated() {
        MetricRegistrySchema.Builder bldr = MetricRegistrySchema.Builder.newInstance();

        for (byte i = 0; i < CNT; i++)
            bldr.add(ITEM_NAME_PREF + i, MetricType.findByType(i));

        MetricRegistrySchema schema = bldr.build();

        byte[] arr = new byte[schema.length() + ARR_EXPANDED_DELTA];

        schema.toBytes(arr, SCHEMA_OFF);

        MetricRegistrySchema.fromBytes(arr, SCHEMA_OFF + ARR_EXPANDED_DELTA, schema.length());
    }
}
