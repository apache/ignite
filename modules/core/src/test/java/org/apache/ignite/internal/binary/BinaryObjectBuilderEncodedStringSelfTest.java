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

package org.apache.ignite.internal.binary;

import java.util.Arrays;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.binary.BinaryStringEncoding.ENC_NAME_WINDOWS_1251;

/**
 * Binary builder test.
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class BinaryObjectBuilderEncodedStringSelfTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setCompactFooter(compactFooter());

        bCfg.setIdMapper(new BinaryBasicIdMapper(false));
        bCfg.setNameMapper(new BinaryBasicNameMapper(false));

        cfg.setBinaryConfiguration(bCfg);

        cfg.setEncoding(ENC_NAME_WINDOWS_1251);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @return Whether to use compact footer.
     */
    protected boolean compactFooter() {
        return true;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStringField() throws Exception {
        BinaryObjectBuilder builder = grid(0).binary().builder("Class");

        builder.setField("stringField", "Новгород");

        BinaryObject po = builder.build();

        assertEquals("Новгород", po.<String>field("stringField"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLossyStringField() throws Exception {
        BinaryObjectBuilder builder = grid(0).binary().builder("Class");

        builder.setField("stringField", "Düsseldorf");

        BinaryObject po = builder.build();

        assertFalse("Düsseldorf".equals(po.<String>field("stringField")));
    }


    /**
     * @throws Exception If failed.
     */
    public void testStringArrayField() throws Exception {
        BinaryObjectBuilder builder = grid(0).binary().builder("Class");

        String[] origCities = new String[] {"Новгород", "Chicago"};

        builder.setField("stringArrayField", origCities);

        BinaryObject po = builder.build();

        assertTrue(Arrays.equals(origCities, po.<String[]>field("stringArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLossyStringArrayField() throws Exception {
        BinaryObjectBuilder builder = grid(0).binary().builder("Class");

        String[] origCities = new String[] {"Düsseldorf", "北京市"};

        builder.setField("stringArrayField", origCities);

        BinaryObject po = builder.build();

        String[] binaryField = po.field("stringArrayField");

        for (int i = 0; i < origCities.length; i++)
            assertFalse(origCities[i].equals(binaryField[i]));
    }
}
