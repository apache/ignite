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

import org.apache.ignite.binary.BinaryStringEncoding;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Unit tests for serialized string field comparer.
 */
public class BinarySerializedStringFieldComparatorSelfTest extends BinarySerializedFieldComparatorAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(igniteInstanceName);

        BinaryConfiguration bCfg = cfg.getBinaryConfiguration();

        if (bCfg == null) {
            bCfg = new BinaryConfiguration();

            cfg.setBinaryConfiguration(bCfg);
        }

        bCfg.setEncoding(BinaryStringEncoding.ENC_NAME_WINDOWS_1251);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /**
     * Test encoded string fields.
     *
     * @throws Exception If failed.
     */
    public void testEncodedString() throws Exception {
        checkTwoValues("str1", "str2");
    }
}