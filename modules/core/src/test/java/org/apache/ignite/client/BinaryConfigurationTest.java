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

package org.apache.ignite.client;

import static org.apache.ignite.internal.binary.BinaryUtils.FLAG_COMPACT_FOOTER;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.client.thin.AbstractThinClientTest;
import org.junit.Test;

/**
 * Tests binary configuration behavior.
 */
public class BinaryConfigurationTest extends AbstractThinClientTest {
    // TODO: See BinaryConfigurationRetrievalTest


    @Override
    protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    @Test
    public void testAutoBinaryConfigurationEnabledRetrievesValuesFromServer() throws Exception {
        try (Ignite server = startGrid(0); IgniteClient client = startClient(0)) {
            client.getOrCreateCache("c").put(1, new Person(1, "1"));

            BinaryObjectImpl res = server.<Integer, BinaryObjectImpl>cache("c").get(1);
            assertEquals(true, res.isFlagSet(FLAG_COMPACT_FOOTER));
        }
    }

    @Test
    public void testAutoBinaryConfigurationDisabledKeepsClientSettingsAsIs() {

    }
}
