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

package org.apache.ignite.marshaller;

import java.util.HashMap;
import java.util.Map;
import javax.management.BadAttributeValueExpException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientConnectionException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class ObjectInputStreamFilteringTest extends GridCommonAbstractTest {
    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testJdkMarshaller() throws Exception {
        startGrid(0);

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"))) {
            LogListener logLsnr = LogListener
                .matches("Deserialization of class javax.management.BadAttributeValueExpException is disallowed")
                .build();

            listeningLog.registerListener(logLsnr);

            GridTestUtils.assertThrowsWithCause(
                () -> client.cache("missing-cache").put("0", new BadAttributeValueExpException("val")),
                ClientConnectionException.class
            );

            assertTrue(logLsnr.check());
        }
    }

    /** */
    @Test
    public void testJavaDeserialization() throws Exception {
        startGrid(0);

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"))) {
            LogListener logLsnr = LogListener
                .matches("Failed to deserialize object [typeName=org.apache.ignite.marshaller.ObjectInputStreamFilteringTest$Holder]")
                .andMatches("filter status: REJECTED")
                .build();

            listeningLog.registerListener(logLsnr);

            HashMap excludedClsWrapper = new HashMap();
            excludedClsWrapper.put("0", new BadAttributeValueExpException("val"));

            Holder javaDeserializationObj = new Holder();

            javaDeserializationObj.map = excludedClsWrapper;

            GridTestUtils.assertThrowsWithCause(
                () -> client.cache("missing-cache").put("0", javaDeserializationObj),
                ClientConnectionException.class);

            assertTrue(logLsnr.check());
        }
    }

    /** */
    public static class Holder extends IgniteDataTransferObject {
        /** */
        @Order(0)
        public Map map;
    }
}
