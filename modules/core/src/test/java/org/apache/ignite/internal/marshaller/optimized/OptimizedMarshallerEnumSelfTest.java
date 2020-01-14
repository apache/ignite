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

package org.apache.ignite.internal.marshaller.optimized;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class OptimizedMarshallerEnumSelfTest {

    private String igniteHome = System.getProperty("user.dir");

    private final IgniteLogger rootLog = new GridTestLog4jLogger(false);

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEnumSerialisation() throws Exception {
        OptimizedMarshaller marsh = new OptimizedMarshaller();

        MarshallerContextTestImpl context = new MarshallerContextTestImpl();

        context.onMarshallerProcessorStarted(newContext(), null);

        marsh.setContext(context);

        byte[] bytes = marsh.marshal(TestEnum.Bond);

        TestEnum unmarshalled = marsh.unmarshal(bytes, Thread.currentThread().getContextClassLoader());

        assertEquals(TestEnum.Bond, unmarshalled);
        assertEquals(TestEnum.Bond.desc, unmarshalled.desc);
    }

    private GridKernalContext newContext() throws IgniteCheckedException {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteHome(igniteHome);
        cfg.setClientMode(false);

        return new GridTestKernalContext(rootLog.getLogger(OptimizedMarshallerEnumSelfTest.class), cfg);
    }

    private enum TestEnum {
        Equity("Equity") {
            @Override public String getTestString() {
                return "eee";
            }
        },

        Bond("Bond") {
            @Override public String getTestString() {
                return "qqq";
            }
        };

        public final String desc;

        TestEnum(String desc) {
            this.desc = desc;
        }

        public abstract String getTestString();
    }
}
