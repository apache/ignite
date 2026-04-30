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

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class DiscoverySerializationExceptionLoggedTest extends GridCommonAbstractTest {
    /** */
    private ListeningTestLogger lsnrLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(lsnrLog);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        lsnrLog = new ListeningTestLogger(log);

        startGrids(2);
    }

    /** */
    @Test
    public void testSerdesExceptionLogged() throws Exception {
        // ??? Why MessageWrapper?
        LogListener serdesErrLsnr = LogListener
            .matches("No registration for class " + TestMessage.class.getSimpleName())
            .build();

        lsnrLog.registerListener(serdesErrLsnr);

        grid(1).context().discovery().sendCustomEvent(new TestMessage(""));

        assertTrue(serdesErrLsnr.check(getTestTimeout() / 2));
    }

    /** */
    @Test
    public void testSerdesExceptionLoggedOnClient() throws Exception {
        LogListener serdesErrLsnr = LogListener
            .matches("No registration for class " + TestMessage.class.getSimpleName())
            .build();

        lsnrLog.registerListener(serdesErrLsnr);

        startClientGrid(3).context().discovery().sendCustomEvent(new TestMessage(""));

        assertTrue(serdesErrLsnr.check(getTestTimeout() / 2));
    }

}
