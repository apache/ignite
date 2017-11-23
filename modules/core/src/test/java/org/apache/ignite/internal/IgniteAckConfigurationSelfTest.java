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

package org.apache.ignite.internal;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

/**
 * Test for ack IgniteConfiguration info in IgniteKernal.
 */
public class IgniteAckConfigurationSelfTest extends GridCommonAbstractTest {
    /** String logger. */
    private GridStringLogger strLog;

    /** Ignite config pattern. */
    private Pattern igniteCfgPtrn = Pattern.compile("IgniteConfiguration \\[.*\\]");

    /** Ignite config matcher. */
    private Matcher igniteCfgMatcher;

    /** Result. */
    private String res;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
        
        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(strLog = Mockito.spy(new GridStringLogger()));

        if (igniteInstanceName.equals("testAckIgniteConfigurationQuietMode"))
            when(!strLog.isQuiet()).thenReturn(true);

        return cfg;
    }

    /**
     * Test logging configuration when isQuiteMode is false.
     *
     * @throws Exception If any error occurs.
     */
    public void testAckIgniteConfigurationNonQuietMode() throws Exception {
            startGrid("testAckIgniteConfigurationNonQuietMode");

            res = strLog.toString();

            igniteCfgMatcher = igniteCfgPtrn.matcher(res);

            assertTrue(igniteCfgMatcher.find());

            stopGrid("testAckIgniteConfigurationNonQuietMode");
    }

    /**
     * Test logging configuration when isQuiteMode is true.
     *
     * @throws Exception If any error occurs.
     */
    public void testAckIgniteConfigurationQuietMode() throws Exception {
        startGrid("testAckIgniteConfigurationQuietMode");

        res = strLog.toString();

        igniteCfgMatcher = igniteCfgPtrn.matcher(res);

        assertFalse(igniteCfgMatcher.find());

        stopGrid("testAckIgniteConfigurationQuietMode");
    }

}
