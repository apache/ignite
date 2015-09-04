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

import org.apache.ignite.GridTestTask;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static org.apache.ignite.events.EventType.EVTS_ALL;

/**
 * Test for invalid input parameters.
 */
@GridCommonTest(group = "Kernal Self")
public class GridFailedInputParametersSelfTest extends GridCommonAbstractTest {
    /** */
    private static Ignite ignite;

    /** */
    public GridFailedInputParametersSelfTest() {
        super(/*start grid*/true);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = G.ignite(getTestGridName());
    }

    /**
     * @throws Exception Thrown in case of any errors.
     */
    public void testAddEventLocalListener() throws Exception {
        try {
            ignite.events().localListen(null, EVTS_ALL);

            assert false : "Null listener can't be added.";
        }
        catch (NullPointerException ignored) {
            // No-op.
        }
    }

    /**
     * @throws Exception Thrown in case of any errors.
     */
    public void testRemoveEventLocalListener() throws Exception {
        try {
            ignite.events().stopLocalListen(null);

            assert false : "Null listener can't be removed.";
        }
        catch (NullPointerException ignored) {
            // No-op.
        }
    }

    /**
     * @throws Exception Thrown in case of any errors.
     */
    public void testAddDiscoveryListener() throws Exception {
        try {
            ignite.events().localListen(null, EVTS_ALL);

            assert false : "Null listener can't be added.";
        }
        catch (NullPointerException ignored) {
            // No-op.
        }
    }

    /**
     * @throws Exception Thrown in case of any errors.
     */
    public void testRemoveDiscoveryListener() throws Exception {
        try {
            ignite.events().stopLocalListen(null);

            assert false : "Null listener can't be removed.";
        }
        catch (NullPointerException ignored) {
            // No-op.
        }
    }

    /**
     * @throws Exception Thrown in case of any errors.
     */
    public void testGetNode() throws Exception {
        try {
            ignite.cluster().node(null);

            assert false : "Null nodeId can't be entered.";
        }
        catch (NullPointerException ignored) {
            // No-op.
        }
    }

    /**
     * @throws Exception Thrown in case of any errors.
     */
    public void testPingNode() throws Exception {
        try {
            ignite.cluster().pingNode(null);

            assert false : "Null nodeId can't be entered.";
        }
        catch (NullPointerException ignored) {
            // No-op.
        }
    }

    /**
     * @throws Exception Thrown in case of any errors.
     */
    public void testDeployTask() throws Exception {
        try {
            ignite.compute().localDeployTask(null, null);

            assert false : "Null task can't be entered.";
        }
        catch (NullPointerException ignored) {
            // No-op.
        }

        try {
            ignite.compute().localDeployTask(null, null);

            assert false : "Null task can't be entered.";
        }
        catch (NullPointerException ignored) {
            // No-op.
        }

        // Check for exceptions.
        ignite.compute().localDeployTask(GridTestTask.class, U.detectClassLoader(GridTestTask.class));
    }
}