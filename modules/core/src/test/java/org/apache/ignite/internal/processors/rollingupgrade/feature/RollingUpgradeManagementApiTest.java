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

package org.apache.ignite.internal.processors.rollingupgrade.feature;

import org.apache.ignite.client.ClientException;
import org.apache.ignite.internal.TestCommandArgument;
import org.apache.ignite.internal.TestCommandResponse;
import org.apache.ignite.internal.TestCommandTask;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.A;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.B;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.C;
import static org.apache.ignite.internal.processors.rollingupgrade.message.TestMessage.D;

/** */
public class RollingUpgradeManagementApiTest extends AbstractRollingUpgradeManagementApiTest {
    /** */
    @Test
    public void testOlderClientCommandRunsInTheClientDialectOnUpgradedNodes() throws Exception {
        TestCommandResponse res = executeCommand("2.20.0", "2.21.0", "2.21.0");

        assertReceived("2.20.0", A, null, C, null, res);
    }

    /** */
    @Test
    public void testOlderClientCommandRunsInTheClientDialectOnJobNodeNewerThanTaskNode() throws Exception {
        TestCommandResponse res = executeCommand("2.20.0", "2.21.0", "2.21.1");

        assertReceived("2.20.0", A, null, C, null, res);
    }

    /** */
    @Test
    public void testCommandRunsInTheClientDialectWhenAllVersionsMatch() throws Exception {
        TestCommandResponse res = executeCommand("2.21.0", "2.21.0", "2.21.0");

        assertReceived("2.21.0", null, B, null, D, res);
    }

    /** */
    @Test
    public void testCommandMappedToANodeThatDroppedTheClientVersionRejected() {
        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> executeCommand("2.19.0", "2.20.0", "2.21.0"),
            ClientException.class,
            "Update binary version of the Ignite Management API");
    }

    /** */
    @Test
    public void testCommandMappedToANodeOlderThanTheClientRejected() {
        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> executeCommand("2.21.0", "2.21.0", "2.20.0"),
            ClientException.class,
            "Retry the operation after the Rolling Upgrade has completed");
    }

    /** */
    @Test
    public void testCommandMappedToANodeNewerThanTheClientAllowed() throws Exception {
        TestCommandResponse res = executeCommand("2.20.0", "2.20.0", "2.21.0");

        assertReceived("2.20.0", A, null, C, null, res);
    }

    /** */
    @Test
    public void testCommandMappedFromANewerToAnOlderNodeAllowed() throws Exception {
        TestCommandResponse res = executeCommand("2.20.0", "2.21.0", "2.20.0");

        assertReceived("2.20.0", A, null, C, null, res);
    }

    /** */
    private TestCommandResponse executeCommand(String cliVer, String connVer, String jobVer) throws Exception {
        boolean jobNodeOlder = IgniteProductVersion.fromString(jobVer).compareTo(IgniteProductVersion.fromString(connVer)) < 0;

        int connNodeIdx = jobNodeOlder ? 1 : 0;
        int jobNodeIdx = jobNodeOlder ? 0 : 1;

        startGrid(0, jobNodeOlder ? jobVer : connVer);

        if (!connVer.equals(jobVer))
            ru(0).enableVersionUpgrade();

        startGrid(1, jobNodeOlder ? connVer : jobVer);

        return executeCommandFromClient(connNodeIdx, jobNodeIdx, cliVer, TestCommandTask.class, new TestCommandArgument(A, B));
    }

    /** */
    private static void assertReceived(
        String expVer,
        @Nullable String expFldA,
        @Nullable String expFldB,
        @Nullable String expFldC,
        @Nullable String expFldD,
        TestCommandResponse res
    ) throws Exception {
        IgniteNodeFeatureSet expFeatures = createNodeFeatureSet(expVer);

        assertEquals(expFeatures, res.jobFeatures);
        assertEquals(expFeatures, res.taskFeatures);
        assertEquals(expFldA, res.fldA);
        assertEquals(expFldB, res.fldB);
        assertEquals(expFldC, res.fldC);
        assertEquals(expFldD, res.fldD);
    }
}
