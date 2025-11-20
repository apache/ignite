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

package org.apache.ignite.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.rollingupgrade.RollingUpgradeCommand;
import org.apache.ignite.internal.management.rollingupgrade.RollingUpgradeStatusCommand;
import org.apache.ignite.internal.management.rollingupgrade.RollingUpgradeStatusNode;
import org.apache.ignite.internal.management.rollingupgrade.RollingUpgradeTaskResult;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;

/** Tests {@link RollingUpgradeCommand} command. */
public class RollingUpgradeCommandTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** */
    public static final String ENABLE = "enable";

    /** */
    public static final String DISABLE = "disable";

    /** */
    public static final String FORCE = "--force";

    /** */
    public static final String ROLLING_UPGRADE = "--rolling-upgrade";

    /** */
    public static final String STATUS = "status";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        autoConfirmation = true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        if (crd.context().rollingUpgrade().enabled())
            crd.context().rollingUpgrade().disable();
    }

    /** */
    @Test
    public void testEnableAndDisable() {
        IgniteProductVersion curVer = IgniteProductVersion.fromString(crd.localNode().attribute(ATTR_BUILD_VER));

        String targetVerStr = curVer.major() + "." + (curVer.minor() + 1) + ".0";
        IgniteProductVersion targetVer = IgniteProductVersion.fromString(targetVerStr);

        int res = execute(ROLLING_UPGRADE, ENABLE, targetVerStr);

        assertEquals(EXIT_CODE_OK, res);

        RollingUpgradeTaskResult taskRes = (RollingUpgradeTaskResult)lastOperationResult;

        assertNull(taskRes.errorMessage());
        assertEquals(curVer, taskRes.currentVersion());
        assertEquals(targetVer, taskRes.targetVersion());

        assertTrue(crd.context().rollingUpgrade().enabled());

        res = execute(ROLLING_UPGRADE, DISABLE);

        assertEquals(EXIT_CODE_OK, res);

        taskRes = (RollingUpgradeTaskResult)lastOperationResult;

        assertNull(taskRes.errorMessage());
        assertEquals(curVer, taskRes.currentVersion());
        assertNull(taskRes.targetVersion());

        assertFalse(crd.context().rollingUpgrade().enabled());
    }

    /** */
    @Test
    public void testDoubleDisable() {
        IgniteProductVersion curVer = IgniteProductVersion.fromString(crd.localNode().attribute(ATTR_BUILD_VER));

        int res = execute(ROLLING_UPGRADE, DISABLE);

        assertEquals(EXIT_CODE_OK, res);
        RollingUpgradeTaskResult taskRes = (RollingUpgradeTaskResult)lastOperationResult;

        assertEquals(curVer, taskRes.currentVersion());
        assertNull(taskRes.targetVersion());
        assertNull(taskRes.errorMessage());

        res = execute(ROLLING_UPGRADE, DISABLE);

        assertEquals(EXIT_CODE_OK, res);
        taskRes = (RollingUpgradeTaskResult)lastOperationResult;

        assertNull(taskRes.errorMessage());
        assertEquals(curVer, taskRes.currentVersion());
        assertNull(taskRes.targetVersion());

        assertFalse(crd.context().rollingUpgrade().enabled());
    }

    /** */
    @Test
    public void testEnableWithDifferentTargetVersions() {
        IgniteProductVersion curVer = IgniteProductVersion.fromString(crd.localNode().attribute(ATTR_BUILD_VER));

        String targetVerStr = curVer.major() + "." + (curVer.minor() + 1) + ".0";
        IgniteProductVersion targetVer = IgniteProductVersion.fromString(targetVerStr);

        execute(ROLLING_UPGRADE, ENABLE, targetVerStr);

        String anotherTargetVerStr = curVer.major() + "." + curVer.minor() + "." + (curVer.maintenance() + 1);

        int res = execute(ROLLING_UPGRADE, ENABLE, anotherTargetVerStr);

        assertEquals(EXIT_CODE_OK, res);
        RollingUpgradeTaskResult taskRes = (RollingUpgradeTaskResult)lastOperationResult;

        assertNotNull(taskRes.errorMessage());
        assertTrue(taskRes.errorMessage().contains("Rolling upgrade is already enabled with a different current and target version"));

        assertEquals(curVer, taskRes.currentVersion());
        assertEquals(targetVer, taskRes.targetVersion());

        assertTrue(crd.context().rollingUpgrade().enabled());
    }

    /** */
    @Test
    public void testForceEnable() {
        IgniteProductVersion curVer = IgniteProductVersion.fromString(crd.localNode().attribute(ATTR_BUILD_VER));

        String targetVerStr = curVer.major() + "." + (curVer.minor() + 1) + "." + (curVer.maintenance() + 1);
        IgniteProductVersion targetVer = IgniteProductVersion.fromString(targetVerStr);

        int res = execute(ROLLING_UPGRADE, ENABLE, targetVerStr);

        assertEquals(EXIT_CODE_OK, res);

        RollingUpgradeTaskResult taskRes = (RollingUpgradeTaskResult)lastOperationResult;

        assertNotNull(taskRes.errorMessage());
        assertTrue(taskRes.errorMessage().contains("Minor version can only be incremented by 1"));
        assertNull(taskRes.targetVersion());

        String anotherTargetVerStr = curVer.major() + "." + curVer.minor() + "." + (curVer.maintenance() + 1);

        execute(ROLLING_UPGRADE, ENABLE, targetVerStr, FORCE);

        taskRes = (RollingUpgradeTaskResult)lastOperationResult;
        assertNull(taskRes.errorMessage());

        res = execute(ROLLING_UPGRADE, ENABLE, anotherTargetVerStr, FORCE);

        assertEquals(EXIT_CODE_OK, res);
        taskRes = (RollingUpgradeTaskResult)lastOperationResult;

        assertNotNull(taskRes.errorMessage());
        assertTrue(taskRes.errorMessage().contains("Rolling upgrade is already enabled with a different current and target version"));

        assertEquals(curVer, taskRes.currentVersion());
        assertEquals(targetVer, taskRes.targetVersion());

        assertTrue(crd.context().rollingUpgrade().enabled());
    }

    /** */
    @Test
    public void testStatusWhenDisabled() {
        int res = execute(ROLLING_UPGRADE, STATUS);

        assertEquals(EXIT_CODE_OK, res);

        RollingUpgradeTaskResult taskRes = (RollingUpgradeTaskResult)lastOperationResult;

        assertNull(taskRes.errorMessage());
        assertNull(taskRes.currentVersion());
        assertNull(taskRes.targetVersion());

        List<RollingUpgradeStatusNode> nodes = taskRes.nodes();

        assertNotNull(nodes);
        assertEquals(SERVER_NODE_CNT + 1, nodes.size());

        IgniteProductVersion curVer = IgniteProductVersion.fromString(crd.localNode().attribute(ATTR_BUILD_VER));

        nodes.forEach(node -> assertEquals(curVer, node.version()));

        RollingUpgradeStatusCommand statusCmd = new RollingUpgradeStatusCommand();

        List<String> lines = new ArrayList<>();

        statusCmd.printResult(null, taskRes, lines::add);

        List<String> expectedLines = new ArrayList<>();

        expectedLines.add("Rolling upgrade status: disabled");
        expectedLines.add("Version " + curVer + ":");

        nodes.forEach(node -> expectedLines.add("  " + node.nodeId()));

        assertEquals(expectedLines, lines);
    }

    /** */
    @Test
    public void testStatusWhenEnabled() throws Exception {
        IgniteProductVersion curVer = IgniteProductVersion.fromString(crd.localNode().attribute(ATTR_BUILD_VER));

        String targetVerStr = curVer.major() + "." + (curVer.minor() + 1) + "." + curVer.maintenance();
        IgniteProductVersion targetVer = IgniteProductVersion.fromString(targetVerStr);

        int res = execute(ROLLING_UPGRADE, ENABLE, targetVerStr);

        assertEquals(EXIT_CODE_OK, res);

        assertTrue(crd.context().rollingUpgrade().enabled());

        Consumer<IgniteConfiguration> cfgC = cfg -> {
            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi() {
                @Override public void setNodeAttributes(Map<String, Object> attrs, IgniteProductVersion ver) {
                    super.setNodeAttributes(attrs, ver);
                    attrs.put(ATTR_BUILD_VER, targetVerStr);
                }
            };

            discoSpi.setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder());
            cfg.setDiscoverySpi(discoSpi);
        };

        try (IgniteEx ignored = startGrid(SERVER_NODE_CNT + 1, cfgC)) {
            res = execute(ROLLING_UPGRADE, STATUS);
        }

        assertEquals(EXIT_CODE_OK, res);

        RollingUpgradeTaskResult taskRes = (RollingUpgradeTaskResult)lastOperationResult;

        assertNull(taskRes.errorMessage());
        assertEquals(curVer, taskRes.currentVersion());
        assertEquals(targetVer, taskRes.targetVersion());

        List<RollingUpgradeStatusNode> nodes = taskRes.nodes();

        assertNotNull(nodes);
        assertEquals(SERVER_NODE_CNT + 2, nodes.size());

        List<RollingUpgradeStatusNode> oldNodes = nodes.stream().filter(n -> n.version().equals(curVer)).collect(toList());

        List<RollingUpgradeStatusNode> newNodes = nodes.stream().filter(n -> n.version().equals(targetVer)).collect(toList());

        assertEquals(SERVER_NODE_CNT + 1, oldNodes.size());
        assertEquals(1, newNodes.size());

        RollingUpgradeStatusCommand statusCmd = new RollingUpgradeStatusCommand();

        List<String> lines = new ArrayList<>();

        statusCmd.printResult(null, taskRes, lines::add);

        List<String> expectedLines = new ArrayList<>();

        expectedLines.add("Rolling upgrade status: enabled");
        expectedLines.add("Current version: " + curVer);
        expectedLines.add("Target version: " + targetVer);

        expectedLines.add("Version " + curVer + ":");
        oldNodes.forEach(node -> expectedLines.add("  " + node.nodeId()));

        expectedLines.add("Version " + targetVer + ":");
        newNodes.forEach(node -> expectedLines.add("  " + node.nodeId()));

        assertEquals(expectedLines, lines);
    }
}
