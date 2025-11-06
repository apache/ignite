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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.management.rollingupgrade.RollingUpgradeCommand;
import org.apache.ignite.internal.management.rollingupgrade.RollingUpgradeTaskResult;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteProductVersion;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_BUILD_VER;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;

/** Tests {@link RollingUpgradeCommand} command. */
public class RollingUpgradeCommandTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** */
    public static final String ENABLE = "enable";

    /** */
    public static final String DISABLE = "disable";

    /** */
    public static final String ROLLING_UPGRADE = "--rolling-upgrade";

    /** */
    public static final String TARGET_VERSION = "--target-version";

    /** */
    @Test
    public void testEnableAndDisable() {
        IgniteProductVersion curVer = IgniteProductVersion.fromString(crd.localNode().attribute(ATTR_BUILD_VER));

        String targetVerStr = curVer.major() + "." + (curVer.minor() + 1) + ".0";
        IgniteProductVersion targetVer = IgniteProductVersion.fromString(targetVerStr);

        int res = execute(ROLLING_UPGRADE, ENABLE, TARGET_VERSION, targetVerStr);

        assertEquals(EXIT_CODE_OK, res);

        RollingUpgradeTaskResult taskRes = (RollingUpgradeTaskResult)lastOperationResult;

        assertNotNull(taskRes);
        assertNull(taskRes.exception());
        assertTrue(taskRes.enabled());

        assertEquals(curVer, taskRes.currentVersion());
        assertEquals(targetVer, taskRes.targetVersion());

        assertTrue(crd.context().rollingUpgrade().enabled());

        res = execute(ROLLING_UPGRADE, DISABLE);

        assertEquals(EXIT_CODE_OK, res);

        taskRes = (RollingUpgradeTaskResult)lastOperationResult;

        assertNotNull(taskRes);
        assertFalse(taskRes.enabled());
        assertNull(taskRes.exception());
        assertNull(taskRes.currentVersion());
        assertNull(taskRes.targetVersion());

        assertFalse(crd.context().rollingUpgrade().enabled());
    }

    /** */
    @Test
    public void testDoubleDisable() {
        int res = execute(ROLLING_UPGRADE, DISABLE);

        assertEquals(EXIT_CODE_OK, res);
        RollingUpgradeTaskResult taskRes = (RollingUpgradeTaskResult)lastOperationResult;

        assertNotNull(taskRes);
        assertFalse(taskRes.enabled());
        assertNull(taskRes.exception());

        res = execute(ROLLING_UPGRADE, DISABLE);

        assertEquals(EXIT_CODE_OK, res);
        taskRes = (RollingUpgradeTaskResult)lastOperationResult;

        assertNotNull(taskRes);
        assertFalse(taskRes.enabled());
        assertNull(taskRes.exception());
        assertNull(taskRes.currentVersion());
        assertNull(taskRes.targetVersion());
        assertFalse(crd.context().rollingUpgrade().enabled());
    }

    /** */
    @Test
    public void testEnableWithDifferentTargetVersions() {
        IgniteProductVersion curVer = IgniteProductVersion.fromString(crd.localNode().attribute(ATTR_BUILD_VER));

        String targetVerStr = curVer.major() + "." + (curVer.minor() + 1) + ".0";
        IgniteProductVersion targetVer = IgniteProductVersion.fromString(targetVerStr);

        execute(ROLLING_UPGRADE, ENABLE, TARGET_VERSION, targetVerStr);

        String anotherTargetVerStr = curVer.major() + "." + curVer.minor() + "." + (curVer.maintenance() + 1);

        int res = execute(ROLLING_UPGRADE, ENABLE, TARGET_VERSION, anotherTargetVerStr);

        assertEquals(EXIT_CODE_OK, res);
        RollingUpgradeTaskResult taskRes = (RollingUpgradeTaskResult)lastOperationResult;

        assertNotNull(taskRes);
        assertNotNull(taskRes.exception());
        assertTrue(X.hasCause(taskRes.exception(),
            "Rolling upgrade is already enabled with a different current and target version",
            IgniteCheckedException.class));
        assertTrue(taskRes.enabled());
        assertEquals(targetVer, taskRes.targetVersion());

        assertTrue(crd.context().rollingUpgrade().enabled());
    }
}
