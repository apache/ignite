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

import org.apache.ignite.internal.management.rollingupgrade.RollingUpgradeCommand;
import org.apache.ignite.internal.management.rollingupgrade.RollingUpgradeTaskResult;
import org.apache.ignite.internal.util.lang.IgnitePair;
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
    public void testCommands() {
        int res = execute(ROLLING_UPGRADE, DISABLE);

        assertEquals(EXIT_CODE_OK, res);

        RollingUpgradeTaskResult result = (RollingUpgradeTaskResult)lastOperationResult;

        assertNotNull(result);
        assertNull(result.exception());
        assertFalse(result.enabled());

        IgniteProductVersion curVer = IgniteProductVersion.fromString(crd.localNode().attribute(ATTR_BUILD_VER));

        String targetVerStr = curVer.major() + "." + (curVer.minor() + 1) + ".0";
        IgniteProductVersion targetVer = IgniteProductVersion.fromString(targetVerStr);

        res = execute(ROLLING_UPGRADE, ENABLE, TARGET_VERSION, targetVerStr);

        assertEquals(EXIT_CODE_OK, res);

        result = (RollingUpgradeTaskResult)lastOperationResult;

        assertNotNull(result);
        assertNull(result.exception());
        assertTrue(result.enabled());

        IgnitePair<IgniteProductVersion> versions = result.rollUpVers();

        assertNotNull(versions);
        assertEquals(curVer, versions.get1());
        assertEquals(targetVer, versions.get2());

        assertTrue(crd.context().rollingUpgrade().enabled());

        res = execute(ROLLING_UPGRADE, DISABLE);

        assertEquals(EXIT_CODE_OK, res);

        result = (RollingUpgradeTaskResult)lastOperationResult;

        assertNotNull(result);
        assertFalse(result.enabled());
        assertNull(result.exception());
        assertNull(result.rollUpVers());

        assertFalse(crd.context().rollingUpgrade().enabled());
    }
}
