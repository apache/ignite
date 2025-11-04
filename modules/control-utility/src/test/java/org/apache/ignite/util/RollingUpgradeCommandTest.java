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
import org.apache.ignite.lang.IgniteProductVersion;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;

/** Tests {@link RollingUpgradeCommand} command. */
public class RollingUpgradeCommandTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** */
    public static final String ENABLE = "enable";

    /** */
    public static final String DISABLE = "disable";

    /** */
    public static final String ROLLING_UPGRADE = "--rolling-upgrade";

    /** */
    @Test
    public void testCommands() {
        int res = execute(ROLLING_UPGRADE, DISABLE);

        assertEquals(EXIT_CODE_OK, res);
        assertEquals("Rolling upgrade disabled.", lastOperationResult);

        IgniteProductVersion curVer = crd.version();

        IgniteProductVersion nextVer = new IgniteProductVersion(curVer.major(),
            (byte)(curVer.minor() + 1),
            (byte)0,
            curVer.revisionTimestamp(),
            curVer.revisionHash());

        res = execute(ROLLING_UPGRADE, ENABLE, nextVer.toString());

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, res);
        assertEquals(null, lastOperationResult);

        res = execute(ROLLING_UPGRADE, DISABLE);

        assertEquals(EXIT_CODE_OK, res);
        assertEquals("Rolling upgrade disabled.", lastOperationResult);
    }
}
