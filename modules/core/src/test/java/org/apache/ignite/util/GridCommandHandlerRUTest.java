/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.util;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.processors.ru.RollingUpgradeModeChangeResult.Result;
import org.apache.ignite.internal.visor.ru.VisorRollingUpgradeChangeModeResult;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.processors.ru.RollingUpgradeModeChangeResult.Result.FAIL;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Command handler tests for rolling-upgrade.
 */
public class GridCommandHandlerRUTest extends GridCommandHandlerAbstractTest {
    /** */
    private boolean addExtraArguments;

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName() {
        return "gridCommandHandlerTest";
    }

    /** {@inheritDoc} */
    @Override protected void addExtraArguments(List<String> args) {
        if (addExtraArguments)
            super.addExtraArguments(args);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        addExtraArguments = true;
    }

    /**
     * Tests that enabling rolling upgrade is possible without auto confirmation flag.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEnablingRollingUpgradeWithoutAutoConfirmation() throws Exception {
        Ignite ignite = startGrid(0);

        addExtraArguments = false;

        CommandHandler hnd = new CommandHandler();

        // Should fail with UnsupportedOperationException.
        assertEquals(EXIT_CODE_OK, execute(hnd, "--rolling-upgrade", "on"));

        checkFailedRollingUpgradeChangeModeResult(
            hnd.getLastOperationResult(),
            "Enabling rolling upgrade should fail",
            UnsupportedOperationException.class);

        // Should fail with UnsupportedOperationException.
        assertEquals(EXIT_CODE_OK, execute(hnd, "--rolling-upgrade", "on", "force"));

        checkFailedRollingUpgradeChangeModeResult(
            hnd.getLastOperationResult(),
            "Enabling rolling upgrade should fail",
            UnsupportedOperationException.class);

        // Should fail with UnsupportedOperationException.
        assertEquals(EXIT_CODE_OK, execute(hnd, "--rolling-upgrade", "on", "force", "--yes"));

        checkFailedRollingUpgradeChangeModeResult(
            hnd.getLastOperationResult(),
            "Enabling rolling upgrade should fail",
            UnsupportedOperationException.class);

        // Should fail with IllegalArgumentException.
        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute(hnd, "--rolling-upgrade", "on", "unknown-parameter"));
    }

    /**
     * Tests enabling/disabling rolling upgrade.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRollingUpgrade() throws Exception {
        Ignite ignite = startGrid(0);

        CommandHandler hnd = new CommandHandler();

        // Apache Ignite does not support rolling upgrade from out of the box.
        assertEquals(EXIT_CODE_OK, execute(hnd, "--rolling-upgrade", "on"));

        VisorRollingUpgradeChangeModeResult res = hnd.getLastOperationResult();

        assertTrue("Enabling rolling upgrade should fail [res=" + res + ']', FAIL == res.getResult());
        assertEquals(
            "The cause of the failure should be UnsupportedOperationException [cause=" + res.getCause() + ']',
            res.getCause().getClassName(), UnsupportedOperationException.class.getName());

        assertEquals(EXIT_CODE_OK, execute(hnd, "--rolling-upgrade", "off"));

        res = hnd.getLastOperationResult();

        assertTrue("Disabling rolling upgrade should fail [res=" + res + ']', FAIL == res.getResult());
        assertEquals(
            "The cause of the failure should be UnsupportedOperationException [cause=" + res.getCause() + ']',
            res.getCause().getClassName(), UnsupportedOperationException.class.getName());
    }

    /**
     * Tests execution of '--rolling-upgrade state' command.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRollingUpgradeStatus() throws Exception {
        Ignite ignite = startGrid(0);

        injectTestSystemOut();

        assertEquals(EXIT_CODE_OK, execute("--rolling-upgrade", "status"));

        String out = testOut.toString();

        assertContains(log, out, "Rolling upgrade is disabled");
    }

    /**
     * Checks that the given {@code res} has {@link Result#FAIL} status due to unsupported operation exception.
     *
     * @param res Change mode result.
     * @param msg Error message.
     * @param eCls Class of expected exception.
     */
    private void checkFailedRollingUpgradeChangeModeResult(
        VisorRollingUpgradeChangeModeResult res,
        String msg,
        Class eCls) {
        assertTrue(msg + " [res=" + res + ']', FAIL == res.getResult());
        assertEquals(
            "The cause of the failure should be " + eCls.getName() + " [cause=" + res.getCause() + ']',
            res.getCause().getClassName(), eCls.getName());

    }
}
