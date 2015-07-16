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

import org.apache.ignite.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Update notifier test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridUpdateNotifierSelfTest extends GridCommonAbstractTest {
    private String updateStatusParams;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, "true");

        Properties props = U.field(IgniteProperties.class, "PROPS");

        updateStatusParams = props.getProperty("ignite.update.status.params");

        props.setProperty("ignite.update.status.params", "ver=" + IgniteProperties.get("ignite.version"));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, "false");

        Properties props = U.field(IgniteProperties.class, "PROPS");

        props.setProperty("ignite.update.status.params", updateStatusParams);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNotifier() throws Exception {
        String nodeVer = IgniteProperties.get("ignite.version");

        GridUpdateNotifier ntf = new GridUpdateNotifier(null, nodeVer,
            TEST_GATEWAY, Collections.<PluginProvider>emptyList(), false);

        ntf.checkForNewVersion(new SelfExecutor(), log);

        String ver = ntf.latestVersion();

        info("Latest version: " + ver);

        assertNotNull("Ignite latest version has not been detected.", ver);

        byte nodeMaintenance = IgniteProductVersion.fromString(nodeVer).maintenance();

        byte lastMaintenance = IgniteProductVersion.fromString(ver).maintenance();

        assertTrue("Wrong latest version.", (nodeMaintenance == 0 && lastMaintenance == 0) ||
            (nodeMaintenance > 0 && lastMaintenance > 0));

        ntf.reportStatus(log);
    }

    /**
     * Executor that runs task in current thread.
     */
    private static class SelfExecutor implements Executor {
        /** {@inheritDoc} */
        @Override public void execute(@NotNull Runnable r) {
            r.run();
        }
    }

    /**
     * Test kernal gateway that always return uninitialized user stack trace.
     */
    private static final GridKernalGateway TEST_GATEWAY = new GridKernalGateway() {
        @Override public void readLock() throws IllegalStateException {}

        @Override public void readLockAnyway() {}

        @Override public void setState(GridKernalState state) {}

        @Override public GridKernalState getState() {
            return null;
        }

        @Override public void readUnlock() {}

        @Override public void writeLock() {}

        @Override public void writeUnlock() {}

        @Override public String userStackTrace() {
            return null;
        }

        @Override public boolean tryWriteLock(long timeout) {
            return false;
        }

        @Override public GridFutureAdapter<?> onDisconnected() {
            return null;
        }

        @Override public void onReconnected() {
            // No-op.
        }
    };
}
