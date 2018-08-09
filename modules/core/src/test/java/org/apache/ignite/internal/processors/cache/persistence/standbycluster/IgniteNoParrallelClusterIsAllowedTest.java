/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.standbycluster;

import junit.framework.AssertionFailedError;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 *
 */
public class IgniteNoParrallelClusterIsAllowedTest extends IgniteChangeGlobalStateAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder vmIpFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * @throws Exception if failed.
     */
    public void testSimple() throws Exception {
        startPrimaryNodes(primaryNodes());

        tryToStartBackupClusterWhatShouldFail();

        primary(0).cluster().active(true);

        tryToStartBackupClusterWhatShouldFail();

        primary(0).cluster().active(false);

        tryToStartBackupClusterWhatShouldFail();

        primary(0).cluster().active(true);

        tryToStartBackupClusterWhatShouldFail();

        stopAllPrimary();

        startBackUp(backUpNodes());

        stopAllBackUp();

        startPrimaryNodes(primaryNodes());

        tryToStartBackupClusterWhatShouldFail();
    }

    /**
     *
     */
    private void tryToStartBackupClusterWhatShouldFail() {
        try {
            startBackUpNodes(backUpNodes());

            fail();
        }
        catch (AssertionFailedError er) {
                throw er;
        }
        catch (Throwable e) {
            while (true) {
                String message = e.getMessage();

                if (message.contains("Failed to acquire file lock during"))
                    break;

                if (e.getCause() != null)
                    e = e.getCause();
                else
                    fail();
            }
        }
    }

    /**
     *
     */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), testName(), true));

        cleanPersistenceDir();
    }

    /**
     *
     */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), testName(), true));
    }
}
