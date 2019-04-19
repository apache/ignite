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

package org.apache.ignite.internal.processors.cache.persistence.standbycluster;

import junit.framework.AssertionFailedError;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class IgniteNoParrallelClusterIsAllowedTest extends IgniteChangeGlobalStateAbstractTest {
    /**
     * @throws Exception if failed.
     */
    @Test
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

                if (message.contains("Failed to acquire file lock ["))
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
