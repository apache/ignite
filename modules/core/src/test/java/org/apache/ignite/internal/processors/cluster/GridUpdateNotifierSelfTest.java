/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cluster;

import java.util.Properties;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.IgniteProperties;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Update notifier test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridUpdateNotifierSelfTest extends GridCommonAbstractTest {
    /** */
    private String updateStatusParams;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 120 * 1000;
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
        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, "false");

        Properties props = U.field(IgniteProperties.class, "PROPS");

        props.setProperty("ignite.update.status.params", updateStatusParams);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNotifier() throws Exception {
        String nodeVer = IgniteProperties.get("ignite.version");

        HttpIgniteUpdatesChecker updatesCheckerMock = Mockito.mock(HttpIgniteUpdatesChecker.class);

        // Return current node version and some other info
        Mockito.when(updatesCheckerMock.getUpdates(true))
                .thenReturn("meta=meta" + "\n" + "version=" + nodeVer + "\n" + "downloadUrl=url");

        GridUpdateNotifier ntf = new GridUpdateNotifier(null, nodeVer, false, updatesCheckerMock);

        ntf.checkForNewVersion(log, true);

        String ver = ntf.latestVersion();

        // Wait 60 sec for response.
        for (int i = 0; ver == null && i < 600; i++) {
            Thread.sleep(100);

            ver = ntf.latestVersion();
        }

        info("Notifier version [ver=" + ver + ", nodeVer=" + nodeVer + ']');

        assertNotNull("Ignite latest version has not been detected.", ver);

        byte nodeMaintenance = IgniteProductVersion.fromString(nodeVer).maintenance();

        byte lastMaintenance = IgniteProductVersion.fromString(ver).maintenance();

        assertTrue("Wrong latest version [nodeVer=" + nodeMaintenance + ", lastVer=" + lastMaintenance + ']',
            (nodeMaintenance == 0 && lastMaintenance == 0) || (nodeMaintenance > 0 && lastMaintenance > 0));

        ntf.reportStatus(log);
    }
}
