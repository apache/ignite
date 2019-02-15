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

package org.apache.ignite.testsuites;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.spi.discovery.tcp.ipfinder.gce.TcpDiscoveryGoogleStorageIpFinderSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Google Compute Engine integration tests.
 */
@RunWith(AllTests.class)
public class IgniteGCETestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Google Compute Engine Integration Test Suite");

        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryGoogleStorageIpFinderSelfTest.class));

        return suite;
    }

    /**
     * @return Service Account Id.
     */
    public static String getServiceAccountId() {
        String id = System.getenv("test.gce.account.id");

        assert id != null : "Environment variable 'test.gce.account.id' is not set";

        return id;
    }

    /**
     * @return Service Account p12 file path.
     */
    public static String getP12FilePath() {
        String path = System.getenv("test.gce.p12.path");

        assert path != null : "Environment variable 'test.gce.p12.path' is not set";

        return path;
    }

    /**
     * @return GCE project name.
     */
    public static String getProjectName() {
        String name = System.getenv("test.gce.project.name");

        assert name != null : "Environment variable 'test.gce.project.name' is not set";

        return name;
    }
}
