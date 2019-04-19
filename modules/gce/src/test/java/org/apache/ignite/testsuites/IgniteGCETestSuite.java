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

package org.apache.ignite.testsuites;

import org.apache.ignite.spi.discovery.tcp.ipfinder.gce.TcpDiscoveryGoogleStorageIpFinderSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Google Compute Engine integration tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({TcpDiscoveryGoogleStorageIpFinderSelfTest.class})
public class IgniteGCETestSuite {
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
