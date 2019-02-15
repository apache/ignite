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

import org.apache.ignite.spi.checkpoint.s3.S3CheckpointManagerSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpiConfigSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpiSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpiStartStopBucketEndpointSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpiStartStopSSEAlgorithmSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpiStartStopSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3SessionCheckpointSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinderAwsCredentialsProviderSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinderAwsCredentialsSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinderBucketEndpointSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinderClientSideEncryptionSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinderKeyPrefixSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinderSSEAlgorithmSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.client.DummyObjectListingTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.client.DummyS3ClientTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.encrypt.AsymmetricKeyEncryptionServiceTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.encrypt.AwsKmsEncryptionServiceTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.encrypt.MockEncryptionServiceTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.encrypt.SymmetricKeyEncryptionServiceTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * S3 integration tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Checkpoint SPI.
    S3CheckpointSpiConfigSelfTest.class,
    S3CheckpointSpiSelfTest.class,
    S3CheckpointSpiStartStopSelfTest.class,
    S3CheckpointManagerSelfTest.class,
    S3SessionCheckpointSelfTest.class,
    S3CheckpointSpiStartStopBucketEndpointSelfTest.class,
    S3CheckpointSpiStartStopSSEAlgorithmSelfTest.class,

    // S3 Encryption tests.
    MockEncryptionServiceTest.class,
    AwsKmsEncryptionServiceTest.class,
    SymmetricKeyEncryptionServiceTest.class,
    AsymmetricKeyEncryptionServiceTest.class,

    // S3 IP finder.
    DummyS3ClientTest.class,
    DummyObjectListingTest.class,
    TcpDiscoveryS3IpFinderAwsCredentialsSelfTest.class,
    TcpDiscoveryS3IpFinderAwsCredentialsProviderSelfTest.class,
    TcpDiscoveryS3IpFinderBucketEndpointSelfTest.class,
    TcpDiscoveryS3IpFinderSSEAlgorithmSelfTest.class,
    TcpDiscoveryS3IpFinderKeyPrefixSelfTest.class,
    TcpDiscoveryS3IpFinderClientSideEncryptionSelfTest.class,
})
public class IgniteS3TestSuite {
    /**
     * @return Access key.
     */
    public static String getAccessKey() {
        return getRequiredEnvVar("test.amazon.access.key");
    }

    /**
     * @return Access key.
     */
    public static String getSecretKey() {
        return getRequiredEnvVar("test.amazon.secret.key");
    }

    /**
     * @param dfltBucketName Default bucket name.
     * @return Bucket name.
     */
    public static String getBucketName(final String dfltBucketName) {
        String val = System.getenv("test.s3.bucket.name");

        return val == null ? dfltBucketName : val;
    }

    /**
     * @param name Name of environment.
     * @return Environment variable value.
     */
    private static String getRequiredEnvVar(String name) {
        String key = System.getenv(name);

        assert key != null : String.format("Environment variable '%s' is not set", name);

        return key;
    }
}
