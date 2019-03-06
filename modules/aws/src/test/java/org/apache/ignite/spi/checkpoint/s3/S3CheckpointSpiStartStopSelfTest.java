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

package org.apache.ignite.spi.checkpoint.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.ignite.spi.GridSpiStartStopAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.apache.ignite.testsuites.IgniteS3TestSuite;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Grid S3 checkpoint SPI start stop self test.
 */
@GridSpiTest(spi = S3CheckpointSpi.class, group = "Checkpoint SPI")
public class S3CheckpointSpiStartStopSelfTest extends GridSpiStartStopAbstractTest<S3CheckpointSpi> {
    /** {@inheritDoc} */
    @Override protected void spiConfigure(S3CheckpointSpi spi) throws Exception {
        AWSCredentials cred = new BasicAWSCredentials(IgniteS3TestSuite.getAccessKey(),
            IgniteS3TestSuite.getSecretKey());

        spi.setAwsCredentials(cred);

        spi.setBucketNameSuffix(S3CheckpointSpiSelfTest.getBucketNameSuffix());

        super.spiConfigure(spi);
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-2420")
    @Test
    @Override public void testStartStop() throws Exception {
        super.testStartStop();
    }
}
