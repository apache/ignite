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

package org.apache.ignite.spi.checkpoint.s3;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.checkpoint.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.spi.*;
import org.apache.ignite.testsuites.*;

/**
 * Grid S3 checkpoint SPI self test.
 */
@GridSpiTest(spi = S3CheckpointSpi.class, group = "Checkpoint SPI")
public class S3CheckpointSpiSelfTest extends GridSpiAbstractTest<S3CheckpointSpi> {
    /** */
    private static final int CHECK_POINT_COUNT = 10;

    /** */
    private static final String KEY_PREFIX = "testCheckpoint";

    /** {@inheritDoc} */
    @Override protected void spiConfigure(S3CheckpointSpi spi) throws Exception {
        AWSCredentials cred = new BasicAWSCredentials(IgniteS3TestSuite.getAccessKey(),
            IgniteS3TestSuite.getSecretKey());

        spi.setAwsCredentials(cred);

        spi.setBucketNameSuffix("test");

        super.spiConfigure(spi);
    }

    /**
     * @throws Exception If error.
     */
    @Override protected void afterSpiStopped() throws Exception {
        AWSCredentials cred = new BasicAWSCredentials(IgniteS3TestSuite.getAccessKey(),
            IgniteS3TestSuite.getSecretKey());

        AmazonS3 s3 = new AmazonS3Client(cred);

        String bucketName = S3CheckpointSpi.BUCKET_NAME_PREFIX + "test-bucket";

        try {
            ObjectListing list = s3.listObjects(bucketName);

            while (true) {
                for (S3ObjectSummary sum : list.getObjectSummaries())
                    s3.deleteObject(bucketName, sum.getKey());

                if (list.isTruncated())
                    list = s3.listNextBatchOfObjects(list);
                else
                    break;
            }
        }
        catch (AmazonClientException e) {
            throw new IgniteSpiException("Failed to read checkpoint bucket: " + bucketName, e);
        }
    }

    /**
     * @throws Exception Thrown in case of any errors.
     */
    public void testSaveLoadRemoveWithoutExpire() throws Exception {
        String dataPrefix = "Test check point data ";

        // Save states.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            GridCheckpointTestState state = new GridCheckpointTestState(dataPrefix + i);

            getSpi().saveCheckpoint(KEY_PREFIX + i, GridTestIoUtils.serializeJdk(state), 0, true);
        }

        // Load and check states.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            final String key = KEY_PREFIX + i;

            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() {
                    assertNotNull("Missing checkpoint: " + key,
                        getSpi().loadCheckpoint(key));
                }
            });

            // Doing it again as pulling value from repeated assertion is tricky,
            // and all assertions below shouldn't be retried in case of failure.
            byte[] serState = getSpi().loadCheckpoint(key);

            GridCheckpointTestState state = GridTestIoUtils.deserializeJdk(serState);

            assertNotNull("Can't load checkpoint state for key: " + key,  state);
            assertEquals("Invalid state loaded [expected='" + dataPrefix + i + "', received='" + state.getData() + "']",
                dataPrefix + i, state.getData());
        }

        // Remove states.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            final String key = KEY_PREFIX + i;

            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() {
                    assertTrue(getSpi().removeCheckpoint(key));
                }
            });
        }

        // Check that states was removed.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            final String key = KEY_PREFIX + i;

            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() {
                    assertNull(getSpi().loadCheckpoint(key));
                }
            });
        }
    }

    /**
     * @throws Exception Thrown in case of any errors.
     */
    public void testSaveWithExpire() throws Exception {
        // Save states.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            GridCheckpointTestState state = new GridCheckpointTestState("Test check point data " + i + '.');

            getSpi().saveCheckpoint(KEY_PREFIX + i, GridTestIoUtils.serializeJdk(state), 1, true);
        }

        // For small expiration intervals no warranty that state will be removed.
        Thread.sleep(100);

        // Check that states was removed.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            final String key = KEY_PREFIX + i;

            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() {
                    assertNull("Checkpoint state should not be loaded with key: " + key,
                        getSpi().loadCheckpoint(key));
                }
            });
        }
    }

    /**
     * @throws Exception Thrown in case of any errors.
     */
    public void testDuplicates() throws Exception {
        int idx1 = 1;
        int idx2 = 2;

        GridCheckpointTestState state1 = new GridCheckpointTestState(Integer.toString(idx1));
        GridCheckpointTestState state2 = new GridCheckpointTestState(Integer.toString(idx2));

        getSpi().saveCheckpoint(KEY_PREFIX, GridTestIoUtils.serializeJdk(state1), 0, true);
        getSpi().saveCheckpoint(KEY_PREFIX, GridTestIoUtils.serializeJdk(state2), 0, true);

        assertWithRetries(new GridAbsClosureX() {
            @Override public void applyx() {
                assertNotNull(getSpi().loadCheckpoint(KEY_PREFIX));
            }
        });

        byte[] serState = getSpi().loadCheckpoint(KEY_PREFIX);

        GridCheckpointTestState state = GridTestIoUtils.deserializeJdk(serState);

        assertNotNull(state);
        assertEquals(state2, state);

        // Remove.
        getSpi().removeCheckpoint(KEY_PREFIX);

        assertWithRetries(new GridAbsClosureX() {
            @Override public void applyx() {
                assertNull(getSpi().loadCheckpoint(KEY_PREFIX));
            }
        });
    }

    /**
     * Wrapper around {@link GridTestUtils#retryAssert(org.apache.ignite.IgniteLogger, int, long, GridAbsClosure)}.
     * Provides s3-specific timeouts.
     * @param assertion Closure with assertion inside.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If was interrupted.
     */
    private void assertWithRetries(GridAbsClosureX assertion) throws IgniteInterruptedCheckedException {
        GridTestUtils.retryAssert(log, 6, 5000, assertion);
    }
}
