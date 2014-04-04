/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.checkpoint.s3;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.GridLogger;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.checkpoint.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.spi.*;

/**
 * Grid S3 checkpoint SPI self test.
 */
@GridSpiTest(spi = GridS3CheckpointSpi.class, group = "Checkpoint SPI")
public class GridS3CheckpointSpiSelfTest extends GridSpiAbstractTest<GridS3CheckpointSpi> {
    /** */
    private static final int CHECK_POINT_COUNT = 10;

    /** */
    private static final String KEY_PREFIX = "testCheckpoint";

    /** {@inheritDoc} */
    @Override protected void spiConfigure(GridS3CheckpointSpi spi) throws Exception {
        AWSCredentials cred = new BasicAWSCredentials(GridTestProperties.getProperty("amazon.access.key"),
            GridTestProperties.getProperty("amazon.secret.key"));

        spi.setAwsCredentials(cred);

        spi.setBucketNameSuffix("test");

        super.spiConfigure(spi);
    }

    /**
     * @throws Exception If error.
     */
    @Override protected void afterSpiStopped() throws Exception {
        AWSCredentials cred = new BasicAWSCredentials(GridTestProperties.getProperty("amazon.access.key"),
            GridTestProperties.getProperty("amazon.secret.key"));

        AmazonS3 s3 = new AmazonS3Client(cred);

        String bucketName = GridS3CheckpointSpi.BUCKET_NAME_PREFIX + "test";

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
            throw new GridSpiException("Failed to read checkpoint bucket: " + bucketName, e);
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
                @Override public void applyx() throws GridException {
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
                @Override public void applyx() throws GridException {
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
                @Override public void applyx() throws GridException {
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
            @Override public void applyx() throws GridException {
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
            @Override public void applyx() throws GridException {
                assertNull(getSpi().loadCheckpoint(KEY_PREFIX));
            }
        });
    }

    /**
     * Wrapper around {@link GridTestUtils#retryAssert(GridLogger, int, long, GridAbsClosure)}.
     * Provides s3-specific timeouts.
     * @param assertion Closure with assertion inside.
     * @throws GridInterruptedException If was interrupted.
     */
    private void assertWithRetries(GridAbsClosureX assertion) throws GridInterruptedException {
        GridTestUtils.retryAssert(log, 6, 5000, assertion);
    }
}
