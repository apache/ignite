/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.checkpoint.s3;

import com.amazonaws.auth.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.kernal.managers.checkpoint.*;
import org.gridgain.testsuites.bamboo.*;

/**
 * Checkpoint manager test using {@link GridS3CheckpointSpi}.
 */
public class GridS3CheckpointManagerSelfTest extends GridCheckpointManagerAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        assertTrue("Unexpected grid name: " + gridName, gridName.contains("s3"));

        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridS3CheckpointSpi spi = new GridS3CheckpointSpi();

        AWSCredentials cred = new BasicAWSCredentials(GridS3TestSuite.getAccessKey(),
            GridS3TestSuite.getSecretKey());

        spi.setAwsCredentials(cred);

        spi.setBucketNameSuffix("test");

        cfg.setCheckpointSpi(spi);

        return cfg;
    }

    /**
     * @throws Exception Thrown if any exception occurs.
     */
    public void testS3Based() throws Exception {
        retries = 6;

        doTest("s3");
    }

    /**
     * @throws Exception Thrown if any exception occurs.
     */
    public void testMultiNodeS3Based() throws Exception {
        retries = 6;

        doMultiNodeTest("s3");
    }
}
