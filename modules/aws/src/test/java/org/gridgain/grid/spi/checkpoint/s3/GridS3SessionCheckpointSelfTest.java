/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.checkpoint.s3;

import com.amazonaws.auth.*;
import org.gridgain.grid.*;
import org.gridgain.grid.session.*;
import org.gridgain.testsuites.bamboo.*;

/**
 * Grid session checkpoint self test using {@link GridS3CheckpointSpi}.
 */
public class GridS3SessionCheckpointSelfTest extends GridSessionCheckpointAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testS3Checkpoint() throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        GridS3CheckpointSpi spi = new GridS3CheckpointSpi();

        AWSCredentials cred = new BasicAWSCredentials(GridS3TestSuite.getAccessKey(),
            GridS3TestSuite.getSecretKey());

        spi.setAwsCredentials(cred);

        spi.setBucketNameSuffix("test");

        cfg.setCheckpointSpi(spi);

        GridSessionCheckpointSelfTest.spi = spi;

        checkCheckpoints(cfg);
    }
}
