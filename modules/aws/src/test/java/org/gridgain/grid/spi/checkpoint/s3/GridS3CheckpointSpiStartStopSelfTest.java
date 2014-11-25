/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.checkpoint.s3;

import com.amazonaws.auth.*;
import org.gridgain.grid.spi.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.spi.*;
import org.gridgain.testsuites.bamboo.*;

/**
 * Grid S3 checkpoint SPI start stop self test.
 */
@GridSpiTest(spi = GridS3CheckpointSpi.class, group = "Checkpoint SPI")
public class GridS3CheckpointSpiStartStopSelfTest extends GridSpiStartStopAbstractTest<GridS3CheckpointSpi> {
    /** {@inheritDoc} */
    @Override protected void spiConfigure(GridS3CheckpointSpi spi) throws Exception {
        AWSCredentials cred = new BasicAWSCredentials(GridS3TestSuite.getAccessKey(),
            GridS3TestSuite.getSecretKey());

        spi.setAwsCredentials(cred);

        spi.setBucketNameSuffix("test");

        super.spiConfigure(spi);
    }
}
