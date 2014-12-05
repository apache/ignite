/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.checkpoint.noop;

import org.apache.ignite.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.spi.checkpoint.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 * No-op implementation of {@link org.gridgain.grid.spi.checkpoint.CheckpointSpi}. This is default implementation
 * since {@code 4.5.0} version.
 */
@IgniteSpiNoop
@IgniteSpiMultipleInstancesSupport(true)
public class GridNoopCheckpointSpi extends IgniteSpiAdapter implements CheckpointSpi {
    /** Logger. */
    @IgniteLoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
        U.warn(log, "Checkpoints are disabled (to enable configure any GridCheckpointSpi implementation)");
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] loadCheckpoint(String key) throws IgniteSpiException {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean saveCheckpoint(String key, byte[] state, long timeout, boolean overwrite) throws IgniteSpiException {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean removeCheckpoint(String key) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void setCheckpointListener(CheckpointListener lsnr) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNoopCheckpointSpi.class, this);
    }
}
