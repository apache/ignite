/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.license.os;

import org.apache.ignite.product.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.kernal.processors.license.*;
import org.jetbrains.annotations.*;

/**
 * No-op implementation for {@link GridLicenseProcessor}.
 */
public class GridOsLicenseProcessor extends GridProcessorAdapter implements GridLicenseProcessor {
    /**
     * @param ctx Kernal context.
     */
    public GridOsLicenseProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void updateLicense(String licTxt) throws GridProductLicenseException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void ackLicense() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void checkLicense() throws GridProductLicenseException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean enabled(GridLicenseSubsystem ed) {
        return true;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridProductLicense license() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public long gracePeriodLeft() {
        return -1;
    }
}
