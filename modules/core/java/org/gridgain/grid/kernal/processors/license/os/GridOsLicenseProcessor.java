/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.license.os;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.kernal.processors.license.*;
import org.gridgain.grid.product.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * No-op implementation for {@link GridLicenseProcessor}.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridOsLicenseProcessor extends GridProcessorAdapter implements GridLicenseProcessor {
    /** */
    private static final GridProductLicense dummyLic = new GridProductLicense() {
        @Override public String disabledSubsystems() {
            return null;
        }

        @Override public String version() {
            return null;
        }

        @Override public UUID id() {
            return null;
        }

        @Override public String versionRegexp() {
            return null;
        }

        @Override public Date issueDate() {
            return null;
        }

        @Override public int maintenanceTime() {
            return 0;
        }

        @Override public String issueOrganization() {
            return null;
        }

        @Override public String userOrganization() {
            return null;
        }

        @Override public String licenseNote() {
            return null;
        }

        @Override public String userWww() {
            return null;
        }

        @Override public String userEmail() {
            return null;
        }

        @Override public String userName() {
            return null;
        }

        @Override public Date expireDate() {
            return null;
        }

        @Override public int maxNodes() {
            return 0;
        }

        @Override public int maxComputers() {
            return 0;
        }

        @Override public int maxCpus() {
            return 0;
        }

        @Override public long maxUpTime() {
            return 0;
        }

        @Override public long gracePeriod() {
            return 0;
        }

        @Override @Nullable public String attributeName() {
            return null;
        }

        @Override @Nullable public String attributeValue() {
            return null;
        }

        @Nullable @Override public String getCacheDistributionModes() {
            return null;
        }
    };

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
    @Override public boolean enabled(GridProductEdition ed) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public GridProductLicense license() {
        return dummyLic;
    }

    /** {@inheritDoc} */
    @Override public long gracePeriodLeft() {
        return 0;
    }
}
