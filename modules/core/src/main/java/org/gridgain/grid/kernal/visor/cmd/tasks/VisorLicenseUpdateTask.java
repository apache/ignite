/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Task to upload license.
 */
@GridInternal
public class VisorLicenseUpdateTask extends VisorOneNodeTask<GridBiTuple<UUID, String>,
    GridBiTuple<GridProductLicenseException, UUID>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorLicenseUpdateJob job(GridBiTuple<UUID, String> arg) {
        return new VisorLicenseUpdateJob(arg);
    }

    private static class VisorLicenseUpdateJob
        extends VisorJob<GridBiTuple<UUID, String>, GridBiTuple<GridProductLicenseException, UUID>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         */
        private VisorLicenseUpdateJob(GridBiTuple<UUID, String> arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected GridBiTuple<GridProductLicenseException, UUID> run(GridBiTuple<UUID, String> arg)
            throws GridException {
            try {
                if (arg.get1() != null) {
                    GridProductLicense lic = g.product().license();

                    if (lic == null)
                        return new GridBiTuple<>(
                            new GridProductLicenseException("Missing licence to compare id", null), null);

                    if (!lic.id().equals(arg.get1()))
                        return new GridBiTuple<>(null, lic.id());
                }

                g.product().updateLicense(arg.get2());

                return new GridBiTuple<>(null, g.product().license().id());
            }
            catch (GridProductLicenseException e) {
                return new GridBiTuple<>(e, null);
            }
            catch (Exception e) {
                return new GridBiTuple<>(new GridProductLicenseException("Failed to load licence", null, e), null);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorLicenseUpdateJob.class, this);
        }
    }
}
