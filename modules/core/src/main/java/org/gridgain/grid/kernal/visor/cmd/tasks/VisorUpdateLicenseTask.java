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
import org.gridgain.grid.product.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Task to upload license.
 */
@GridInternal
public class VisorUpdateLicenseTask extends VisorOneNodeTask<T2<UUID, String>, T2<GridProductLicenseException, UUID>> {
    private static class VisorUpdateLicenseJob
        extends VisorJob<T2<UUID, String>, T2<GridProductLicenseException, UUID>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         */
        private VisorUpdateLicenseJob(T2<UUID, String> arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected T2<GridProductLicenseException, UUID> run(T2<UUID, String> arg) throws GridException {
            try {
                if (arg.get1() != null) {
                    GridProductLicense lic = g.product().license();

                    if (lic == null)
                        return new T2<>(new GridProductLicenseException("Missing licence to compare id", null), null);

                    if (!lic.id().equals(arg.get1()))
                        return new T2<>(null, lic.id());
                }

                g.product().updateLicense(arg.get2());

                return new T2<>(null, g.product().license().id());
            }
            catch (GridProductLicenseException e) {
                return new T2<>(e, null);
            }
            catch (Exception e) {
                return new T2<>(new GridProductLicenseException("Failed to load licence", null, e), null);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorUpdateLicenseJob.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorUpdateLicenseJob job(T2<UUID, String> arg) {
        return new VisorUpdateLicenseJob(arg);
    }
}
