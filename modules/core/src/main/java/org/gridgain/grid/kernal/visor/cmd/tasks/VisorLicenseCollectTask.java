/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.kernal.visor.cmd.dto.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Collect license from nodes task.
 */
@GridInternal
public class VisorLicenseCollectTask extends
    VisorMultiNodeTask<Void, Iterable<GridBiTuple<UUID, VisorLicense>>, VisorLicense> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorLicenseCollectJob job(Void arg) {
        return new VisorLicenseCollectJob(arg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Iterable<GridBiTuple<UUID, VisorLicense>> reduce(List<GridComputeJobResult> results) throws GridException {
        Collection<GridBiTuple<UUID, VisorLicense>> licenses = new ArrayList<>(results.size());

        for (GridComputeJobResult r : results) {
            VisorLicense license = r.getException() != null ? null : (VisorLicense) r.getData();

            licenses.add(new GridBiTuple<>(r.getNode().id(), license));
        }

        return licenses;
    }

    /**
     * Job that collect license from nodes.
     */
    private static class VisorLicenseCollectJob extends VisorJob<Void, VisorLicense> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Formal job argument.
         */
        private VisorLicenseCollectJob(Void arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Nullable @Override protected VisorLicense run(Void arg) throws GridException {
            return VisorLicense.from(g);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorLicenseCollectJob.class, this);
        }
    }
}
