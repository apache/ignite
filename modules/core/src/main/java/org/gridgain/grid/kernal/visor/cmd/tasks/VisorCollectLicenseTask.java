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
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Collect license from nodes task.
 */
@GridInternal
public class VisorCollectLicenseTask extends VisorMultiNodeTask<VisorMultiNodeArg,
    Iterable<T2<UUID, VisorLicense>>, VisorLicense> {

    /**
     * Collect license from nodes job.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorCollectLicenseJob extends VisorJob<VisorMultiNodeArg, VisorLicense> {
        /** */
        private static final long serialVersionUID = 0L;

        protected VisorCollectLicenseJob(VisorMultiNodeArg arg) {
            super(arg);
        }

        @Override protected VisorLicense run(VisorMultiNodeArg arg) throws GridException {
            return VisorLicense.create(g);
        }
    }

    @Override protected VisorCollectLicenseJob job(VisorMultiNodeArg arg) {
        return new VisorCollectLicenseJob(arg);
    }

    @Nullable @Override
    public Iterable<T2<UUID, VisorLicense>> reduce(List<GridComputeJobResult> results) throws GridException {
        Collection<T2<UUID, VisorLicense>> licenses = new ArrayList<>(results.size());

        for (GridComputeJobResult r : results) {
            VisorLicense license = r.getException() != null ? null : (VisorLicense) r.getData();

            licenses.add(new T2<>(r.getNode().id(), license));
        }

        return licenses;
    }
}
