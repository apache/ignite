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
import org.gridgain.grid.product.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Update license from nodes task.
 * TODO GG-8358 WHY WE HAVE two simailar task?
 */
@GridInternal
public class VisorReplaceLicenseTask extends VisorMultiNodeTask<VisorReplaceLicenseTask.VisorReplaceLicenseArg,
    Iterable<VisorReplaceLicenseTask.VisorReplaceLicenseResult>, VisorReplaceLicenseTask.VisorReplaceLicenseState> {

    @SuppressWarnings("PublicInnerClass")
    public static class VisorReplaceLicenseArg implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final UUID licenseId;

        private final String[] licenseLine;

        public VisorReplaceLicenseArg(UUID id, String[] line) {
            licenseId = id;
            licenseLine = line;
        }

        /**
         * @return License id.
         */
        public UUID licenseId() {
            return licenseId;
        }

        /**
         * @return License line.
         */
        public String[] licenseLine() {
            return licenseLine;
        }
    }

    public enum VisorReplaceLicenseState {
        SKIPPED,
        FAILED,
        SUCCESSFULLY
    }

    @SuppressWarnings("PublicInnerClass")
    public static class VisorReplaceLicenseResult implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final UUID nid;

        private final VisorReplaceLicenseState state;

        public VisorReplaceLicenseResult(UUID nid, VisorReplaceLicenseState state) {
            this.nid = nid;
            this.state = state;
        }

        /**
         * @return Nid.
         */
        public UUID nid() {
            return nid;
        }

        /**
         * @return State.
         */
        public VisorReplaceLicenseState state() {
            return state;
        }
    }

    private static class VisorReplaceLicenseJob extends VisorJob<VisorReplaceLicenseArg, VisorReplaceLicenseState> {
        /** */
        private static final long serialVersionUID = 0L;

        private VisorReplaceLicenseJob(VisorReplaceLicenseArg arg) {
            super(arg);
        }

        @Override protected VisorReplaceLicenseState run(VisorReplaceLicenseArg arg) throws GridException {
            GridProductLicense lic = g.product().license();

            if (lic != null && lic.id().equals(arg.licenseId())) {
                try {
                    File file = new File(new URL(g.configuration().getLicenseUrl()).toURI());

                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, false))) {
                        for (String line : arg.licenseLine()) {
                            writer.write(line);

                            writer.newLine();
                        }
                    }

                    return VisorReplaceLicenseState.SUCCESSFULLY;
                }
                catch (Exception ignored) {
                    return VisorReplaceLicenseState.FAILED;
                }
            }

            return VisorReplaceLicenseState.SKIPPED;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorReplaceLicenseJob.class, this);
        }
    }

    @Override
    protected VisorReplaceLicenseJob job(VisorReplaceLicenseArg arg) {
        return new VisorReplaceLicenseJob(arg);
    }

    @Nullable @Override
    public Iterable<VisorReplaceLicenseResult> reduce(List<GridComputeJobResult> results) throws GridException {
         Collection<VisorReplaceLicenseResult> res = new ArrayList<>(results.size());

        for (GridComputeJobResult r : results) {
            if (r.getData() instanceof VisorReplaceLicenseState)
                res.add(new VisorReplaceLicenseResult(r.getNode().id(), (VisorReplaceLicenseState)r.getData()));
            else
                res.add(new VisorReplaceLicenseResult(r.getNode().id(), VisorReplaceLicenseState.FAILED));
        }

        return res;
    }
}
