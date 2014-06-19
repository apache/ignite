/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.mongo.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Task for reset mongo metrics.
 */
@GridInternal
public class VisorMongoMetricsResetTask extends VisorOneNodeTask<Void, Boolean> {
    /**
     * Job that reset mongo metrics.
     */
    private static class VisorMongoMetricsResetJob extends VisorJob<Void, Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         */
        private VisorMongoMetricsResetJob(Void arg) {
            super(arg);
        }

        @Override protected Boolean run(Void arg) throws GridException {
            GridMongo mongo = null; // TODO: gg-mongo g.mongo()

            if (mongo == null)
                throw new GridException("Failed to reset Mongo metrics: Mongo not found");

            mongo.resetMetrics();

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorMongoMetricsResetJob.class, this);
        }
    }

    @Override protected VisorMongoMetricsResetJob job(Void arg) {
        return new VisorMongoMetricsResetJob(arg);
    }
}
