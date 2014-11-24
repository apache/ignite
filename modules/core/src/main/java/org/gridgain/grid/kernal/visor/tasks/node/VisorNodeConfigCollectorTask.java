/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.tasks.node;

import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.dto.node.*;
import org.gridgain.grid.kernal.visor.tasks.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Grid configuration data collect task.
 */
@GridInternal
public class VisorNodeConfigCollectorTask extends VisorOneNodeTask<Void, VisorGridConfig> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorConfigCollectorJob job(Void arg) {
        return new VisorConfigCollectorJob();
    }

    /**
     * Grid configuration data collect job.
     */
    private static class VisorConfigCollectorJob extends VisorJob<Void, VisorGridConfig> {
        /** */
        private static final long serialVersionUID = 0L;

        private VisorConfigCollectorJob() {
            super(null);
        }

        /** {@inheritDoc} */
        @Override protected VisorGridConfig run(Void arg) {
            return VisorGridConfig.from(g);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorConfigCollectorJob.class, this);
        }
    }
}
