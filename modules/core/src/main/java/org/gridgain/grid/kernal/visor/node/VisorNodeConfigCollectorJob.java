package org.gridgain.grid.kernal.visor.node;

import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Grid configuration data collect job.
 */
public class VisorNodeConfigCollectorJob extends VisorJob<Void, VisorGridConfig> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param arg Formal job argument.
     */
    public VisorNodeConfigCollectorJob(Void arg) {
        super(arg);
    }

    /** {@inheritDoc} */
    @Override protected VisorGridConfig run(Void arg) {
        return new VisorGridConfig().fill(g);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorNodeConfigCollectorJob.class, this);
    }
}
