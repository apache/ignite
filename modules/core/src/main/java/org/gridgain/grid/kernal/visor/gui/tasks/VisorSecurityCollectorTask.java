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
import org.gridgain.grid.security.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.kernal.GridProductImpl.ENT;

/**
 * Collects security information from node.
 */
@GridInternal
public class VisorSecurityCollectorTask extends VisorOneNodeTask<Void, Collection<GridSecuritySubject>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorSecurityCollectorJob job(@Nullable Void arg) {
        return new VisorSecurityCollectorJob(arg);
    }

    /**
     * Job that collect security information form node.
     */
    private static class VisorSecurityCollectorJob extends VisorJob<Void, Collection<GridSecuritySubject>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Node ID to collect security information.
         */
        protected VisorSecurityCollectorJob(@Nullable Void arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected Collection<GridSecuritySubject> run(Void arg) throws GridException {
            if (ENT)
                return g.security().authenticatedSubjects();
            else
                return Collections.emptyList();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorSecurityCollectorJob.class, this);
        }
    }
}
