/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.tasks;

import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.kernal.visor.cmd.dto.*;
import org.gridgain.grid.util.typedef.internal.*;

import static java.lang.System.*;

/**
 * Grid configuration data collect task.
 */
@GridInternal
public class VisorConfigCollectorTask extends VisorOneNodeTask<Void, VisorGridConfig> {
    /**
     * Returns boolean value from system property or provided function.
     *
     * @param propName System property host.
     * @param dflt Function that returns `Boolean`.
     * @return `Boolean` value
     */
    private static boolean boolValue(String propName, boolean dflt) {
        String sysProp = getProperty(propName);

        return (sysProp != null && !sysProp.isEmpty()) ? Boolean.getBoolean(sysProp) : dflt;
    }

    /**
     * Returns boolean value from system property or provided function.
     *
     * @param propName System property host.
     * @param dflt Function that returns `Boolean`.
     * @return `Boolean` value
     */
    private static Integer intValue(String propName, Integer dflt) {
        String sysProp = getProperty(propName);

        return (sysProp != null && !sysProp.isEmpty()) ? Integer.getInteger(sysProp) : dflt;
    }

    /**
     * Grid configuration data collect job.
     */
    private static class VisorConfigurationJob extends VisorJob<Void, VisorGridConfig> {
        /** */
        private static final long serialVersionUID = 0L;

        private VisorConfigurationJob() {
            super(null);
        }

        /** {@inheritDoc} */
        @Override protected VisorGridConfig run(Void arg) {
            return VisorGridConfig.from(g);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorConfigurationJob.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorConfigurationJob job(Void arg) {
        return new VisorConfigurationJob();
    }
}
