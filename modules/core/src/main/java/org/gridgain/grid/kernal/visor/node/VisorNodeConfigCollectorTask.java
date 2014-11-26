/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.node;

import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;

/**
 * Grid configuration data collect task.
 */
@GridInternal
public class VisorNodeConfigCollectorTask extends VisorOneNodeTask<Void, VisorGridConfig> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorNodeConfigCollectorJob job(Void arg) {
        return new VisorNodeConfigCollectorJob(arg);
    }
}
