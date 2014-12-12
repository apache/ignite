/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.tostring.*;

import java.util.Map;

/**
 * Interface for all processors.
 */
@GridToStringExclude
public interface GridProcessor extends GridComponent {
    /**
     * Adds attributes from this component to map of all node attributes.
     *
     * @param attrs Map of all attributes.
     * @throws IgniteCheckedException If failed.
     */
    public void addAttributes(Map<String, Object> attrs) throws IgniteCheckedException;
}
