/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.event;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;

import java.util.*;

/**
 * Lightweight counterpart for {@link GridDeploymentEvent}.
 */
public class VisorGridDeploymentEvent extends VisorGridEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deployment alias. */
    private final String alias;

    /** Create event with given parameters. */
    public VisorGridDeploymentEvent(
        int typeId,
        GridUuid id,
        String name,
        UUID nid,
        long timestamp,
        String message,
        String shortDisplay,
        String alias
    ) {
        super(typeId, id, name, nid, timestamp, message, shortDisplay);

        this.alias = alias;
    }

    /**
     * @return Deployment alias.
     */
    public String alias() {
        return alias;
    }
}
