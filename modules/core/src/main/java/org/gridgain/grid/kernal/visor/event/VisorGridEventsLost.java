/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.event;

import org.apache.ignite.lang.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

import static org.apache.ignite.events.GridEventType.*;

/**
 * Special event for events lost situations.
 */
public class VisorGridEventsLost extends VisorGridEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Create event with given parameters.
     *
     * @param nid Node where events were lost.
     */
    public VisorGridEventsLost(UUID nid) {
        super(EVT_VISOR_EVENTS_LOST, IgniteUuid.randomUuid(), "EVT_VISOR_EVENTS_LOST", nid, U.currentTimeMillis(),
            "Some Visor events were lost and Visor may show inconsistent results. " +
            "Configure your grid to disable not important events.",
            "");
    }
}
