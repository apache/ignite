/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.segmentation;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.startup.cmdline.*;

/**
 * Policy that defines how node will react on topology segmentation. Note that default
 * segmentation policy is defined by {@link org.gridgain.grid.IgniteConfiguration#DFLT_SEG_PLC} property.
 * @see GridSegmentationResolver
 */
public enum GridSegmentationPolicy {
    /**
     * When segmentation policy is {@code RESTART_JVM}, all listeners will receive
     * {@link GridEventType#EVT_NODE_SEGMENTED} event and then JVM will be restarted.
     * Note, that this will work <b>only</b> if GridGain is started with {@link GridCommandLineStartup}
     * via standard {@code ggstart.{sh|bat}} shell script.
     */
    RESTART_JVM,

    /**
     * When segmentation policy is {@code STOP}, all listeners will receive
     * {@link GridEventType#EVT_NODE_SEGMENTED} event and then particular grid node
     * will be stopped via call to {@link GridGain#stop(String, boolean)}.
     */
    STOP,

    /**
     * When segmentation policy is {@code NOOP}, all listeners will receive
     * {@link GridEventType#EVT_NODE_SEGMENTED} event and it is up to user to
     * implement logic to handle this event.
     */
    NOOP
}

