/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import org.gridgain.grid.segmentation.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Segmentation configuration data.
 */
public class VisorSegmentationConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /**Segmentation policy. */
    private final GridSegmentationPolicy plc;

    /**Segmentation resolvers. */
    @Nullable private final String resolvers;

    /**Frequency of network segment check by discovery manager. */
    private final long checkFreq;

    /**Whether or not node should wait for correct segment on start. */
    private final boolean waitOnStart;

    /**Whether or not all resolvers should succeed for node to be in correct segment. */
    private final boolean passRequired;

    public VisorSegmentationConfig(GridSegmentationPolicy plc, @Nullable String resolvers, long checkFreq,
        boolean waitOnStart,
        boolean passRequired) {
        this.plc = plc;
        this.resolvers = resolvers;
        this.checkFreq = checkFreq;
        this.waitOnStart = waitOnStart;
        this.passRequired = passRequired;
    }

    /**
     * @return Segmentation policy.
     */
    public GridSegmentationPolicy policy() {
        return plc;
    }

    /**
     * @return Segmentation resolvers.
     */
    @Nullable public String resolvers() {
        return resolvers;
    }

    /**
     * @return Frequency of network segment check by discovery manager.
     */
    public long checkFrequency() {
        return checkFreq;
    }

    /**
     * @return Whether or not node should wait for correct segment on start.
     */
    public boolean waitOnStart() {
        return waitOnStart;
    }

    /**
     * @return Whether or not all resolvers should succeed for node to be in correct segment.
     */
    public boolean allResolversPassRequired() {
        return passRequired;
    }
}
