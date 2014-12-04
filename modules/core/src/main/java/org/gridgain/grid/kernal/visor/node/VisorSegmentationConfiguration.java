/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.node;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.segmentation.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Data transfer object for node segmentation configuration properties.
 */
public class VisorSegmentationConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Segmentation policy. */
    private GridSegmentationPolicy plc;

    /** Segmentation resolvers. */
    private String resolvers;

    /** Frequency of network segment check by discovery manager. */
    private long checkFreq;

    /** Whether or not node should wait for correct segment on start. */
    private boolean waitOnStart;

    /** Whether or not all resolvers should succeed for node to be in correct segment. */
    private boolean passRequired;

    /**
     * @param c Grid configuration.
     * @return Data transfer object for node segmentation configuration properties.
     */
    public static VisorSegmentationConfiguration from(IgniteConfiguration c) {
        VisorSegmentationConfiguration cfg = new VisorSegmentationConfiguration();

        cfg.policy(c.getSegmentationPolicy());
        cfg.resolvers(compactArray(c.getSegmentationResolvers()));
        cfg.checkFrequency(c.getSegmentCheckFrequency());
        cfg.waitOnStart(c.isWaitForSegmentOnStart());
        cfg.passRequired(c.isAllSegmentationResolversPassRequired());

        return cfg;
    }

    /**
     * @return Segmentation policy.
     */
    public GridSegmentationPolicy policy() {
        return plc;
    }

    /**
     * @param plc New segmentation policy.
     */
    public void policy(GridSegmentationPolicy plc) {
        this.plc = plc;
    }

    /**
     * @return Segmentation resolvers.
     */
    @Nullable public String resolvers() {
        return resolvers;
    }

    /**
     * @param resolvers New segmentation resolvers.
     */
    public void resolvers(@Nullable String resolvers) {
        this.resolvers = resolvers;
    }

    /**
     * @return Frequency of network segment check by discovery manager.
     */
    public long checkFrequency() {
        return checkFreq;
    }

    /**
     * @param checkFreq New frequency of network segment check by discovery manager.
     */
    public void checkFrequency(long checkFreq) {
        this.checkFreq = checkFreq;
    }

    /**
     * @return Whether or not node should wait for correct segment on start.
     */
    public boolean waitOnStart() {
        return waitOnStart;
    }

    /**
     * @param waitOnStart New whether or not node should wait for correct segment on start.
     */
    public void waitOnStart(boolean waitOnStart) {
        this.waitOnStart = waitOnStart;
    }

    /**
     * @return Whether or not all resolvers should succeed for node to be in correct segment.
     */
    public boolean passRequired() {
        return passRequired;
    }

    /**
     * @param passRequired New whether or not all resolvers should succeed for node to be in correct segment.
     */
    public void passRequired(boolean passRequired) {
        this.passRequired = passRequired;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorSegmentationConfiguration.class, this);
    }
}
