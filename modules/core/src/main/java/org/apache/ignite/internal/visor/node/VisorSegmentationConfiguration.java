/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.node;

import org.apache.ignite.configuration.*;
import org.apache.ignite.plugin.segmentation.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

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
