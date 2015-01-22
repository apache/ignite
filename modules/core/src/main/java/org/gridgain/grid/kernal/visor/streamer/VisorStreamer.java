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

package org.gridgain.grid.kernal.visor.streamer;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Data transfer object for {@link org.apache.ignite.IgniteStreamer}.
 */
public class VisorStreamer implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Streamer name. */
    private String name;

    /** Metrics. */
    private VisorStreamerMetrics metrics;

    /** Stages. */
    private Collection<VisorStreamerStageMetrics> stages;

    /**
     * @param s Streamer.
     * @return Data transfer object for given streamer.
     */
    public static VisorStreamer from(IgniteStreamer s) {
        assert s != null;

        VisorStreamer streamer = new VisorStreamer();

        streamer.name(s.name());
        streamer.metrics(VisorStreamerMetrics.from(s));
        streamer.stages(VisorStreamerStageMetrics.stages(s));

        return streamer;
    }

    /**
     * @return Streamer name.
     */
    public String name() {
        return name;
    }

    /**
     * @param name New streamer name.
     */
    public void name(String name) {
        this.name = name;
    }

    /**
     * @return Metrics.
     */
    public VisorStreamerMetrics metrics() {
        return metrics;
    }

    /**
     * @param metrics New metrics.
     */
    public void metrics(VisorStreamerMetrics metrics) {
        this.metrics = metrics;
    }

    /**
     * @return Stages.
     */
    public Collection<VisorStreamerStageMetrics> stages() {
        return stages;
    }

    /**
     * @param stages New stages.
     */
    public void stages(Collection<VisorStreamerStageMetrics> stages) {
        this.stages = stages;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorStreamer.class, this);
    }
}
