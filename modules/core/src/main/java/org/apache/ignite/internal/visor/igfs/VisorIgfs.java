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

package org.apache.ignite.internal.visor.igfs;

import java.io.Serializable;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data transfer object for {@link org.apache.ignite.IgniteFileSystem}.
 */
public class VisorIgfs implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** IGFS instance name. */
    private final String name;

    /** IGFS instance working mode. */
    private final IgfsMode mode;

    /** IGFS metrics. */
    private final VisorIgfsMetrics metrics;

    /** Whether IGFS has configured secondary file system. */
    private final boolean secondaryFsConfigured;

    /**
     * Create data transfer object.
     *
     * @param name IGFS name.
     * @param mode IGFS mode.
     * @param metrics IGFS metrics.
     * @param secondaryFsConfigured Whether IGFS has configured secondary file system.
     */
    public VisorIgfs(
        String name,
        IgfsMode mode,
        VisorIgfsMetrics metrics,
        boolean secondaryFsConfigured
    ) {
        this.name = name;
        this.mode = mode;
        this.metrics = metrics;
        this.secondaryFsConfigured = secondaryFsConfigured;
    }

    /**
     * @param igfs Source IGFS.
     * @return Data transfer object for given IGFS.
     */
    public static VisorIgfs from(IgniteFileSystem igfs) {
        assert igfs != null;

        return new VisorIgfs(
            igfs.name(),
            igfs.configuration().getDefaultMode(),
            VisorIgfsMetrics.from(igfs.metrics()),
            igfs.configuration().getSecondaryFileSystem() != null
        );
    }

    /**
     * @return IGFS instance name.
     */
    public String name() {
        return name;
    }

    /**
     * @return IGFS instance working mode.
     */
    public IgfsMode mode() {
        return mode;
    }

    /**
     * @return IGFS metrics.
     */
    public VisorIgfsMetrics metrics() {
        return metrics;
    }

    /**
     * @return Whether IGFS has configured secondary file system.
     */
    public boolean secondaryFileSystemConfigured() {
        return secondaryFsConfigured;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorIgfs.class, this);
    }
}