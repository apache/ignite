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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * IGFS endpoint descriptor.
 */
public class VisorIgfsEndpoint implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** IGFS name. */
    private final String igfsName;

    /** Grid name. */
    private final String gridName;

    /** Host address / name. */
    private final String hostName;

    /** Port number. */
    private final int port;

    /**
     * Create IGFS endpoint descriptor with given parameters.
     *
     * @param igfsName IGFS name.
     * @param gridName Grid name.
     * @param hostName Host address / name.
     * @param port Port number.
     */
    public VisorIgfsEndpoint(@Nullable String igfsName, String gridName, @Nullable String hostName, int port) {
        this.igfsName = igfsName;
        this.gridName = gridName;
        this.hostName = hostName;
        this.port = port;
    }

    /**
     * @return IGFS name.
     */
    @Nullable public String igfsName() {
        return igfsName;
    }

    /**
     * @return Grid name.
     */
    public String gridName() {
        return gridName;
    }

    /**
     * @return Host address / name.
     */
    @Nullable public String hostName() {
        return hostName;
    }

    /**
     * @return Port number.
     */
    public int port() {
        return port;
    }

    /**
     * @return URI Authority
     */
    public String authority() {
        String addr = hostName + ":" + port;

        if (igfsName == null && gridName == null)
            return addr;
        else if (igfsName == null)
            return gridName + "@" + addr;
        else if (gridName == null)
            return igfsName + "@" + addr;
        else
            return igfsName + ":" + gridName + "@" + addr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorIgfsEndpoint.class, this);
    }
}