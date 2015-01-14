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

package org.gridgain.grid.kernal.visor.ggfs;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * GGFS endpoint descriptor.
 */
public class VisorGgfsEndpoint implements Serializable{
    /** */
    private static final long  serialVersionUID = 0L;

    /** GGFS name. */
    private final String ggfsName;

    /** Grid name. */
    private final String gridName;

    /** Host address / name. */
    private final String hostName;

    /** Port number. */
    private final int port;

    /**
     * Create GGFS endpoint descriptor with given parameters.
     * @param ggfsName GGFS name.
     * @param gridName Grid name.
     * @param hostName Host address / name.
     * @param port Port number.
     */
    public VisorGgfsEndpoint(@Nullable String ggfsName, String gridName, @Nullable String hostName, int port) {
        this.ggfsName = ggfsName;
        this.gridName = gridName;
        this.hostName = hostName;
        this.port = port;
    }

    /**
     * @return GGFS name.
     */
    @Nullable public String ggfsName() {
        return ggfsName;
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

        if (ggfsName == null && gridName == null)
            return addr;
        else if (ggfsName == null)
            return gridName + "@" + addr;
        else if (gridName == null)
            return ggfsName + "@" + addr;
        else
            return ggfsName + ":" + gridName + "@" + addr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGgfsEndpoint.class, this);
    }
}
