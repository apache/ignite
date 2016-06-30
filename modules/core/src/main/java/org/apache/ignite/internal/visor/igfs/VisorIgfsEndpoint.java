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
import org.apache.ignite.internal.LessNamingBean;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * IGFS endpoint descriptor.
 */
public class VisorIgfsEndpoint implements Serializable, LessNamingBean {
    /** */
    private static final long serialVersionUID = 0L;

    /** IGFS name. */
    private final String igfsName;

    /** Instance name. */
    private final String instanceName;

    /** Host address / name. */
    private final String hostName;

    /** Port number. */
    private final int port;

    /**
     * Create IGFS endpoint descriptor with given parameters.
     *
     * @param igfsName IGFS name.
     * @param instanceName Instance name.
     * @param hostName Host address / name.
     * @param port Port number.
     */
    public VisorIgfsEndpoint(@Nullable String igfsName, String instanceName, @Nullable String hostName, int port) {
        this.igfsName = igfsName;
        this.instanceName = instanceName;
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
     * @deprecated Use {@link #instanceName()} instead.
     */
    @Deprecated
    public String gridName() {
        return instanceName;
    }

    /**
     * @return Instance name.
     */
    public String instanceName() {
        return instanceName;
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

        if (igfsName == null && instanceName == null)
            return addr;
        else if (igfsName == null)
            return instanceName + "@" + addr;
        else if (instanceName == null)
            return igfsName + "@" + addr;
        else
            return igfsName + ":" + instanceName + "@" + addr;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorIgfsEndpoint.class, this);
    }
}
