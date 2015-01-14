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

package org.gridgain.grid.kernal.managers.deployment.protocol.gg;

import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.managers.deployment.*;

import java.io.*;
import java.net.*;

/**
 * Connection implementation for custom protocol.
 */
public class GridUrlConnection extends URLConnection {
    /** Deployment manager. */
    private GridDeploymentManager mgr;

    /** Input stream. */
    private InputStream in;

    /**
     * Creates connection.
     *
     * @param url Specified URL.
     * @param mgr Deployment manager.
     */
    public GridUrlConnection(URL url, GridDeploymentManager mgr) {
        super(url);

        assert mgr != null;

        this.mgr = mgr;
    }

    /** {@inheritDoc} */
    @Override public void connect() throws IOException {
        URL url = getURL();

        // Gets class loader UUID.
        IgniteUuid ldrId = IgniteUuid.fromString(url.getHost());

        // Gets resource name.
        String name = url.getPath();

        GridDeployment dep = mgr.getDeployment(ldrId);

        if (dep != null) {
            in = dep.classLoader().getParent().getResourceAsStream(name);

            // If resource exists
            connected = true;
        }
    }

    /** {@inheritDoc} */
    @Override public InputStream getInputStream() throws IOException {
        return in;
    }
}
