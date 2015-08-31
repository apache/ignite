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

package org.apache.ignite.internal.managers.deployment.protocol.gg;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import org.apache.ignite.internal.managers.deployment.GridDeploymentManager;

/**
 * Custom stream protocol handler implementation.
 */
public class GridProtocolHandler extends URLStreamHandler {
    /** Deployment manager. */
    private static GridDeploymentManager mgr;

    /**
     * Registers deployment manager.
     *
     * @param mgr Deployment manager.
     */
    public static void registerDeploymentManager(GridDeploymentManager mgr) {
        assert mgr != null;

        GridProtocolHandler.mgr = mgr;
    }

    /**
     * Deregisters deployment manager.
     */
    public static void deregisterDeploymentManager() {
         mgr = null;
    }

    /** {@inheritDoc} */
    @Override protected URLConnection openConnection(URL url) throws IOException {
        return new GridUrlConnection(url, mgr);
    }
}