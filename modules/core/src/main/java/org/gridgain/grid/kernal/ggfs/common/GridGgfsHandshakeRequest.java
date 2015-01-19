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

package org.gridgain.grid.kernal.ggfs.common;

import org.gridgain.grid.util.typedef.internal.*;

import static org.gridgain.grid.kernal.ggfs.common.GridGgfsIpcCommand.*;

/**
 * Handshake request.
 */
public class GridGgfsHandshakeRequest extends GridGgfsMessage {
    /** Expected Grid name. */
    private String gridName;

    /** Expected GGFS name. */
    private String ggfsName;

    /** Logger directory. */
    private String logDir;

    /** {@inheritDoc} */
    @Override public GridGgfsIpcCommand command() {
        return HANDSHAKE;
    }

    /** {@inheritDoc} */
    @Override public void command(GridGgfsIpcCommand cmd) {
        // No-op.
    }

    /**
     * @return Grid name.
     */
    public String gridName() {
        return gridName;
    }

    /**
     * @param gridName Grid name.
     */
    public void gridName(String gridName) {
        this.gridName = gridName;
    }

    /**
     * @return GGFS name.
     */
    public String ggfsName() {
        return ggfsName;
    }

    /**
     * @param ggfsName GGFS name.
     */
    public void ggfsName(String ggfsName) {
        this.ggfsName = ggfsName;
    }

    /**
     * @return Log directory.
     */
    public String logDirectory() {
        return logDir;
    }

    /**
     * @param logDir Log directory.
     */
    public void logDirectory(String logDir) {
        this.logDir = logDir;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsHandshakeRequest.class, this);
    }
}
