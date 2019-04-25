/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.igfs.common;

import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.igfs.common.IgfsIpcCommand.HANDSHAKE;

/**
 * Handshake request.
 */
public class IgfsHandshakeRequest extends IgfsMessage {
    /** Expected IGFS name. */
    private String igfsName;

    /** Logger directory. */
    private String logDir;

    /** {@inheritDoc} */
    @Override public IgfsIpcCommand command() {
        return HANDSHAKE;
    }

    /** {@inheritDoc} */
    @Override public void command(IgfsIpcCommand cmd) {
        // No-op.
    }

    /**
     * @return IGFS name.
     */
    public String igfsName() {
        return igfsName;
    }

    /**
     * @param igfsName IGFS name.
     */
    public void igfsName(String igfsName) {
        this.igfsName = igfsName;
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
        return S.toString(IgfsHandshakeRequest.class, this);
    }
}