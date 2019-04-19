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

package org.apache.ignite.plugin;

import java.util.UUID;
import org.apache.ignite.IgniteException;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class PluginValidationException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Offending node ID. */
    private UUID nodeId;

    /** Error message to send to the offending node. */
    private String rmtMsg;

    /**
     * Constructs invalid plugin exception.
     *
     * @param msg Local error message.
     * @param rmtMsg Error message to send to the offending node.
     * @param nodeId ID of the offending node.
     */
    public PluginValidationException(String msg, String rmtMsg, UUID nodeId) {
        super(msg);

        this.nodeId = nodeId;
        this.rmtMsg = rmtMsg;
    }


    /**
     * @return Offending node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Message to be sent to joining node.
     */
    public String remoteMessage() {
        return rmtMsg;
    }
}