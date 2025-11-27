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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.internal.MessageProcessor;
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;
import static org.apache.ignite.configuration.DeploymentMode.ISOLATED;
import static org.apache.ignite.configuration.DeploymentMode.PRIVATE;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;

/**
 * Message wrapper for {@link DeploymentMode}. See {@link MessageProcessor} for details.
 */
public class DeploymentModeMessage implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 515;

    /** Deployment mode. */
    private DeploymentMode val;

    /** Code. */
    @Order(0)
    private byte code = -1;

    /**
     * Constructor.
     */
    public DeploymentModeMessage() {
    }

    /**
     * Constructor.
     */
    public DeploymentModeMessage(DeploymentMode depMode) {
        val = depMode;
        code = encode(depMode);
    }

    /**
     * @return Code.
     */
    public byte code() {
        return code;
    }

    /**
     * @param code New code.
     */
    public void code(byte code) {
        this.code = code;
        val = decode(code);
    }

    /**
     * @return Deployment mode.
     */
    public DeploymentMode value() {
        return val;
    }

    /** @param depMode Deployment mode to encode. */
    private static byte encode(@Nullable DeploymentMode depMode) {
        if (depMode == null)
            return -1;

        switch (depMode) {
            case PRIVATE: return 0;
            case ISOLATED: return 1;
            case SHARED: return 2;
            case CONTINUOUS: return 3;
        }

        throw new IllegalArgumentException("Unknown deployment mode: " + depMode);
    }

    /** @param code Deployment mode code to decode back to a deployment mode value. */
    @Nullable private static DeploymentMode decode(byte code) {
        switch (code) {
            case -1: return null;
            case 0: return PRIVATE;
            case 1: return ISOLATED;
            case 2: return SHARED;
            case 3: return CONTINUOUS;
        }

        throw new IllegalArgumentException("Unknown deployment mode code: " + code);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
