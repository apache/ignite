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
import org.apache.ignite.internal.Order;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Message for {@link DeploymentMode}.
 * Consistency between code-to-value and value-to-code conversions must be provided.
 */
public class DeploymentModeMessage implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 506;

    /** Deployment mode. */
    private DeploymentMode val;

    /** Code serialization call holder. */
    @Order(0)
    private byte code;

    /** Constructor. */
    public DeploymentModeMessage() {
        // No-op.
    }

    /** Constructor. */
    public DeploymentModeMessage(DeploymentMode val) {
        this.val = val;
    }

    /** @return Code. */
    public byte code() {
        if (val == null)
            return -1;

        switch (val) {
            case PRIVATE:
                return 0;

            case ISOLATED:
                return 1;

            case SHARED:
                return 2;

            case CONTINUOUS:
                return 3;

            default:
                throw new IllegalArgumentException("Unknown deployment mode value: " + val);
        }
    }

    /** @param code Code. */
    public void code(byte code) {
        switch (code) {
            case -1:
                val = null;
                break;

            case 0:
                val = DeploymentMode.PRIVATE;
                break;

            case 1:
                val = DeploymentMode.ISOLATED;
                break;

            case 2:
                val = DeploymentMode.SHARED;
                break;

            case 3:
                val = DeploymentMode.CONTINUOUS;
                break;

            default:
                throw new IllegalArgumentException("Unknown deployment mode code: " + code);
        }
    }

    /** @return Transaction isolation. */
    public DeploymentMode value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }
}
