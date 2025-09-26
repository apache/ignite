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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.marshaller.Marshallers.jdk;

/**
 * Message used to transfer {@link Throwable} objects.
 * Currently, marshalling and unmarshalling is performed by {@link JdkMarshaller}.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class ErrorMessage implements Message {
    /** Serialized form of throwable. */
    @Order(0)
    private byte[] errBytes;

    /**
     * @return Serialized form of throwable.
     */
    public byte[] errBytes() {
        return errBytes;
    }

    /**
     * @param errBytes New serialized form of throwable.
     */
    public void errBytes(byte[] errBytes) {
        this.errBytes = errBytes;
    }

    /** Factory method to marshal {@link Throwable} objects to send them over the network.  */
    public static ErrorMessage fromThrowable(@Nullable Throwable err) {
        if (err == null)
            return null;

        try {
            ErrorMessage msg = new ErrorMessage();

            msg.errBytes(U.marshal(jdk(), err));

            return msg;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** Factory method to unmarshal serialized {@link Throwable} back to java object. */
    public static <T extends Throwable> T toThrowable(@Nullable ErrorMessage msg) {
        try {
            return msg == null ? null : U.unmarshal(jdk(), msg.errBytes(), U.gridClassLoader());
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -100;
    }
}
