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
import org.apache.ignite.internal.MessageProcessor;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.marshaller.Marshallers.jdk;

/**
 * Message used to transfer {@link Throwable} objects.
 * <p>Because raw serialization of throwables is prohibited, you should use this message when it is necessary
 * to transfer some error as part of some message. See {@link MessageProcessor} for details.
 * <p>Currently, under the hood marshalling and unmarshalling is performed by {@link JdkMarshaller}.
 */
@SuppressWarnings({"NullableProblems", "unused"})
public class ErrorMessage implements Message {
    /** Serialization and deserealization methods call holder. */
    @Order(value = 0, method = "errorBytes")
    private @Nullable byte[] errBytes;

    /** Original error. It is transient and necessary only to avoid duplicated serialization and deserializtion. */
    private @Nullable Throwable err;

    /**
     * Default constructor.
     */
    public ErrorMessage() {
        // No-op.
    }

    /**
     * @param err Original error. Will be lazily serialized.
     */
    public ErrorMessage(@Nullable Throwable err) {
        this.err = err;
    }

    /**
     * @return Serialized form of throwable.
     */
    public @Nullable byte[] errorBytes() {
        if (err == null)
            return null;

        try {
            return U.marshal(jdk(), err);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Unable to marshal the holding error.", e);
        }
    }

    /**
     * @param errBytes New serialized form of throwable.
     */
    public void errorBytes(@Nullable byte[] errBytes) {
        if (errBytes == null)
            err = null;
        else {
            try {
                err = U.unmarshal(jdk(), errBytes, U.gridClassLoader());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to unmarshal error data bytes.", e);
            }
        }
    }

    /** */
    public @Nullable Throwable toThrowable() {
        return err;
    }

    /**
     * Safely gets original error from an error message.
     *
     * @param errorMsg Error message.
     * @return Error containing in the message.
     */
    public static @Nullable Throwable error(@Nullable ErrorMessage errorMsg) {
        return errorMsg == null ? null : errorMsg.toThrowable();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -100;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return toThrowable().toString();
    }
}
