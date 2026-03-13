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
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.MarshallableMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Message used to transfer {@link Throwable} objects.
 */
@SuppressWarnings({"NullableProblems", "unused"})
public class ErrorMessage implements MarshallableMessage {
    /** Error bytes. */
    @Order(0)
    @GridToStringExclude
    @Nullable public byte[] errBytes;

    /** Error. */
    private @Nullable Throwable err;

    /**
     * Default constructor.
     */
    public ErrorMessage() {
        // No-op.
    }

    /**
     * @param err Original error.
     */
    public ErrorMessage(@Nullable Throwable err) {
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(Marshaller marsh) throws IgniteCheckedException {
        try {
            if (err != null)
                errBytes = U.marshal(marsh, err);
        }
        catch (IgniteCheckedException e) {
            IgniteCheckedException wrappedErr = new IgniteCheckedException(err.getMessage());

            wrappedErr.setStackTrace(err.getStackTrace());
            wrappedErr.addSuppressed(e);

            errBytes = U.marshal(marsh, wrappedErr);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        if (errBytes != null)
            err = U.unmarshal(marsh, errBytes, clsLdr);
    }

    /** */
    public @Nullable Throwable error() {
        return err;
    }

    /**
     * @param errorMsg Error message.
     * @return Error containing in the message.
     */
    public static @Nullable Throwable error(@Nullable ErrorMessage errorMsg) {
        return errorMsg == null ? null : errorMsg.error();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -66;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ErrorMessage.class, this);
    }
}
