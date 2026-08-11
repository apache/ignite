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

package org.apache.ignite.internal.util;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.MarshallableMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.AbstractMarshaller;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Message used to transfer {@link Throwable} objects.
 */
// TODO IGNITE-28912: move to a common package.
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
    @Override public void marshal(Marshaller marsh) throws IgniteCheckedException {
        if (err == null)
            return;

        Marshaller errMarsh = jdkMarshaller(marsh);

        try {
            errBytes = U.marshal(errMarsh, err);
        }
        catch (Throwable e) {
            errBytes = U.marshal(errMarsh, wrapError(true, e));
        }
    }

    /** {@inheritDoc} */
    @Override public void unmarshal(Marshaller marsh, ClassLoader clsLdr) throws IgniteCheckedException {
        if (errBytes == null)
            return;

        try {
            err = U.unmarshal(jdkMarshaller(marsh), errBytes, clsLdr);
        }
        catch (Throwable e) {
            err = wrapError(false, e);
        }
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
    @Override public String toString() {
        return S.toString(ErrorMessage.class, this);
    }

    /**
     * An error carries an arbitrary user class, and this message travels both transports. A schema-aware marshaller
     * registers class names in the cluster, which sends a discovery message and waits for the answer, while this
     * message is marshalled on discovery threads as well - such a wait would block the thread that has to deliver the
     * answer. So errors always go through the JDK marshaller of the local node, whatever the transport marshaller is.
     *
     * @param marsh Marshaller of the transport.
     * @return JDK marshaller of the local node.
     */
    private static Marshaller jdkMarshaller(Marshaller marsh) {
        if (marsh instanceof JdkMarshaller)
            return marsh;

        MarshallerContext marshCtx = ((AbstractMarshaller)marsh).getContext();

        assert marshCtx != null : "The marshaller is not bound to a node: " + marsh;

        return marshCtx.jdkMarshaller();
    }

    /** */
    private static Throwable wrapError(boolean marshall, Throwable e) {
        String errStr = "Failed to " + (marshall ? "marshall" : "unmarshall") + " an exception.";

        if (e.getCause() != null && e.getCause() != e) {
            errStr += " Original cause: \"" + e.getCause().getMessage() + "\", the stack head: \""
                + e.getCause().getStackTrace()[0].toString() + "\".";
        }

        IgniteCheckedException wrappedErr = new IgniteCheckedException(errStr);

        wrappedErr.setStackTrace(e.getStackTrace());

        wrappedErr.addSuppressed(e);

        return wrappedErr;
    }
}
