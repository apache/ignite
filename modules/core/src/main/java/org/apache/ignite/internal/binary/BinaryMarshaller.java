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

package org.apache.ignite.internal.binary;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.marshaller.AbstractMarshaller;
import org.apache.ignite.marshaller.MarshallerContext;
import org.jetbrains.annotations.Nullable;
import sun.misc.Unsafe;

/**
 * Implementation of {@link org.apache.ignite.marshaller.Marshaller} that lets to serialize and deserialize all objects
 * in the binary format.
 */
public class BinaryMarshaller extends AbstractMarshaller {
    /** */
    private GridPortableMarshaller impl;

    /**
     * Checks whether {@code BinaryMarshaller} is able to work on the current JVM.
     * <p>
     * As long as {@code BinaryMarshaller} uses JVM-private API, which is not guaranteed
     * to be available on all JVM, this method should be called to ensure marshaller could work properly.
     * <p>
     * Result of this method is automatically checked in constructor.
     *
     * @return {@code true} if {@code BinaryMarshaller} can work on the current JVM or
     *      {@code false} if it can't.
     */
    @SuppressWarnings({"TypeParameterExtendsFinalClass", "ErrorNotRethrown"})
    public static boolean available() {
        try {
            Unsafe unsafe = GridUnsafe.unsafe();

            Class<? extends Unsafe> unsafeCls = unsafe.getClass();

            unsafeCls.getMethod("allocateInstance", Class.class);
            unsafeCls.getMethod("copyMemory", Object.class, long.class, Object.class, long.class, long.class);

            return true;
        }
        catch (Exception ignored) {
            return false;
        }
        catch (NoClassDefFoundError ignored) {
            return false;
        }
    }

    /**
     * Returns currently set {@link MarshallerContext}.
     *
     * @return Marshaller context.
     */
    public MarshallerContext getContext() {
        return ctx;
    }

    /**
     * Sets {@link PortableContext}.
     * <p/>
     * @param ctx Portable context.
     */
    @SuppressWarnings("UnusedDeclaration")
    private void setPortableContext(PortableContext ctx, IgniteConfiguration cfg) {
        ctx.configure(this, cfg);

        impl = new GridPortableMarshaller(ctx);
    }

    /** {@inheritDoc} */
    @Override public byte[] marshal(@Nullable Object obj) throws IgniteCheckedException {
        return impl.marshal(obj);
    }

    /** {@inheritDoc} */
    @Override public void marshal(@Nullable Object obj, OutputStream out) throws IgniteCheckedException {
        byte[] arr = marshal(obj);

        try {
            out.write(arr);
        }
        catch (IOException e) {
            throw new BinaryObjectException("Failed to marshal the object: " + obj, e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(byte[] bytes, @Nullable ClassLoader clsLdr) throws IgniteCheckedException {
        return impl.deserialize(bytes, clsLdr);
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(InputStream in, @Nullable ClassLoader clsLdr) throws IgniteCheckedException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();

        // we have to fully read the InputStream because GridPortableMarshaller requires support of a method that
        // returns number of bytes remaining.
        try {
            byte[] arr = new byte[4096];

            int cnt;

            while ((cnt = in.read(arr)) != -1)
                buf.write(arr, 0, cnt);

            buf.flush();

            return impl.deserialize(buf.toByteArray(), clsLdr);
        }
        catch (IOException e) {
            throw new BinaryObjectException("Failed to unmarshal the object from InputStream", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onUndeploy(ClassLoader ldr) {
        impl.context().onUndeploy(ldr);
    }
}
