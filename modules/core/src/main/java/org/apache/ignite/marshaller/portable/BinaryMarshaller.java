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

package org.apache.ignite.marshaller.portable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.portable.GridPortableMarshaller;
import org.apache.ignite.internal.portable.PortableContext;
import org.apache.ignite.marshaller.AbstractMarshaller;
import org.apache.ignite.marshaller.MarshallerContext;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link org.apache.ignite.marshaller.Marshaller} that lets to serialize and deserialize all objects
 * in the portable format.
 * <p>
 * {@code PortableMarshaller} is tested only on Java HotSpot VM on other VMs it could yield unexpected results.
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This marshaller has no mandatory configuration parameters.
 * <h2 class="header">Java Example</h2>
 * <pre name="code" class="java">
 * PortableMarshaller marshaller = new PortableMarshaller();
 *
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * // Override marshaller.
 * cfg.setMarshaller(marshaller);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * PortableMarshaller can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.apache.ignite.configuration.IgniteConfiguration" singleton="true"&gt;
 *     ...
 *     &lt;property name="marshaller"&gt;
 *         &lt;bean class="org.apache.ignite.marshaller.portable.PortableMarshaller"&gt;
 *            ...
 *         &lt;/bean&gt;
 *     &lt;/property&gt;
 *     ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 */
public class BinaryMarshaller extends AbstractMarshaller {
    /** */
    private GridPortableMarshaller impl;

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
