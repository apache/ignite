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

package org.apache.ignite.marshaller.jdk;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.io.GridByteArrayInputStream;
import org.apache.ignite.internal.util.io.GridByteArrayOutputStream;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.AbstractNodeNameAwareMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link org.apache.ignite.marshaller.Marshaller} based on JDK serialization mechanism.
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This marshaller has no mandatory configuration parameters.
 * <h2 class="header">Java Example</h2>
 * {@code JdkMarshaller} needs to be explicitly configured to override default {@link org.apache.ignite.marshaller.optimized.OptimizedMarshaller}.
 * <pre name="code" class="java">
 * JdkMarshaller marshaller = new JdkMarshaller();
 *
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * // Override default marshaller.
 * cfg.setMarshaller(marshaller);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * JdkMarshaller can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.apache.ignite.configuration.IgniteConfiguration" singleton="true"&gt;
 *     ...
 *     &lt;property name="marshaller"&gt;
 *         &lt;bean class="org.apache.ignite.marshaller.jdk.JdkMarshaller"/&gt;
 *     &lt;/property&gt;
 *     ...
 * &lt;/bean&gt;
 * </pre>
 *  <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 */
public class JdkMarshaller extends AbstractNodeNameAwareMarshaller {
    /** {@inheritDoc} */
    @Override protected void marshal0(@Nullable Object obj, OutputStream out) throws IgniteCheckedException {
        assert out != null;

        ObjectOutputStream objOut = null;

        try {
            objOut = new JdkMarshallerObjectOutputStream(new JdkMarshallerOutputStreamWrapper(out));

            // Make sure that we serialize only task, without class loader.
            objOut.writeObject(obj);

            objOut.flush();
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to serialize object: " + obj, e);
        }
        finally{
            U.closeQuiet(objOut);
        }
    }

    /** {@inheritDoc} */
    @Override protected byte[] marshal0(@Nullable Object obj) throws IgniteCheckedException {
        GridByteArrayOutputStream out = null;

        try {
            out = new GridByteArrayOutputStream(DFLT_BUFFER_SIZE);

            marshal(obj, out);

            return out.toByteArray();
        }
        finally {
            U.close(out, null);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override protected <T> T unmarshal0(InputStream in, @Nullable ClassLoader clsLdr) throws IgniteCheckedException {
        assert in != null;

        if (clsLdr == null)
            clsLdr = getClass().getClassLoader();

        ObjectInputStream objIn = null;

        try {
            objIn = new JdkMarshallerObjectInputStream(new JdkMarshallerInputStreamWrapper(in), clsLdr);

            return (T)objIn.readObject();
        }
        catch (ClassNotFoundException e) {
            throw new IgniteCheckedException("Failed to find class with given class loader for unmarshalling " +
                "(make sure same versions of all classes are available on all nodes or enable peer-class-loading) " +
                "[clsLdr=" + clsLdr + ", cls=" + e.getMessage() + "]", e);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to deserialize object with given class loader: " + clsLdr, e);
        }
        finally{
            U.closeQuiet(objIn);
        }
    }

    /** {@inheritDoc} */
    @Override protected <T> T unmarshal0(byte[] arr, @Nullable ClassLoader clsLdr) throws IgniteCheckedException {
        GridByteArrayInputStream in = null;

        try {
            in = new GridByteArrayInputStream(arr, 0, arr.length);

            return unmarshal(in, clsLdr);
        }
        finally {
            U.close(in, null);
        }
    }

    /** {@inheritDoc} */
    @Override public void onUndeploy(ClassLoader ldr) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdkMarshaller.class, this);
    }
}
