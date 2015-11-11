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
import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.portable.GridPortableMarshaller;
import org.apache.ignite.internal.portable.PortableContext;
import org.apache.ignite.marshaller.AbstractMarshaller;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinarySerializer;
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
public class PortableMarshaller extends AbstractMarshaller {
    // TODO ignite-1282 Move to IgniteConfiguration.
    /** Class names. */
    private Collection<String> clsNames;

    /** ID mapper. */
    private BinaryIdMapper idMapper;

    /** Serializer. */
    private BinarySerializer serializer;

    /** Types. */
    private Collection<BinaryTypeConfiguration> typeCfgs;

    /** Keep deserialized flag. */
    private boolean keepDeserialized = true;

    /** */
    private GridPortableMarshaller impl;

    /**
     * Gets class names.
     *
     * @return Class names.
     */
    public Collection<String> getClassNames() {
        return clsNames;
    }

    /**
     * Sets class names of portable objects explicitly.
     *
     * @param clsNames Class names.
     */
    public void setClassNames(Collection<String> clsNames) {
        this.clsNames = new ArrayList<>(clsNames.size());

        for (String clsName : clsNames)
            this.clsNames.add(clsName.trim());
    }

    /**
     * Gets ID mapper.
     *
     * @return ID mapper.
     */
    public BinaryIdMapper getIdMapper() {
        return idMapper;
    }

    /**
     * Sets ID mapper.
     *
     * @param idMapper ID mapper.
     */
    public void setIdMapper(BinaryIdMapper idMapper) {
        this.idMapper = idMapper;
    }

    /**
     * Gets serializer.
     *
     * @return Serializer.
     */
    public BinarySerializer getSerializer() {
        return serializer;
    }

    /**
     * Sets serializer.
     *
     * @param serializer Serializer.
     */
    public void setSerializer(BinarySerializer serializer) {
        this.serializer = serializer;
    }

    /**
     * Gets types configuration.
     *
     * @return Types configuration.
     */
    public Collection<BinaryTypeConfiguration> getTypeConfigurations() {
        return typeCfgs;
    }

    /**
     * Sets type configurations.
     *
     * @param typeCfgs Type configurations.
     */
    public void setTypeConfigurations(Collection<BinaryTypeConfiguration> typeCfgs) {
        this.typeCfgs = typeCfgs;
    }

    /**
     * If {@code true}, {@link org.apache.ignite.binary.BinaryObject} will cache deserialized instance after
     * {@link org.apache.ignite.binary.BinaryObject#deserialize()} is called. All consequent calls of this
     * method on the same instance of {@link org.apache.ignite.binary.BinaryObject} will return that cached
     * value without actually deserializing portable object. If you need to override this
     * behaviour for some specific type, use {@link org.apache.ignite.binary.BinaryTypeConfiguration#setKeepDeserialized(Boolean)}
     * method.
     * <p>
     * Default value if {@code true}.
     *
     * @return Whether deserialized value is kept.
     */
    public boolean isKeepDeserialized() {
        return keepDeserialized;
    }

    /**
     * @param keepDeserialized Whether deserialized value is kept.
     */
    public void setKeepDeserialized(boolean keepDeserialized) {
        this.keepDeserialized = keepDeserialized;
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
    private void setPortableContext(PortableContext ctx) {
        ctx.configure(this);

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

        byte[] arr = new byte[4096];
        int cnt;

        // we have to fully read the InputStream because GridPortableMarshaller requires support of a method that
        // returns number of bytes remaining.
        try {
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
