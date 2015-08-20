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

import org.apache.ignite.*;
import org.apache.ignite.internal.portable.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.portable.*;

import org.jetbrains.annotations.*;

import java.io.*;
import java.sql.*;
import java.util.*;

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
 * <img src="http://ignite.incubator.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 */
public class PortableMarshaller extends AbstractMarshaller {
    /** Default portable protocol version. */
    public static final PortableProtocolVersion DFLT_PORTABLE_PROTO_VER = PortableProtocolVersion.VER_1_4_0;

    /** Class names. */
    private Collection<String> clsNames;

    /** ID mapper. */
    private PortableIdMapper idMapper;

    /** Serializer. */
    private PortableSerializer serializer;

    /** Types. */
    private Collection<PortableTypeConfiguration> typeCfgs;

    /** Use timestamp flag. */
    private boolean useTs = true;

    /** Whether to convert string to bytes using UTF-8 encoding. */
    private boolean convertString = true;

    /** Meta data enabled flag. */
    private boolean metaDataEnabled = true;

    /** Keep deserialized flag. */
    private boolean keepDeserialized = true;

    /** Protocol version. */
    private PortableProtocolVersion protoVer = DFLT_PORTABLE_PROTO_VER;

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
    public PortableIdMapper getIdMapper() {
        return idMapper;
    }

    /**
     * Sets ID mapper.
     *
     * @param idMapper ID mapper.
     */
    public void setIdMapper(PortableIdMapper idMapper) {
        this.idMapper = idMapper;
    }

    /**
     * Gets serializer.
     *
     * @return Serializer.
     */
    public PortableSerializer getSerializer() {
        return serializer;
    }

    /**
     * Sets serializer.
     *
     * @param serializer Serializer.
     */
    public void setSerializer(PortableSerializer serializer) {
        this.serializer = serializer;
    }

    /**
     * Gets types configuration.
     *
     * @return Types configuration.
     */
    public Collection<PortableTypeConfiguration> getTypeConfigurations() {
        return typeCfgs;
    }

    /**
     * Sets type configurations.
     *
     * @param typeCfgs Type configurations.
     */
    public void setTypeConfigurations(Collection<PortableTypeConfiguration> typeCfgs) {
        this.typeCfgs = typeCfgs;
    }

    /**
     * If {@code true} then date values converted to {@link Timestamp} on deserialization.
     * <p>
     * Default value is {@code true}.
     *
     * @return Flag indicating whether date values converted to {@link Timestamp} during unmarshalling.
     */
    public boolean isUseTimestamp() {
        return useTs;
    }

    /**
     * @param useTs Flag indicating whether date values converted to {@link Timestamp} during unmarshalling.
     */
    public void setUseTimestamp(boolean useTs) {
        this.useTs = useTs;
    }

    /**
     * Gets strings must be converted to or from bytes using UTF-8 encoding.
     * <p>
     * Default value is {@code true}.
     *
     * @return Flag indicating whether string must be converted to byte array using UTF-8 encoding.
     */
    public boolean isConvertStringToBytes() {
        return convertString;
    }

    /**
     * Sets strings must be converted to or from bytes using UTF-8 encoding.
     * <p>
     * Default value is {@code true}.
     *
     * @param convertString Flag indicating whether string must be converted to byte array using UTF-8 encoding.
     */
    public void setConvertStringToBytes(boolean convertString) {
        this.convertString = convertString;
    }

    /**
     * If {@code true}, meta data will be collected or all types. If you need to override this behaviour for
     * some specific type, use {@link PortableTypeConfiguration#setMetaDataEnabled(Boolean)} method.
     * <p>
     * Default value if {@code true}.
     *
     * @return Whether meta data is collected.
     */
    public boolean isMetaDataEnabled() {
        return metaDataEnabled;
    }

    /**
     * @param metaDataEnabled Whether meta data is collected.
     */
    public void setMetaDataEnabled(boolean metaDataEnabled) {
        this.metaDataEnabled = metaDataEnabled;
    }

    /**
     * If {@code true}, {@link PortableObject} will cache deserialized instance after
     * {@link PortableObject#deserialize()} is called. All consequent calls of this
     * method on the same instance of {@link PortableObject} will return that cached
     * value without actually deserializing portable object. If you need to override this
     * behaviour for some specific type, use {@link PortableTypeConfiguration#setKeepDeserialized(Boolean)}
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
     * Gets portable protocol version.
     * <p>
     * Defaults to {@link #DFLT_PORTABLE_PROTO_VER}.
     *
     * @return Portable protocol version.
     */
    public PortableProtocolVersion getProtocolVersion() {
        return protoVer;
    }

    /**
     * Sets portable protocol version.
     * <p>
     * Defaults to {@link #DFLT_PORTABLE_PROTO_VER}.
     *
     * @param protoVer Portable protocol version.
     */
    public void setProtocolVersion(PortableProtocolVersion protoVer) {
        this.protoVer = protoVer;
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
    private void setPortableContext(PortableContext ctx) {
        ctx.configure(this);

        impl = new GridPortableMarshaller(ctx);
    }

    /** {@inheritDoc} */
    @Override public byte[] marshal(@Nullable Object obj) throws IgniteCheckedException {
        return impl.marshal(obj, 0);
    }

    /** {@inheritDoc} */
    @Override public void marshal(@Nullable Object obj, OutputStream out) throws IgniteCheckedException {
        byte[] arr = marshal(obj);

        try {
            out.write(arr);
        }
        catch (IOException e) {
            throw new PortableException("Failed to marshal the object: " + obj, e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(byte[] bytes, @Nullable ClassLoader clsLdr) throws IgniteCheckedException {
        return impl.deserialize(bytes, clsLdr);
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(InputStream in, @Nullable ClassLoader clsLdr) throws IgniteCheckedException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        byte[] arr = new byte[4096];
        int cnt;

        // we have to fully read the InputStream because GridPortableMarshaller requires support of a method that
        // returns number of bytes remaining.
        try {
            while ((cnt = in.read(arr)) != -1)
                buffer.write(arr, 0, cnt);

            buffer.flush();

            return impl.deserialize(buffer.toByteArray(), clsLdr);
        }
        catch (IOException e) {
            throw new PortableException("Failed to unmarshal the object from InputStream", e);
        }
    }
}

