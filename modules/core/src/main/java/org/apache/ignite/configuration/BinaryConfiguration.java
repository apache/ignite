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

package org.apache.ignite.configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryNameMapper;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.internal.InstanceFactory;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.BinaryWriteMode;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

/**
 * Configuration object for Ignite Binary Objects.
 *
 * @see org.apache.ignite.IgniteBinary
 */
public class BinaryConfiguration {
    /** Default compact footer flag setting. */
    public static final boolean DFLT_COMPACT_FOOTER = true;

    /** ID mapper. */
    private BinaryIdMapper idMapper;

    /** Name mapper. */
    private BinaryNameMapper nameMapper;

    /** Serializer. */
    private BinarySerializer serializer;

    /** Instances initialization factory. */
    private ConcurrentMap<Class<?>, InstanceFactory> initializationFactory = new ConcurrentHashMap8<>();

    /** Types. */
    private Collection<BinaryTypeConfiguration> typeCfgs;

    /** Compact footer flag. */
    private boolean compactFooter = DFLT_COMPACT_FOOTER;

    /**
     * Sets class names of binary objects explicitly.
     *
     * @param clsNames Class names.
     */
    public void setClassNames(Collection<String> clsNames) {
        if (typeCfgs == null)
            typeCfgs = new ArrayList<>(clsNames.size());

        for (String clsName : clsNames)
            typeCfgs.add(new BinaryTypeConfiguration(clsName));
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
     * Gets name mapper.
     *
     * @return Name mapper.
     */
    public BinaryNameMapper getNameMapper() {
        return nameMapper;
    }

    /**
     * Sets name mapper.
     *
     * @param nameMapper Name mapper.
     */
    public void setNameMapper(BinaryNameMapper nameMapper) {
        this.nameMapper = nameMapper;
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
     * Get whether to write footers in compact form. When enabled, Ignite will not write fields metadata
     * when serializing objects, because internally {@code BinaryMarshaller} already distribute metadata inside
     * cluster. This increases serialization performance.
     * <p>
     * <b>WARNING!</b> This mode should be disabled when already serialized data can be taken from some external
     * sources (e.g. cache store which stores data in binary form, data center replication, etc.). Otherwise binary
     * objects without any associated metadata could appear in the cluster and Ignite will not be able to deserialize
     * it.
     * <p>
     * Defaults to {@link #DFLT_COMPACT_FOOTER}.
     *
     * @return Whether to write footers in compact form.
     */
    public boolean isCompactFooter() {
        return compactFooter;
    }

    /**
     * Set whether to write footers in compact form. See {@link #isCompactFooter()} for more info.
     *
     * @param compactFooter Whether to write footers in compact form.
     */
    public void setCompactFooter(boolean compactFooter) {
        this.compactFooter = compactFooter;
    }

    /**
     * Gets {@link InstanceFactory} by Class from the Initialization Factory.
     *
     * @param clazz - Class which mapping for the {@link InstanceFactory}.
     * @return the {@link InstanceFactory} to which the specified Class is mapped, or null if this Initialization
     * Factory contains no mapping for the Class.
     */
    @Nullable public InstanceFactory getInstanceFactory(@NotNull Class<?> clazz) {
        return initializationFactory.get(clazz);
    }

    /**
     * Removes {@link InstanceFactory} from the Initialization Factory.
     *
     * @param clazz - Class which mapping for the {@link InstanceFactory}.
     * @return - the previous {@link InstanceFactory} associated with Class, or null if there was no mapping for Class.
     */
    @Nullable public InstanceFactory removeInstanceFactory(@NotNull Class<?> clazz) {
        return initializationFactory.remove(clazz);
    }

    /**
     * Associates the specified {@link InstanceFactory} with the specified Class in the Initialization Factory If the
     * Initialization Factory previously contained a mapping for the Class, the old {@link InstanceFactory} is replaced
     * by the specified {@link InstanceFactory}.
     *
     * @param clazz - Class which mapping for the {@link InstanceFactory}.
     * @param factory - InstanceFactory which mapping for Class.
     */
    public void putInstanceFactory(@NotNull Class<?> clazz, @NotNull InstanceFactory factory) {
        BinaryWriteMode mode = BinaryUtils.mode(clazz);

        assert (mode == BinaryWriteMode.BINARY || mode == BinaryWriteMode.OBJECT) : "The initialization factory doesn't support the class: " + clazz;

        initializationFactory.put(clazz, factory);
    }

    /**
     * Reset the instances Initialization Factory.
     */
    public void resetInitializationFactory() {
        this.initializationFactory = new ConcurrentHashMap8<>();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryConfiguration.class, this);
    }
}
