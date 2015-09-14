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

package org.apache.ignite.internal.portable.api;

import java.sql.Timestamp;
import java.util.Collection;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Defines configuration properties for a specific portable type. Providing per-type
 * configuration is optional, as it is generally enough, and also optional, to provide global portable
 * configuration using {@link PortableMarshaller#setClassNames(Collection)}.
 * However, this class allows you to change configuration properties for a specific
 * portable type without affecting configuration for other portable types.
 * <p>
 * Per-type portable configuration can be specified in {@link PortableMarshaller#getTypeConfigurations()} method.
 */
public class PortableTypeConfiguration {
    /** Class name. */
    private String clsName;

    /** ID mapper. */
    private PortableIdMapper idMapper;

    /** Serializer. */
    private PortableSerializer serializer;

    /** Use timestamp flag. */
    private Boolean useTs;

    /** Meta data enabled flag. */
    private Boolean metaDataEnabled;

    /** Keep deserialized flag. */
    private Boolean keepDeserialized;

    /** Affinity key field name. */
    private String affKeyFieldName;

    /**
     */
    public PortableTypeConfiguration() {
        // No-op.
    }

    /**
     * @param clsName Class name.
     */
    public PortableTypeConfiguration(String clsName) {
        this.clsName = clsName;
    }

    /**
     * Gets type name.
     *
     * @return Type name.
     */
    public String getClassName() {
        return clsName;
    }

    /**
     * Sets type name.
     *
     * @param clsName Type name.
     */
    public void setClassName(String clsName) {
        this.clsName = clsName;
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
     * If {@code true} then date values converted to {@link Timestamp} during unmarshalling.
     *
     * @return Flag indicating whether date values converted to {@link Timestamp} during unmarshalling.
     */
    public Boolean isUseTimestamp() {
        return useTs;
    }

    /**
     * @param useTs Flag indicating whether date values converted to {@link Timestamp} during unmarshalling.
     */
    public void setUseTimestamp(Boolean useTs) {
        this.useTs = useTs;
    }

    /**
     * Defines whether meta data is collected for this type. If provided, this value will override
     * {@link PortableMarshaller#isMetaDataEnabled()} property.
     *
     * @return Whether meta data is collected.
     */
    public Boolean isMetaDataEnabled() {
        return metaDataEnabled;
    }

    /**
     * @param metaDataEnabled Whether meta data is collected.
     */
    public void setMetaDataEnabled(Boolean metaDataEnabled) {
        this.metaDataEnabled = metaDataEnabled;
    }

    /**
     * Defines whether {@link PortableObject} should cache deserialized instance. If provided,
     * this value will override {@link PortableMarshaller#isKeepDeserialized()}
     * property.
     *
     * @return Whether deserialized value is kept.
     */
    public Boolean isKeepDeserialized() {
        return keepDeserialized;
    }

    /**
     * @param keepDeserialized Whether deserialized value is kept.
     */
    public void setKeepDeserialized(Boolean keepDeserialized) {
        this.keepDeserialized = keepDeserialized;
    }

    /**
     * Gets affinity key field name.
     *
     * @return Affinity key field name.
     */
    public String getAffinityKeyFieldName() {
        return affKeyFieldName;
    }

    /**
     * Sets affinity key field name.
     *
     * @param affKeyFieldName Affinity key field name.
     */
    public void setAffinityKeyFieldName(String affKeyFieldName) {
        this.affKeyFieldName = affKeyFieldName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PortableTypeConfiguration.class, this, super.toString());
    }
}