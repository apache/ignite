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

package org.apache.ignite.binary;

import java.util.Collection;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.marshaller.portable.PortableMarshaller;

/**
 * Defines configuration properties for a specific portable type. Providing per-type
 * configuration is optional, as it is generally enough, and also optional, to provide global portable
 * configuration using {@link PortableMarshaller#setClassNames(Collection)}.
 * However, this class allows you to change configuration properties for a specific
 * portable type without affecting configuration for other portable types.
 * <p>
 * Per-type portable configuration can be specified in {@link PortableMarshaller#getTypeConfigurations()} method.
 */
public class BinaryTypeConfiguration {
    /** Class name. */
    private String clsName;

    /** ID mapper. */
    private BinaryTypeIdMapper idMapper;

    /** Serializer. */
    private BinarySerializer serializer;

    /** Meta data enabled flag. */
    private Boolean metaDataEnabled;

    /** Keep deserialized flag. */
    private Boolean keepDeserialized;

    /**
     */
    public BinaryTypeConfiguration() {
        // No-op.
    }

    /**
     * @param clsName Class name.
     */
    public BinaryTypeConfiguration(String clsName) {
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
    public BinaryTypeIdMapper getIdMapper() {
        return idMapper;
    }

    /**
     * Sets ID mapper.
     *
     * @param idMapper ID mapper.
     */
    public void setIdMapper(BinaryTypeIdMapper idMapper) {
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
     * Defines whether {@link BinaryObject} should cache deserialized instance. If provided,
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryTypeConfiguration.class, this, super.toString());
    }
}