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

package org.apache.ignite.portables;

import org.apache.ignite.internal.util.typedef.internal.*;

import java.sql.Timestamp;
import java.util.*;

/**
 * Defines configuration for GridGain portable functionality. All configuration
 * properties defined here can be overridden on per-type level in
 * {@link PortableTypeConfiguration}. Type configurations are provided via
 * {@link #getTypeConfigurations()} method.
 */
public class PortableConfiguration {
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

    /** Meta data enabled flag. */
    private boolean metaDataEnabled = true;

    /** Keep deserialized flag. */
    private boolean keepDeserialized = true;

    /**
     * Gets class names.
     *
     * @return Class names.
     */
    public Collection<String> getClassNames() {
        return clsNames;
    }

    /**
     * Sets class names.
     *
     * @param clsNames Class names.
     */
    public void setClassNames(Collection<String> clsNames) {
        this.clsNames = clsNames;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PortableConfiguration.class, this);
    }
}
