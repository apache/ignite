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

package org.apache.ignite.platform.dotnet;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Mirror of .Net class BinaryConfiguration.cs
 */
public class PlatformDotNetBinaryConfiguration {
    /** Type cfgs. */
    private Collection<PlatformDotNetBinaryTypeConfiguration> typesCfg;

    /** Types. */
    private Collection<String> types;

    /** Default name mapper. */
    private String dfltNameMapper;

    /** Default id mapper. */
    private String dfltIdMapper;

    /** Default serializer. */
    private String dfltSerializer;

    /** Whether to cache deserialized value in IGridBinaryObject */
    private boolean dfltKeepDeserialized = true;

    /**
     * Default constructor.
     */
    public PlatformDotNetBinaryConfiguration() {
        // No-op.
    }

    /**
     * Copy constructor.
     * @param cfg configuration to copy.
     */
    public PlatformDotNetBinaryConfiguration(PlatformDotNetBinaryConfiguration cfg) {
        if (cfg.getTypesConfiguration() != null) {
            typesCfg = new ArrayList<>();

            for (PlatformDotNetBinaryTypeConfiguration typeCfg : cfg.getTypesConfiguration())
                typesCfg.add(new PlatformDotNetBinaryTypeConfiguration(typeCfg));
        }

        if (cfg.getTypes() != null)
            types = new ArrayList<>(cfg.getTypes());

        dfltNameMapper = cfg.getDefaultNameMapper();
        dfltIdMapper = cfg.getDefaultIdMapper();
        dfltSerializer = cfg.getDefaultSerializer();
        dfltKeepDeserialized = cfg.isDefaultKeepDeserialized();
    }

    /**
     * @return Type cfgs.
     */
    public Collection<PlatformDotNetBinaryTypeConfiguration> getTypesConfiguration() {
        return typesCfg;
    }

    /**
     * @param typesCfg New type cfgs.
     * @return {@code this} for chaining.
     */
    public PlatformDotNetBinaryConfiguration setTypesConfiguration(
        Collection<PlatformDotNetBinaryTypeConfiguration> typesCfg) {
        this.typesCfg = typesCfg;

        return this;
    }

    /**
     * @return Types.
     */
    public Collection<String> getTypes() {
        return types;
    }

    /**
     * @param types New types.
     * @return {@code this} for chaining.
     */
    public PlatformDotNetBinaryConfiguration setTypes(Collection<String> types) {
        this.types = types;

        return this;
    }

    /**
     * @return Default name mapper.
     */
    public String getDefaultNameMapper() {
        return dfltNameMapper;
    }

    /**
     * @param dfltNameMapper New default name mapper.
     * @return {@code this} for chaining.
     */
    public PlatformDotNetBinaryConfiguration setDefaultNameMapper(String dfltNameMapper) {
        this.dfltNameMapper = dfltNameMapper;

        return this;
    }

    /**
     * @return Default id mapper.
     */
    public String getDefaultIdMapper() {
        return dfltIdMapper;
    }

    /**
     * @param dfltIdMapper New default id mapper.
     * @return {@code this} for chaining.
     */
    public PlatformDotNetBinaryConfiguration setDefaultIdMapper(String dfltIdMapper) {
        this.dfltIdMapper = dfltIdMapper;

        return this;
    }

    /**
     * @return Default serializer.
     */
    public String getDefaultSerializer() {
        return dfltSerializer;
    }

    /**
     * @param dfltSerializer New default serializer.
     * @return {@code this} for chaining.
     */
    public PlatformDotNetBinaryConfiguration setDefaultSerializer(String dfltSerializer) {
        this.dfltSerializer = dfltSerializer;

        return this;
    }

    /**
     * Gets default keep deserialized flag. See {@link #setDefaultKeepDeserialized(boolean)} for more information.
     *
     * @return  Flag indicates whether to cache deserialized value in IGridBinaryObject.
     */
    public boolean isDefaultKeepDeserialized() {
        return dfltKeepDeserialized;
    }

    /**
     * Sets default keep deserialized flag.
     * <p />
     * Can be overridden for particular type using
     * {@link PlatformDotNetBinaryTypeConfiguration#setKeepDeserialized(Boolean)}.
     *
     * @param keepDeserialized Keep deserialized flag.
     * @return {@code this} for chaining.
     */
    public PlatformDotNetBinaryConfiguration setDefaultKeepDeserialized(boolean keepDeserialized) {
        this.dfltKeepDeserialized = keepDeserialized;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformDotNetBinaryConfiguration.class, this);
    }
}
