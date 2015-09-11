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

package org.apache.ignite.internal.processors.platform.dotnet;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.portable.PortableException;
import org.apache.ignite.portable.PortableMarshalAware;
import org.apache.ignite.portable.PortableRawReader;
import org.apache.ignite.portable.PortableRawWriter;
import org.apache.ignite.portable.PortableReader;
import org.apache.ignite.portable.PortableWriter;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Mirror of .Net class PortableConfiguration.cs
 */
public class PlatformDotNetPortableConfiguration implements PortableMarshalAware {
    /** Type cfgs. */
    private Collection<PlatformDotNetPortableTypeConfiguration> typesCfg;

    /** Types. */
    private Collection<String> types;

    /** Default name mapper. */
    private String dfltNameMapper;

    /** Default id mapper. */
    private String dfltIdMapper;

    /** Default serializer. */
    private String dfltSerializer;

    /** Default metadata enabled. */
    private boolean dfltMetadataEnabled = true;

    /** Whether to cache deserialized value in IGridPortableObject */
    private boolean dfltKeepDeserialized = true;

    /**
     * Default constructor.
     */
    public PlatformDotNetPortableConfiguration() {
        // No-op.
    }

    /**
     * Copy constructor.
     * @param cfg configuration to copy.
     */
    public PlatformDotNetPortableConfiguration(PlatformDotNetPortableConfiguration cfg) {
        if (cfg.getTypesConfiguration() != null) {
            typesCfg = new ArrayList<>();

            for (PlatformDotNetPortableTypeConfiguration typeCfg : cfg.getTypesConfiguration())
                typesCfg.add(new PlatformDotNetPortableTypeConfiguration(typeCfg));
        }

        if (cfg.getTypes() != null)
            types = new ArrayList<>(cfg.getTypes());

        dfltNameMapper = cfg.getDefaultNameMapper();
        dfltIdMapper = cfg.getDefaultIdMapper();
        dfltSerializer = cfg.getDefaultSerializer();
        dfltMetadataEnabled = cfg.isDefaultMetadataEnabled();
        dfltKeepDeserialized = cfg.isDefaultKeepDeserialized();
    }

    /**
     * @return Type cfgs.
     */
    public Collection<PlatformDotNetPortableTypeConfiguration> getTypesConfiguration() {
        return typesCfg;
    }

    /**
     * @param typesCfg New type cfgs.
     */
    public void setTypesConfiguration(Collection<PlatformDotNetPortableTypeConfiguration> typesCfg) {
        this.typesCfg = typesCfg;
    }

    /**
     * @return Types.
     */
    public Collection<String> getTypes() {
        return types;
    }

    /**
     * @param types New types.
     */
    public void setTypes(Collection<String> types) {
        this.types = types;
    }

    /**
     * @return Default name mapper.
     */
    public String getDefaultNameMapper() {
        return dfltNameMapper;
    }

    /**
     * @param dfltNameMapper New default name mapper.
     */
    public void setDefaultNameMapper(String dfltNameMapper) {
        this.dfltNameMapper = dfltNameMapper;
    }

    /**
     * @return Default id mapper.
     */
    public String getDefaultIdMapper() {
        return dfltIdMapper;
    }

    /**
     * @param dfltIdMapper New default id mapper.
     */
    public void setDefaultIdMapper(String dfltIdMapper) {
        this.dfltIdMapper = dfltIdMapper;
    }

    /**
     * @return Default serializer.
     */
    public String getDefaultSerializer() {
        return dfltSerializer;
    }

    /**
     * @param dfltSerializer New default serializer.
     */
    public void setDefaultSerializer(String dfltSerializer) {
        this.dfltSerializer = dfltSerializer;
    }

    /**
     * Gets default metadata enabled flag. See {@link #setDefaultMetadataEnabled(boolean)} for more information.
     *
     * @return Default metadata enabled flag.
     */
    public boolean isDefaultMetadataEnabled() {
        return dfltMetadataEnabled;
    }

    /**
     * Sets default metadata enabled flag. When set to {@code true} all portable types will save it's metadata to
     * cluster.
     * <p />
     * Can be overridden for particular type using
     * {@link PlatformDotNetPortableTypeConfiguration#setMetadataEnabled(Boolean)}.
     *
     * @param dfltMetadataEnabled Default metadata enabled flag.
     */
    public void setDefaultMetadataEnabled(boolean dfltMetadataEnabled) {
        this.dfltMetadataEnabled = dfltMetadataEnabled;
    }

    /**
     * Gets default keep deserialized flag. See {@link #setDefaultKeepDeserialized(boolean)} for more information.
     *
     * @return  Flag indicates whether to cache deserialized value in IGridPortableObject.
     */
    public boolean isDefaultKeepDeserialized() {
        return dfltKeepDeserialized;
    }

    /**
     * Sets default keep deserialized flag.
     * <p />
     * Can be overridden for particular type using
     * {@link PlatformDotNetPortableTypeConfiguration#setKeepDeserialized(Boolean)}.
     *
     * @param keepDeserialized Keep deserialized flag.
     */
    public void setDefaultKeepDeserialized(boolean keepDeserialized) {
        this.dfltKeepDeserialized = keepDeserialized;
    }

    /** {@inheritDoc} */
    @Override public void writePortable(PortableWriter writer) throws PortableException {
        PortableRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeCollection(typesCfg);
        rawWriter.writeCollection(types);
        rawWriter.writeString(dfltNameMapper);
        rawWriter.writeString(dfltIdMapper);
        rawWriter.writeString(dfltSerializer);
        rawWriter.writeBoolean(dfltMetadataEnabled);
        rawWriter.writeBoolean(dfltKeepDeserialized);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(PortableReader reader) throws PortableException {
        PortableRawReader rawReader = reader.rawReader();

        typesCfg = rawReader.readCollection();
        types = rawReader.readCollection();
        dfltNameMapper = rawReader.readString();
        dfltIdMapper = rawReader.readString();
        dfltSerializer = rawReader.readString();
        dfltMetadataEnabled = rawReader.readBoolean();
        dfltKeepDeserialized = rawReader.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformDotNetPortableConfiguration.class, this);
    }
}