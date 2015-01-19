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

package org.gridgain.grid.dotnet;

import org.apache.ignite.portables.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 * Mirror of .Net class GridDotNetPortableTypeConfiguration.cs
 */
public class GridDotNetPortableTypeConfiguration implements PortableMarshalAware {
    /** */
    private String assemblyName;

    /** */
    private String typeName;

    /** */
    private String nameMapper;

    /** */
    private String idMapper;

    /** */
    private String serializer;

    /** */
    private String affinityKeyFieldName;

    /** */
    private Boolean metadataEnabled;

    /** Whether to cache deserialized value in IGridPortableObject. */
    private Boolean keepDeserialized;

    /**
     * Default constructor.
     */
    public GridDotNetPortableTypeConfiguration() {
        // No-op.
    }

    /**
     * Copy constructor.
     * @param cfg configuration to copy.
     */
    public GridDotNetPortableTypeConfiguration(GridDotNetPortableTypeConfiguration cfg) {
        assemblyName = cfg.getAssemblyName();
        typeName = cfg.getTypeName();
        nameMapper = cfg.getNameMapper();
        idMapper = cfg.getIdMapper();
        serializer = cfg.getSerializer();
        affinityKeyFieldName = cfg.getAffinityKeyFieldName();
        metadataEnabled = cfg.getMetadataEnabled();
        keepDeserialized = cfg.isKeepDeserialized();
    }

    /**
     * @return Assembly name.
     */
    public String getAssemblyName() {
        return assemblyName;
    }

    /**
     * @param assemblyName New assembly name.
     */
    public void setAssemblyName(String assemblyName) {
        this.assemblyName = assemblyName;
    }

    /**
     * @return Type name.
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * @param typeName New type name.
     */
    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    /**
     * @return Name mapper.
     */
    public String getNameMapper() {
        return nameMapper;
    }

    /**
     * @param nameMapper New name mapper.
     */
    public void setNameMapper(String nameMapper) {
        this.nameMapper = nameMapper;
    }

    /**
     * @return Id mapper.
     */
    public String getIdMapper() {
        return idMapper;
    }

    /**
     * @param idMapper New id mapper.
     */
    public void setIdMapper(String idMapper) {
        this.idMapper = idMapper;
    }

    /**
     * @return Serializer.
     */
    public String getSerializer() {
        return serializer;
    }

    /**
     * @param serializer New serializer.
     */
    public void setSerializer(String serializer) {
        this.serializer = serializer;
    }

    /**
     * @return Metadata enabled.
     */
    public Boolean getMetadataEnabled() {
        return metadataEnabled;
    }

    /**
     * @param metadataEnabled New metadata enabled.
     */
    public void setMetadataEnabled(Boolean metadataEnabled) {
        this.metadataEnabled = metadataEnabled;
    }

    /**
     * @return Affinity key field name.
     */
    public String getAffinityKeyFieldName() {
        return affinityKeyFieldName;
    }

    /**
     * @param affinityKeyFieldName Affinity key field name.
     */
    public void setAffinityKeyFieldName(String affinityKeyFieldName) {
        this.affinityKeyFieldName = affinityKeyFieldName;
    }

    /**
     * @return Flag indicates whether to cache deserialized value in IGridPortableObject.
     */
    @Nullable public Boolean isKeepDeserialized() {
        return keepDeserialized;
    }

    /**
     * @param keepDeserialized Keep deserialized flag.
     */
    public void setKeepDeserialized(@Nullable Boolean keepDeserialized) {
        this.keepDeserialized = keepDeserialized;
    }

    /** {@inheritDoc} */
    @Override public void writePortable(PortableWriter writer) throws PortableException {
        PortableRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeString(assemblyName);
        rawWriter.writeString(typeName);
        rawWriter.writeString(nameMapper);
        rawWriter.writeString(idMapper);
        rawWriter.writeString(serializer);
        rawWriter.writeString(affinityKeyFieldName);
        rawWriter.writeObject(metadataEnabled);
        rawWriter.writeObject(keepDeserialized);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(PortableReader reader) throws PortableException {
        PortableRawReader rawReader = reader.rawReader();

        assemblyName = rawReader.readString();
        typeName = rawReader.readString();
        nameMapper = rawReader.readString();
        idMapper = rawReader.readString();
        serializer = rawReader.readString();
        affinityKeyFieldName = rawReader.readString();
        metadataEnabled = (Boolean)rawReader.readObject();
        keepDeserialized = (Boolean)rawReader.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDotNetPortableTypeConfiguration.class, this);
    }
}

