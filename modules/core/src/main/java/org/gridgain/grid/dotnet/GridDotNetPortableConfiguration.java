/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dotnet;

import org.gridgain.grid.portables.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Mirror of .Net class GridDotNetPortableConfiguration.cs
 */
public class GridDotNetPortableConfiguration implements GridPortableMarshalAware {
    /** */
    private Collection<GridDotNetPortableTypeConfiguration> typesCfg;

    /** */
    private Collection<String> types;

    /** */
    private String dfltNameMapper;

    /** */
    private String dfltIdMapper;

    /** */
    private String dfltSerializer;

    /** */
    private boolean dfltMetadataEnabled;

    /**
     * Default constructor.
     */
    public GridDotNetPortableConfiguration() {

    }

    /**
     * Copy constructor.
     * @param cfg configuration to copy.
     */
    public GridDotNetPortableConfiguration(GridDotNetPortableConfiguration cfg) {
        if (cfg.getTypesConfiguration() != null) {
            typesCfg = new ArrayList<>();

            for (GridDotNetPortableTypeConfiguration typeCfg : cfg.getTypesConfiguration())
                typesCfg.add(new GridDotNetPortableTypeConfiguration(typeCfg));
        }

        if (cfg.getTypes() != null)
            types = new ArrayList<>(cfg.getTypes());

        dfltNameMapper = cfg.getDefaultNameMapper();
        dfltIdMapper = cfg.getDefaultIdMapper();
        dfltSerializer = cfg.getDefaultSerializer();
        dfltMetadataEnabled = cfg.getDefaultMetadataEnabled();
    }

    /**
     * @return Type cfgs.
     */
    public Collection<GridDotNetPortableTypeConfiguration> getTypesConfiguration() {
        return typesCfg;
    }

    /**
     * @param typesCfg New type cfgs.
     */
    public void setTypesConfiguration(Collection<GridDotNetPortableTypeConfiguration> typesCfg) {
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
     * @return Default metadata enabled.
     */
    public boolean getDefaultMetadataEnabled() {
        return dfltMetadataEnabled;
    }

    /**
     * @param dfltMetadataEnabled New default metadata enabled.
     */
    public void setDefaultMetadataEnabled(boolean dfltMetadataEnabled) {
        this.dfltMetadataEnabled = dfltMetadataEnabled;
    }

    /** {@inheritDoc} */
    @Override public void writePortable(GridPortableWriter writer) throws GridPortableException {
        GridPortableRawWriter rawWriter = writer.rawWriter();

        rawWriter.writeCollection(typesCfg);

        rawWriter.writeCollection(types);

        rawWriter.writeString(dfltNameMapper);

        rawWriter.writeString(dfltIdMapper);

        rawWriter.writeString(dfltSerializer);

        rawWriter.writeBoolean(dfltMetadataEnabled);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(GridPortableReader reader) throws GridPortableException {
        GridPortableRawReader rawReader = reader.rawReader();

        typesCfg = rawReader.readCollection();

        types = rawReader.readCollection();

        dfltNameMapper = rawReader.readString();

        dfltIdMapper = rawReader.readString();

        dfltSerializer = rawReader.readString();

        dfltMetadataEnabled = rawReader.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDotNetPortableConfiguration.class, this);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDotNetPortableConfiguration.class, this);
    }
}
