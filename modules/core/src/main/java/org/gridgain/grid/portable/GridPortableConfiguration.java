/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.portable;

import java.util.*;

/**
 * Common portable configuration.
 */
public class GridPortableConfiguration {
    /** ID mapper. */
    private GridPortableIdMapper idMapper;

    /** Serializer. */
    private GridPortableSerializer serializer;

    /** Types. */
    private Collection<GridPortableTypeConfiguration> typeCfgs;

    /**
     * Gets ID mapper.
     *
     * @return ID mapper.
     */
    public GridPortableIdMapper getIdMapper() {
        return idMapper;
    }

    /**
     * Sets ID mapper.
     *
     * @param idMapper ID mapper.
     */
    public void setIdMapper(GridPortableIdMapper idMapper) {
        this.idMapper = idMapper;
    }

    /**
     * Gets serializer.
     *
     * @return Serializer.
     */
    public GridPortableSerializer getSerializer() {
        return serializer;
    }

    /**
     * Sets serializer.
     *
     * @param serializer Serializer.
     */
    public void setSerializer(GridPortableSerializer serializer) {
        this.serializer = serializer;
    }

    /**
     * Gets types configuration.
     *
     * @return Types configuration.
     */
    public Collection<GridPortableTypeConfiguration> getTypeConfigurations() {
        return typeCfgs;
    }

    /**
     * Sets type configurations.
     *
     * @param typeCfgs Type configurations.
     */
    public void setTypeConfigurations(Collection<GridPortableTypeConfiguration> typeCfgs) {
        this.typeCfgs = typeCfgs;
    }
}
