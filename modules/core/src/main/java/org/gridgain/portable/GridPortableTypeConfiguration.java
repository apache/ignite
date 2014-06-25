/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.portable;

/**
 * Portable type configuration.
 */
public class GridPortableTypeConfiguration {
    /** Class name. */
    private String clsName;

    /** ID mapper. */
    private GridPortableIdMapper idMapper;

    /** Serializer. */
    private GridPortableSerializer serializer;

    /**
     */
    public GridPortableTypeConfiguration() {
        // No-op.
    }

    /**
     * @param clsName Class name.
     */
    public GridPortableTypeConfiguration(String clsName) {
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
}
