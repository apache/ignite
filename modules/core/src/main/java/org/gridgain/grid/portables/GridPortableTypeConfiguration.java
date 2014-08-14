/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.portables;

/**
 * Defines configuration properties for a specific portable type. Providing per-type
 * configuration is optional, as it is generally enough to provide global
 * portable configuration in {@link GridPortableConfiguration} instance. However,
 * this class allows you to change configuration properties for a specific
 * portable type without affecting configuration for other portable types.
 * <p>
 * Per-type portable configuration can be specified in
 * {@link GridPortableConfiguration#getTypeConfigurations()} method.
 */
public class GridPortableTypeConfiguration {
    /** Class name. */
    private String clsName;

    /** ID mapper. */
    private GridPortableIdMapper idMapper;

    /** Serializer. */
    private GridPortableSerializer serializer;

    /** Use timestamp flag. */
    private boolean useTs = true;

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

    /**
     * If {@code true} then date values converted to {@link java.sql.Timestamp} during unmarshalling.
     * Default value is {@code true}.
     *
     * @return Flag indicating whether date values converted to {@link java.sql.Timestamp} during unmarshalling.
     */
    public boolean isUseTimestamp() {
        return useTs;
    }

    /**
     * @param useTs Flag indicating whether date values converted to {@link java.sql.Timestamp} during unmarshalling.
     */
    public void setUseTimestamp(boolean useTs) {
        this.useTs = useTs;
    }
}
