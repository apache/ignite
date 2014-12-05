/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.portables;

import org.gridgain.grid.util.typedef.internal.*;

import java.sql.*;

/**
 * Defines configuration properties for a specific portable type. Providing per-type
 * configuration is optional, as it is generally enough to provide global
 * portable configuration in {@link PortableConfiguration} instance. However,
 * this class allows you to change configuration properties for a specific
 * portable type without affecting configuration for other portable types.
 * <p>
 * Per-type portable configuration can be specified in
 * {@link PortableConfiguration#getTypeConfigurations()} method.
 */
public class GridPortableTypeConfiguration {
    /** Class name. */
    private String clsName;

    /** ID mapper. */
    private PortableIdMapper idMapper;

    /** Serializer. */
    private GridPortableSerializer serializer;

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
     * {@link PortableConfiguration#isMetaDataEnabled()} property.
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
     * Defines whether {@link GridPortableObject} should cache deserialized instance. If provided,
     * this value will override {@link PortableConfiguration#isKeepDeserialized()} property.
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
        return S.toString(GridPortableTypeConfiguration.class, this, super.toString());
    }
}
