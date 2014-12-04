/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.portables;

/**
 * Type and field ID mapper for portable objects. GridGain never writes full
 * strings for field or type names. Instead, for performance reasons, GridGain
 * writes integer hash codes for type and field names. It has been tested that
 * hash code conflicts for the type names or the field names
 * within the same type are virtually non-existent and, to gain performance, it is safe
 * to work with hash codes. For the cases when hash codes for different types or fields
 * actually do collide {@code GridPortableIdMapper} allows to override the automatically
 * generated hash code IDs for the type and field names.
 * <p>
 * Portable ID mapper can be configured for all portable objects via
 * {@link GridPortableConfiguration#getIdMapper()} method, or for a specific
 * portable type via {@link GridPortableTypeConfiguration#getIdMapper()} method.
 */
public interface GridPortableIdMapper {
    /**
     * Gets type ID for provided class name.
     * <p>
     * If {@code 0} is returned, hash code of class simple name will be used.
     *
     * @param clsName Class name.
     * @return Type ID.
     */
    public int typeId(String clsName);

    /**
     * Gets ID for provided field.
     * <p>
     * If {@code 0} is returned, hash code of field name will be used.
     *
     * @param typeId Type ID.
     * @param fieldName Field name.
     * @return Field ID.
     */
    public int fieldId(int typeId, String fieldName);
}
