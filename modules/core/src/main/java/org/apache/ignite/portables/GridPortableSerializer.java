/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.portables;

/**
 * Interface that allows to implement custom serialization logic for portable objects.
 * Can be used instead of {@link GridPortableMarshalAware} in case if the class
 * cannot be changed directly.
 * <p>
 * Portable serializer can be configured for all portable objects via
 * {@link GridPortableConfiguration#getSerializer()} method, or for a specific
 * portable type via {@link GridPortableTypeConfiguration#getSerializer()} method.
 */
public interface GridPortableSerializer {
    /**
     * Writes fields to provided writer.
     *
     * @param obj Empty object.
     * @param writer Portable object writer.
     * @throws GridPortableException In case of error.
     */
    public void writePortable(Object obj, GridPortableWriter writer) throws GridPortableException;

    /**
     * Reads fields from provided reader.
     *
     * @param obj Empty object
     * @param reader Portable object reader.
     * @throws GridPortableException In case of error.
     */
    public void readPortable(Object obj, GridPortableReader reader) throws GridPortableException;
}
