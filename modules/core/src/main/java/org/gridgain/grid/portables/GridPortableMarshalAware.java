/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.portables;

/**
 * Interface that allows to implement custom serialization
 * logic for portable objects. Portable objects are not required
 * to implement this interface, in which case GridGain will automatically
 * serialize portable objects using reflection.
 * <p>
 * This interface, in a way, is analogous to {@link java.io.Externalizable}
 * interface, which allows users to override default serialization logic,
 * usually for performance reasons. The only difference here is that portable
 * serialization is already very fast and implementing custom serialization
 * logic for portables does not provide significant performance gains.
 * <p>
 * Note that class implementing this interface should have default public constructor
 * (same requirement as {@link java.io.Externalizable}).
 */
public interface GridPortableMarshalAware {
    /**
     * Writes fields to provided writer.
     *
     * @param writer Portable object writer.
     * @throws GridPortableException In case of error.
     */
    public void writePortable(GridPortableWriter writer) throws GridPortableException;

    /**
     * Reads fields from provided reader.
     *
     * @param reader Portable object reader.
     * @throws GridPortableException In case of error.
     */
    public void readPortable(GridPortableReader reader) throws GridPortableException;
}
