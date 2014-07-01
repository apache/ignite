/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.portable;

/**
 * Interface that allows to implement custom serialization/deserialization
 * logic for portable objects.
 */
public interface GridPortable {
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
