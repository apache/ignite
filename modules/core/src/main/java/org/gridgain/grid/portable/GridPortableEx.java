/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.portable;

import java.io.*;

/**
 * Extension if {@link GridPortable} interface that allows
 * to implement custom serialization/deserialization logic.
 */
public interface GridPortableEx {
    /**
     * @param writer Portable object writer.
     * @throws IOException In case of error.
     */
    public void writePortable(GridPortableWriter writer) throws IOException;

    /**
     * @param reader Portable object reader.
     * @throws IOException In case of error.
     */
    public void readPortable(GridPortableReader reader) throws IOException;
}
