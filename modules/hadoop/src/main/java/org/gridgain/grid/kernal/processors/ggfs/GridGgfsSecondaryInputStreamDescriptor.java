/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.ggfs.*;

/**
 * Descriptor of an input stream opened to the secondary file system.
 */
public class GridGgfsSecondaryInputStreamDescriptor {
    /** File info in the primary file system. */
    private final GridGgfsFileInfo info;

    /** Secondary file system input stream wrapper. */
    private final GridGgfsReader secReader;

    /**
     * Constructor.
     *
     * @param info File info in the primary file system.
     * @param secReader Secondary file system reader.
     */
    GridGgfsSecondaryInputStreamDescriptor(GridGgfsFileInfo info, GridGgfsReader secReader) {
        assert info != null;
        assert secReader != null;

        this.info = info;
        this.secReader = secReader;
    }

    /**
     * @return File info in the primary file system.
     */
    GridGgfsFileInfo info() {
        return info;
    }

    /**
     * @return Secondary file system reader.
     */
    GridGgfsReader reader() {
        return secReader;
    }
}
