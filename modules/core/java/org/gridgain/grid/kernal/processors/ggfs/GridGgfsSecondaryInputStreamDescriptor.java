// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

/**
 * Descriptor of an input stream opened to the secondary file system.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridGgfsSecondaryInputStreamDescriptor {
    /** File info in the primary file system. */
    private final GridGgfsFileInfo info;

    /** Secondary file system input stream wrapper. */
    private final GridGgfsSecondaryInputStreamWrapper wrapper;

    /**
     * Constructor.
     *
     * @param info File info in the primary file system.
     * @param wrapper Secondary file system input stream wrapper.
     */
    GridGgfsSecondaryInputStreamDescriptor(GridGgfsFileInfo info, GridGgfsSecondaryInputStreamWrapper wrapper) {
        assert info != null;
        assert wrapper != null;

        this.info = info;
        this.wrapper = wrapper;
    }

    /**
     * @return File info in the primary file system.
     */
    GridGgfsFileInfo info() {
        return info;
    }

    /**
     * @return Secondary file system input stream wrapper.
     */
    GridGgfsSecondaryInputStreamWrapper wrapper() {
        return wrapper;
    }
}
