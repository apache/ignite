/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.common;

import java.util.*;

/**
 * Grid file system commands to call remotely.
 */
public enum GridGgfsIpcCommand {
    /** Handshake command which will send information necessary for client to handle requests correctly. */
    HANDSHAKE,

    /** GGFS status (free/used space). */
    STATUS,

    /** Check specified path exists in the file system. */
    EXISTS,

    /** Get information for the file in specified path. */
    INFO,

    /** Get directory summary. */
    PATH_SUMMARY,

    /** Update information for the file  in specified path. */
    UPDATE,

    /** Rename file. */
    RENAME,

    /** Delete file. */
    DELETE,

    /** Make directories. */
    MAKE_DIRECTORIES,

    /** List files under the specified path. */
    LIST_PATHS,

    /** List files under the specified path. */
    LIST_FILES,

    /** Get affinity block locations for data blocks of the file. */
    AFFINITY,

    /** Updates last access and last modification time for a path. */
    SET_TIMES,

    /** Open file for reading as an input stream. */
    OPEN_READ,

    /** Open existent file as output stream to append data to. */
    OPEN_APPEND,

    /** Create file and open output stream for writing data to. */
    OPEN_CREATE,

    /** Close stream. */
    CLOSE,

    /** Read file's data block. */
    READ_BLOCK,

    /** Write file's data block. */
    WRITE_BLOCK,

    /** Server response. */
    CONTROL_RESPONSE;

    /** All values */
    private static final List<GridGgfsIpcCommand> ALL = Arrays.asList(values());

    /**
     * Resolve command by its ordinal.
     *
     * @param ordinal Command ordinal.
     * @return Resolved command.
     */
    public static GridGgfsIpcCommand valueOf(int ordinal) {
        return ALL.get(ordinal);
    }
}
