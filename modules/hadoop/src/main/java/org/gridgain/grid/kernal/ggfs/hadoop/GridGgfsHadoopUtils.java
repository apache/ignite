/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Utility constants and methods for GGFS Hadoop file system.
 */
public class GridGgfsHadoopUtils {
    /** Parameter name for endpoint no embed mode flag. */
    public static final String PARAM_GGFS_ENDPOINT_NO_EMBED = "fs.ggfs.%s.endpoint.no_embed";

    /** Parameter name for endpoint no shared memory flag. */
    public static final String PARAM_GGFS_ENDPOINT_NO_LOCAL_SHMEM = "fs.ggfs.%s.endpoint.no_local_shmem";

    /** Parameter name for endpoint no local TCP flag. */
    public static final String PARAM_GGFS_ENDPOINT_NO_LOCAL_TCP = "fs.ggfs.%s.endpoint.no_local_tcp";

    /**
     * Get string parameter.
     *
     * @param cfg Configuration.
     * @param name Parameter name.
     * @param authority Authority.
     * @param dflt Default value.
     * @return String value.
     */
    public static String parameter(Configuration cfg, String name, String authority, String dflt) {
        return cfg.get(String.format(name, authority != null ? authority : ""), dflt);
    }

    /**
     * Get integer parameter.
     *
     * @param cfg Configuration.
     * @param name Parameter name.
     * @param authority Authority.
     * @param dflt Default value.
     * @return Integer value.
     * @throws IOException In case of parse exception.
     */
    public static int parameter(Configuration cfg, String name, String authority, int dflt) throws IOException {
        String name0 = String.format(name, authority != null ? authority : "");

        try {
            return cfg.getInt(name0, dflt);
        }
        catch (NumberFormatException ignore) {
            throw new IOException("Failed to parse parameter value to integer: " + name0);
        }
    }

    /**
     * Get boolean parameter.
     *
     * @param cfg Configuration.
     * @param name Parameter name.
     * @param authority Authority.
     * @param dflt Default value.
     * @return Boolean value.
     */
    public static boolean parameter(Configuration cfg, String name, String authority, boolean dflt) {
        return cfg.getBoolean(String.format(name, authority != null ? authority : ""), dflt);
    }

    /**
     * Cast GG exception to appropriate IO exception.
     *
     * @param e Exception to cast.
     * @return Casted exception.
     */
    public static IOException cast(GridException e) {
        return cast(e, null);
    }

    /**
     * Cast GG exception to appropriate IO exception.
     *
     * @param e Exception to cast.
     * @param path Path for exceptions.
     * @return Casted exception.
     */
    @SuppressWarnings("unchecked")
    public static IOException cast(GridException e, @Nullable String path) {
        assert e != null;

        // First check for any nested IOException; if exists - re-throw it.
        if (e.hasCause(IOException.class))
            return e.getCause(IOException.class);
        else if (e instanceof GridGgfsFileNotFoundException)
            return new FileNotFoundException(path); // TODO: Or PathNotFoundException?
        else if (e instanceof GridGgfsParentNotDirectoryException)
            return new ParentNotDirectoryException(path);
        else if (path != null && e instanceof GridGgfsDirectoryNotEmptyException)
            return new PathIsNotEmptyDirectoryException(path);
        else if (path != null && e instanceof IgniteFsPathAlreadyExistsException)
            return new PathExistsException(path);
        else
            return new IOException(e);
    }

    /**
     * Constructor.
     */
    private GridGgfsHadoopUtils() {
        // No-op.
    }
}
