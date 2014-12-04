/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop;

import org.apache.hadoop.fs.permission.*;
import org.gridgain.grid.*;

import java.util.*;

import static org.gridgain.grid.ggfs.IgniteFs.*;

/**
 * Hadoop file system properties.
 */
class GridGgfsHadoopFSProperties {
    /** Username. */
    private String usrName;

    /** Group name. */
    private String grpName;

    /** Permissions. */
    private FsPermission perm;

    /**
     * Constructor.
     *
     * @param props Properties.
     * @throws GridException In case of error.
     */
    GridGgfsHadoopFSProperties(Map<String, String> props) throws GridException {
        usrName = props.get(PROP_USER_NAME);
        grpName = props.get(PROP_GROUP_NAME);

        String permStr = props.get(PROP_PERMISSION);

        if (permStr != null) {
            try {
                perm = new FsPermission((short)Integer.parseInt(permStr, 8));
            }
            catch (NumberFormatException ignore) {
                throw new GridException("Permissions cannot be parsed: " + permStr);
            }
        }
    }

    /**
     * Get user name.
     *
     * @return User name.
     */
    String userName() {
        return usrName;
    }

    /**
     * Get group name.
     *
     * @return Group name.
     */
    String groupName() {
        return grpName;
    }

    /**
     * Get permission.
     *
     * @return Permission.
     */
    FsPermission permission() {
        return perm;
    }
}
