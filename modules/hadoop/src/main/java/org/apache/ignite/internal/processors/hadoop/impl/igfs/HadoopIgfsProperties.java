/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.hadoop.impl.igfs;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;

import java.util.Map;

/**
 * Hadoop file system properties.
 */
public class HadoopIgfsProperties {
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
     * @throws IgniteException In case of error.
     */
    public HadoopIgfsProperties(Map<String, String> props) throws IgniteException {
        if (props == null)
            return;

        usrName = props.get(IgfsUtils.PROP_USER_NAME);
        grpName = props.get(IgfsUtils.PROP_GROUP_NAME);

        String permStr = props.get(IgfsUtils.PROP_PERMISSION);

        if (permStr != null) {
            try {
                perm = new FsPermission((short)Integer.parseInt(permStr, 8));
            }
            catch (NumberFormatException ignore) {
                throw new IgniteException("Permissions cannot be parsed: " + permStr);
            }
        }
    }

    /**
     * Get user name.
     *
     * @return User name.
     */
    public String userName() {
        return usrName;
    }

    /**
     * Get group name.
     *
     * @return Group name.
     */
    public String groupName() {
        return grpName;
    }

    /**
     * Get permission.
     *
     * @return Permission.
     */
    public FsPermission permission() {
        return perm;
    }
}