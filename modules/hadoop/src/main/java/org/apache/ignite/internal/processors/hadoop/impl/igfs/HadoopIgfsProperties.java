/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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