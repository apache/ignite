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

package org.apache.ignite.hadoop.util;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * Name mapper which maps one user name to another based on predefined dictionary. If name is not found in the
 * dictionary, or dictionary is not defined, either passed user name or some default value could be returned.
 */
public class BasicUserNameMapper implements UserNameMapper {
    /** */
    private static final long serialVersionUID = 0L;

    /** Mappings. */
    private Map<String, String> mappings;

    /** Whether to use default user name. */
    private boolean useDfltUsrName;;

    /** Default user name. */
    private String dfltUsrName;

    /** {@inheritDoc} */
    @Nullable @Override public String map(String name) {
        String res = mappings != null ? mappings.get(name) : null;

        return res != null ? res : useDfltUsrName ? dfltUsrName : name;
    }

    /**
     * Get mappings.
     *
     * @return Mappings.
     */
    @Nullable public Map<String, String> getMappings() {
        return mappings;
    }

    /**
     * Set mappings.
     *
     * @param mappings Mappings.
     */
    public void setMappings(@Nullable Map<String, String> mappings) {
        this.mappings = mappings;
    }

    /**
     * Get whether to use default user name when there is no mapping for current user name.
     *
     * @return Whether to use default user name.
     */
    public boolean isUseDefaultUserName() {
        return useDfltUsrName;
    }

    /**
     * Set whether to use default user name when there is no mapping for current user name.
     *
     * @param useDfltUsrName Whether to use default user name.
     */
    public void setUseDefaultUserName(boolean useDfltUsrName) {
        this.useDfltUsrName = useDfltUsrName;
    }

    /**
     * Get default user name (optional).
     * <p>
     * This user name will be used if provided mappings doesn't contain mapping for the given user name and
     * {#isUseDefaultUserName} is set to {@code true}.
     * <p>
     * Defaults to {@code null}.
     *
     * @return Default user name.
     */
    @Nullable public String getDefaultUserName() {
        return dfltUsrName;
    }

    /**
     * Set default user name (optional). See {@link #getDefaultUserName()} for more information.
     *
     * @param dfltUsrName Default user name.
     */
    public void setDefaultUserName(@Nullable String dfltUsrName) {
        this.dfltUsrName = dfltUsrName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BasicUserNameMapper.class, this);
    }
}
