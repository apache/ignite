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

package org.apache.ignite.internal.processors.marshaller;

import java.io.Serializable;
import org.jetbrains.annotations.Nullable;

/**
 *  Used to exchange mapping information on new mapping added or missing mapping requested flows.
 *  See {@link GridMarshallerMappingProcessor} javadoc for more information.
 */
public final class MarshallerMappingItem implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final byte platformId;

    /** */
    private final int typeId;

    /** */
    private String clsName;

    /**
     * Class name may be null when instance is created to request missing mapping from cluster.
     *
     * @param platformId Platform id.
     * @param typeId Type id.
     * @param clsName Class name. May be null in case when the item is created to request missing mapping from grid.
     */
    public MarshallerMappingItem(byte platformId, int typeId, @Nullable String clsName) {
        this.platformId = platformId;
        this.typeId = typeId;
        this.clsName = clsName;
    }

    /** */
    public int typeId() {
        return typeId;
    }

    /** */
    public byte platformId() {
        return platformId;
    }

    /** */
    public String className() {
        return clsName;
    }

    /**
     * @param clsName Class name.
     */
    public void className(String clsName) {
        this.clsName = clsName;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (!(obj instanceof MarshallerMappingItem))
            return false;

        MarshallerMappingItem that = (MarshallerMappingItem) obj;

        return platformId == that.platformId
                && typeId == that.typeId
                && (clsName != null ? clsName.equals(that.clsName) : that.clsName == null);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * 31 * ((int) platformId) + 31 * typeId + (clsName != null ? clsName.hashCode() : 0) ;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "[platformId: " + platformId + ", typeId:" + typeId + ", clsName: " + clsName + "]";
    }
}
