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

package org.apache.ignite.ml.math;

import java.util.Map;

/**
 * Interface provides support for meta attributes on vectors and matrices.
 */
public interface MetaAttributes {
    /**
     * Implementation should return an instance of the map to store meta attributes.
     */
    public Map<String, Object> getMetaStorage();

    /**
     * Gets meta attribute with given name.
     *
     * @param name Name of the vector meta attribute to get.
     * @param <T> Attribute's type.
     */
    @SuppressWarnings("unchecked")
    public default <T> T getAttribute(String name) {
        return (T)getMetaStorage().get(name);
    }

    /**
     * Sets meta attribute with given name and value.
     *
     * @param name Name of the meta attribute.
     * @param val Attribute value.
     * @param <T> Attribute's type.
     */
    public default <T> void setAttribute(String name, T val) {
        getMetaStorage().put(name, val);
    }

    /**
     * Removes meta attribute with given name.
     *
     * @param name Name of the meta attribute.
     * @return {@code true} if attribute was present and was removed, {@code false} otherwise.
     */
    public default boolean removeAttribute(String name) {
        boolean is = getMetaStorage().containsKey(name);

        if (is)
            getMetaStorage().remove(name);

        return is;
    }

    /**
     * Checks if given meta attribute is present.
     *
     * @param name Attribute name to check.
     */
    public default boolean hasAttribute(String name) {
        return getMetaStorage().containsKey(name);
    }
}
