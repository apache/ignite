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

package org.apache.ignite.binary;

import java.util.Collection;

/**
 * Binary type meta data. Metadata for binary types can be accessed from any of the
 * {@link org.apache.ignite.IgniteBinary#type(String)} methods.
 * Having metadata also allows for proper formatting of {@code BinaryObject#toString()} method,
 * even when binary objects are kept in binary format only, which may be necessary for audit reasons.
 */
public interface BinaryType {
    /**
     * Gets binary type name.
     *
     * @return Binary type name.
     */
    public String typeName();

    /**
     * Gets binary type ID.
     *
     * @return Binary type ID.
     */
    public int typeId();

    /**
     * Gets collection of all field names for this binary type.
     *
     * @return Collection of all field names for this binary type.
     */
    public Collection<String> fieldNames();

    /**
     * Gets name of the field type for a given field.
     *
     * @param fieldName Field name.
     * @return Field type name.
     */
    public String fieldTypeName(String fieldName);

    /**
     * Get {@link BinaryField} for the given field name. Later this field can be used for fast field's value
     * retrieval from concrete {@link BinaryObject}.
     *
     * @param fieldName Field name.
     * @return Binary field.
     */
    public BinaryField field(String fieldName);

    /**
     * Binary objects can optionally specify custom key-affinity mapping in the
     * configuration. This method returns the name of the field which should be
     * used for the key-affinity mapping.
     *
     * @return Affinity key field name.
     */
    public String affinityKeyFieldName();

    /**
     * Check whether type represents enum or not.
     *
     * @return {@code True} if type is enum.
     */
    public boolean isEnum();

    /**
     * @return Collection of enum values.
     */
    public Collection<BinaryObject> enumValues();
}