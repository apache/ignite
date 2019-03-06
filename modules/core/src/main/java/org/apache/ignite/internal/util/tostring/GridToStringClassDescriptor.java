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

package org.apache.ignite.internal.util.tostring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Simple class descriptor containing simple and fully qualified class names as well as
 * the list of class fields.
 */
class GridToStringClassDescriptor {
    /** */
    private final String sqn;

    /** */
    private final String fqn;

    /** */
    private List<GridToStringFieldDescriptor> fields = new ArrayList<>();

    /**
     * @param cls Class.
     */
    GridToStringClassDescriptor(Class<?> cls) {
        assert cls != null;

        fqn = cls.getName();
        sqn = cls.getSimpleName();
    }

    /**
     * @param field Field descriptor to be added.
     */
    void addField(GridToStringFieldDescriptor field) {
        assert field != null;

        fields.add(field);
    }

    /** */
    void sortFields() {
        Collections.sort(fields, new Comparator<GridToStringFieldDescriptor>() {
            /** {@inheritDoc} */
            @Override public int compare(GridToStringFieldDescriptor arg0, GridToStringFieldDescriptor arg1) {
                return arg0.getOrder() < arg1.getOrder() ? -1 : arg0.getOrder() > arg1.getOrder() ? 1 : 0;
            }
        });
    }

    /**
     * @return Simple class name.
     */
    String getSimpleClassName() {
        return sqn;
    }

    /**
     * @return Fully qualified class name.
     */
    String getFullyQualifiedClassName() {
        return fqn;
    }

    /**
     * @return List of fields.
     */
    List<GridToStringFieldDescriptor> getFields() {
        return fields;
    }
}