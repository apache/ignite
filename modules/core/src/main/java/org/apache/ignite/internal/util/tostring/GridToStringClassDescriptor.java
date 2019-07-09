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

package org.apache.ignite.internal.util.tostring;

import java.util.ArrayList;
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
    private ArrayList<GridToStringFieldDescriptor> fields = new ArrayList<>();

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
        fields.trimToSize();

        fields.sort(Comparator.comparingInt(GridToStringFieldDescriptor::getOrder));
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