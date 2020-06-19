/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.configuration;

import java.util.Set;

/**
 * Service class with information about class fields, which have problems in configurator.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class WrongFields {
    /** Missing in configuration fields. */
    private final Set<String> fields;

    /** Deprecated in configuration classes fields. */
    private final Set<String> deprecatedFields;

    /** Removed in configuration classes fields. */
    private final Set<String> rmvFields;

    /**
     * @param fields Missing fields.
     * @param deprecatedFields Deprecated fields.
     * @param rmvFields Removed fields.
     */
    public WrongFields(Set<String> fields, Set<String> deprecatedFields, Set<String> rmvFields) {
        this.fields = fields;
        this.deprecatedFields = deprecatedFields;
        this.rmvFields = rmvFields;
    }

    /**
     * @return Missed at configurator fields.
     */
    public Set<String> getFields() {
        return fields;
    }

    /**
     * @return Deprecated in configuration classes fields.
     */
    public Set<String> getDeprecatedFields() {
        return deprecatedFields;
    }

    /**
     * @return Removed in configuration classes fields.
     */
    public Set<String> getRemovedFields() {
        return rmvFields;
    }

    /**
     * Check that wrong fields are exists.
     *
     * @return {@code true} when problems in configurator are exist or {@code false} otherwise.
     */
    public boolean nonEmpty() {
        return !fields.isEmpty() || !deprecatedFields.isEmpty() || !rmvFields.isEmpty();
    }
}
