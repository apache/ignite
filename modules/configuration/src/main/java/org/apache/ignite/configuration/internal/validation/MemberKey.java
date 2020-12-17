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

package org.apache.ignite.configuration.internal.validation;

import java.util.Objects;

/**
 * Configuration member key.
 */
public class MemberKey {
    /** Class of the field holder. */
    private final Class<?> clazz;

    /** Name of the field. */
    private final String fieldName;

    /** Constructor. */
    public MemberKey(Class<?> clazz, String fieldName) {
        this.clazz = clazz;
        this.fieldName = fieldName;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemberKey key = (MemberKey) o;
        return clazz.equals(key.clazz) &&
                fieldName.equals(key.fieldName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(clazz, fieldName);
    }
}
