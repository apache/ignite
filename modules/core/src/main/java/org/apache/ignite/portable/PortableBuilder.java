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

package org.apache.ignite.portable;

import org.apache.ignite.*;

import org.jetbrains.annotations.*;

/**
 * Portable object builder. Provides ability to build portable objects dynamically without having class definitions.
 * <p>
 * Here is an example of how a portable object can be built dynamically:
 * <pre name=code class=java>
 * PortableBuilder builder = Ignition.ignite().portables().builder("org.project.MyObject");
 *
 * builder.setField("fieldA", "A");
 * builder.setField("fieldB", "B");
 *
 * PortableObject portableObj = builder.build();
 * </pre>
 *
 * <p>
 * Also builder can be initialized by existing portable object. This allows changing some fields without affecting
 * other fields.
 * <pre name=code class=java>
 * PortableBuilder builder = Ignition.ignite().portables().builder(person);
 *
 * builder.setField("name", "John");
 *
 * person = builder.build();
 * </pre>
 * </p>
 *
 * If you need to modify nested portable object you can get builder for nested object using
 * {@link #getField(String)}, changes made on nested builder will affect parent object,
 * for example:
 *
 * <pre name=code class=java>
 * PortableBuilder personBuilder = grid.portables().createBuilder(personPortableObj);
 * PortableBuilder addressBuilder = personBuilder.setField("address");
 *
 * addressBuilder.setField("city", "New York");
 *
 * personPortableObj = personBuilder.build();
 *
 * // Should be "New York".
 * String city = personPortableObj.getField("address").getField("city");
 * </pre>
 *
 * @see IgnitePortables#builder(int)
 * @see IgnitePortables#builder(String)
 * @see IgnitePortables#builder(PortableObject)
 */
public interface PortableBuilder {
    /**
     * Returns value assigned to the specified field.
     * If the value is a portable object instance of {@code GridPortableBuilder} will be returned,
     * which can be modified.
     * <p>
     * Collections and maps returned from this method are modifiable.
     *
     * @param name Field name.
     * @return Filed value.
     */
    public <T> T getField(String name);

    /**
     * Sets field value.
     *
     * @param name Field name.
     * @param val Field value (cannot be {@code null}).
     * @see PortableObject#metaData()
     */
    public PortableBuilder setField(String name, Object val);

    /**
     * Sets field value with value type specification.
     * <p>
     * Field type is needed for proper metadata update.
     *
     * @param name Field name.
     * @param val Field value.
     * @param type Field type.
     * @see PortableObject#metaData()
     */
    public <T> PortableBuilder setField(String name, @Nullable T val, Class<? super T> type);

    /**
     * Sets field value.
     * <p>
     * This method should be used if field is portable object.
     *
     * @param name Field name.
     * @param builder Builder for object field.
     */
    public PortableBuilder setField(String name, @Nullable PortableBuilder builder);

    /**
     * Removes field from this builder.
     *
     * @param fieldName Field name.
     * @return {@code this} instance for chaining.
     */
    public PortableBuilder removeField(String fieldName);

    /**
     * Sets hash code for resulting portable object returned by {@link #build()} method.
     * <p>
     * If not set {@code 0} is used.
     *
     * @param hashCode Hash code.
     * @return {@code this} instance for chaining.
     */
    public PortableBuilder hashCode(int hashCode);

    /**
     * Builds portable object.
     *
     * @return Portable object.
     * @throws PortableException In case of error.
     */
    public PortableObject build() throws PortableException;
}
