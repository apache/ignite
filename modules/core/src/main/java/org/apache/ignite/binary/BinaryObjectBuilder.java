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

package org.apache.ignite.binary;

import org.jetbrains.annotations.Nullable;

/**
 * Binary object builder. Provides ability to build binary objects dynamically without having class definitions.
 * <p>
 * Here is an example of how a binary object can be built dynamically:
 * <pre name=code class=java>
 * BinaryObjectBuilder builder = Ignition.ignite().binary().builder("org.project.MyObject");
 *
 * builder.setField("fieldA", "A");
 * builder.setField("fieldB", "B");
 *
 * BinaryObject binaryObj = builder.build();
 * </pre>
 *
 * <p>
 * Also builder can be initialized by existing binary object. This allows changing some fields without affecting
 * other fields.
 * <pre name=code class=java>
 * BinaryObjectBuilder builder = Ignition.ignite().binary().builder(person);
 *
 * builder.setField("name", "John");
 *
 * person = builder.build();
 * </pre>
 * </p>
 *
 * If you need to modify nested binary object you can get an instance of a builder for nested binary object using
 * {@link #getField(String)}, changes made on nested builder will affect parent object,
 * for example:
 *
 * <pre name=code class=java>
 * BinaryObjectBuilder personBuilder = grid.binary().createBuilder(personBinaryObj);
 * BinaryObjectBuilder addressBuilder = personBuilder.getField("address");
 *
 * addressBuilder.setField("city", "New York");
 *
 * personBinaryObj = personBuilder.build();
 *
 * // Should be "New York".
 * String city = personBinaryObj.getField("address").getField("city");
 * </pre>
 *
 * @see org.apache.ignite.IgniteBinary#builder(String)
 * @see org.apache.ignite.IgniteBinary#builder(BinaryObject)
 */
public interface BinaryObjectBuilder {
    /**
     * Returns value assigned to the specified field.
     * If the value is a binary object then an instance of {@code BinaryObjectBuilder} will be returned,
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
     * @see BinaryObject#type()
     */
    public BinaryObjectBuilder setField(String name, Object val);

    /**
     * Sets field value with value type specification.
     * <p>
     * Field type is needed for proper metadata update.
     *
     * @param name Field name.
     * @param val Field value.
     * @param type Field type.
     * @see BinaryObject#type()
     */
    public <T> BinaryObjectBuilder setField(String name, @Nullable T val, Class<? super T> type);

    /**
     * Sets field value.
     * <p>
     * This method should be used if field is binary object.
     *
     * @param name Field name.
     * @param builder Builder for object field.
     */
    public BinaryObjectBuilder setField(String name, @Nullable BinaryObjectBuilder builder);

    /**
     * Removes field from this builder.
     *
     * @param fieldName Field name.
     * @return {@code this} instance for chaining.
     */
    public BinaryObjectBuilder removeField(String fieldName);

    /**
     * Sets hash code for resulting binary object returned by {@link #build()} method.
     * <p>
     * If not set {@code 0} is used.
     *
     * @param hashCode Hash code.
     * @return {@code this} instance for chaining.
     */
    public BinaryObjectBuilder hashCode(int hashCode);

    /**
     * Builds binary object.
     *
     * @return Binary object.
     * @throws BinaryObjectException In case of error.
     */
    public BinaryObject build() throws BinaryObjectException;
}
