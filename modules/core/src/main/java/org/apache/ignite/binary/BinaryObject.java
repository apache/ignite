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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeMap;

/**
 * Wrapper for binary object in binary format. Once an object is defined as binary,
 * Ignite will always store it in memory in the binary format.
 * User can choose to work either with the binary format or with the deserialized form
 * (assuming that class definitions are present in the classpath).
 * <p>
 * <b>NOTE:</b> user does not need to (and should not) implement this interface directly.
 * <p>
 * To work with the binary format directly, user should create a cache projection
 * over {@code BinaryObject} class and then retrieve individual fields as needed:
 * <pre name=code class=java>
 * IgniteCache&lt;BinaryObject, BinaryObject&gt; prj = cache.withKeepBinary();
 *
 * // Convert instance of MyKey to binary format.
 * // We could also use BinaryObjectBuilder to create the key in binary format directly.
 * BinaryObject key = ignite.binary().toBinary(new MyKey());
 *
 * BinaryObject val = prj.get(key);
 *
 * String field = val.field("myFieldName");
 * </pre>
 * Alternatively, if we have class definitions in the classpath, we may choose to work with deserialized
 * typed objects at all times. In this case we do incur the deserialization cost.
 * <pre name=code class=java>
 * IgniteCache&lt;MyKey.class, MyValue.class&gt; cache = grid.cache(null);
 *
 * MyValue val = cache.get(new MyKey());
 *
 * // Normal java getter.
 * String fieldVal = val.getMyFieldName();
 * </pre>
 * <h1 class="header">Working With Maps and Collections</h1>
 * All maps and collections in binary objects are serialized automatically. When working
 * with different platforms, e.g. C++ or .NET, Ignite will automatically pick the most
 * adequate collection or map in either language. For example, {@link ArrayList} in Java will become
 * {@code List} in C#, {@link LinkedList} in Java is {@link LinkedList} in C#, {@link HashMap}
 * in Java is {@code Dictionary} in C#, and {@link TreeMap} in Java becomes {@code SortedDictionary}
 * in C#, etc.
 * <h1 class="header">Dynamic Structure Changes</h1>
 * Since objects are always cached in the binary format, server does not need to
 * be aware of the class definitions. Moreover, if class definitions are not present or not
 * used on the server, then clients can continuously change the structure of the binary
 * objects without having to restart the cluster. For example, if one client stores a
 * certain class with fields A and B, and another client stores the same class with
 * fields B and C, then the server-side binary object will have the fields A, B, and C.
 * As the structure of a binary object changes, the new fields become available for SQL queries
 * automatically.
 * <h1 class="header">Building Binary Objects</h1>
 * Ignite comes with {@link BinaryObjectBuilder} which allows to build binary objects dynamically:
 * <pre name=code class=java>
 * BinaryObjectBuilder builder = Ignition.ignite().binary().builder("org.project.MyObject");
 *
 * builder.setField("fieldA", "A");
 * builder.setField("fieldB", "B");
 *
 * BinaryObject binaryObj = builder.build();
 * </pre>
 * For the cases when class definition is present
 * in the class path, it is also possible to populate a standard POJO and then
 * convert it to binary format, like so:
 * <pre name=code class=java>
 * MyObject obj = new MyObject();
 *
 * obj.setFieldA("A");
 * obj.setFieldB(123);
 *
 * BinaryObject binaryObj = Ignition.ignite().binary().toBinary(obj);
 * </pre>
 * <h1 class="header">Binary Type Metadata</h1>
 * Even though Ignite binary protocol only works with hash codes for type and field names
 * to achieve better performance, Ignite provides metadata for all binary types which
 * can be queried ar runtime via any of the {@link org.apache.ignite.IgniteBinary#type(Class)}
 * methods. Having metadata also allows for proper formatting of {@code BinaryObject.toString()} method,
 * even when binary objects are kept in binary format only, which may be necessary for audit reasons.
 */
public interface BinaryObject extends Serializable, Cloneable {
    /**
     * Gets type information for this binary object.
     *
     * @return Binary object type information.
     * @throws BinaryObjectException In case of error.
     */
    public BinaryType type() throws BinaryObjectException;

    /**
     * Gets field value.
     *
     * @param fieldName Field name.
     * @return Field value.
     * @throws BinaryObjectException In case of any other error.
     */
    public <F> F field(String fieldName) throws BinaryObjectException;

    /**
     * Checks whether field exists in the object.
     *
     * @param fieldName Field name.
     * @return {@code True} if field exists.
     */
    public boolean hasField(String fieldName);

    /**
     * Gets fully deserialized instance of binary object.
     *
     * @return Fully deserialized instance of binary object.
     * @throws BinaryInvalidTypeException If class doesn't exist.
     * @throws BinaryObjectException In case of any other error.
     */
    public <T> T deserialize() throws BinaryObjectException;

    /**
     * Copies this binary object.
     *
     * @return Copy of this binary object.
     */
    public BinaryObject clone() throws CloneNotSupportedException;

    /**
     * Creates a new {@link BinaryObjectBuilder} based on this binary object. The following code
     * <pre name=code class=java>
     * BinaryObjectBuilder builder = binaryObject.toBuilder();
     * </pre>
     * is equivalent to
     * <pre name=code class=java>
     * BinaryObjectBuilder builder = ignite.binary().builder(binaryObject);
     * </pre>
     *
     * @return Binary object builder.
     * @throws BinaryObjectException If builder cannot be created.
     */
    public BinaryObjectBuilder toBuilder() throws BinaryObjectException;

    /**
     * Get ordinal for this enum object. Use {@link BinaryType#isEnum()} to check if object is of enum type.
     *
     * @return Ordinal.
     * @throws BinaryObjectException If object is not enum.
     */
    public int enumOrdinal() throws BinaryObjectException;

    /**
     * Get name for this enum object. Use {@link BinaryType#isEnum()} to check if object is of enum type.
     *
     * @return Name.
     * @throws BinaryObjectException If object is not enum.
     */
    public String enumName() throws BinaryObjectException;
}