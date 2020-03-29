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

package org.apache.ignite;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Defines binary objects functionality. With binary objects you are able to:
 * <ul>
 * <li>Seamlessly interoperate between Java, .NET, and C++.</li>
 * <li>Make any object binary with zero code change to your existing code.</li>
 * <li>Nest binary objects within each other.</li>
 * <li>Automatically handle {@code circular} or {@code null} references.</li>
 * <li>Automatically convert collections and maps between Java, .NET, and C++.</li>
 * <li>
 *      Optionally avoid deserialization of objects on the server side
 *      (objects are stored in {@link org.apache.ignite.binary.BinaryObject} format).
 * </li>
 * <li>Avoid need to have concrete class definitions on the server side.</li>
 * <li>Dynamically change structure of the classes without having to restart the cluster.</li>
 * <li>Index into binary objects for querying purposes.</li>
 * </ul>
 * <h1 class="header">Working With Binaries Directly</h1>
 * Once an object is defined as binary,
 * Ignite will always store it in memory in the binary (i.e. binary) format.
 * User can choose to work either with the binary format or with the deserialized form
 * (assuming that class definitions are present in the classpath).
 * <p>
 * To work with the binary format directly, user should create a special cache projection
 * using IgniteCache.withKeepBinary() method and then retrieve individual fields as needed:
 * <pre name=code class=java>
 * IgniteCache&lt;BinaryObject, BinaryObject&gt; prj = cache.withKeepBinary();
 *
 * // Convert instance of MyKey to binary format.
 * // We could also use BinaryBuilder to create the key in binary format directly.
 * BinaryObject key = grid.binary().toBinary(new MyKey());
 *
 * BinaryObject val = prj.get(key);
 *
 * String field = val.field("myFieldName");
 * </pre>
 * Alternatively, if we have class definitions in the classpath, we may choose to work with deserialized
 * typed objects at all times.
 * <pre name=code class=java>
 * IgniteCache&lt;MyKey.class, MyValue.class&gt; cache = grid.cache(null);
 *
 * MyValue val = cache.get(new MyKey());
 *
 * // Normal java getter.
 * String fieldVal = val.getMyFieldName();
 * </pre>
 * If we used, for example, one of the automatically handled binary types for a key, like integer,
 * and still wanted to work with binary binary format for values, then we would declare cache projection
 * as follows:
 * <pre name=code class=java>
 * IgniteCache&lt;Integer.class, BinaryObject&gt; prj = cache.withKeepBinary();
 * </pre>
 * <h1 class="header">Automatic Binary Types</h1>
 * Note that only binary classes are converted to {@link org.apache.ignite.binary.BinaryObject} format. Following
 * classes are never converted (e.g., {@link #toBinary(Object)} method will return original
 * object, and instances of these classes will be stored in cache without changes):
 * <ul>
 *     <li>All primitives (byte, int, ...) and there boxed versions (Byte, Integer, ...)</li>
 *     <li>Arrays of primitives (byte[], int[], ...)</li>
 *     <li>{@link String} and array of {@link String}s</li>
 *     <li>{@link UUID} and array of {@link UUID}s</li>
 *     <li>{@link Date} and array of {@link Date}s</li>
 *     <li>{@link Timestamp} and array of {@link Timestamp}s</li>
 *     <li>Enums and array of enums</li>
 *     <li>
 *         Maps, collections and array of objects (but objects inside
 *         them will still be converted if they are binary)
 *     </li>
 * </ul>
 * <h1 class="header">Working With Maps and Collections</h1>
 * All maps and collections in the binary objects are serialized automatically. When working
 * with different platforms, e.g. C++ or .NET, Ignite will automatically pick the most
 * adequate collection or map in either language. For example, {@link ArrayList} in Java will become
 * {@code List} in C#, {@link LinkedList} in Java is {@link LinkedList} in C#, {@link HashMap}
 * in Java is {@code Dictionary} in C#, and {@link TreeMap} in Java becomes {@code SortedDictionary}
 * in C#, etc.
 * <h1 class="header">Building Binary Objects</h1>
 * Ignite comes with {@link org.apache.ignite.binary.BinaryObjectBuilder} which allows to build binary objects dynamically:
 * <pre name=code class=java>
 * BinaryBuilder builder = Ignition.ignite().binary().builder();
 *
 * builder.typeId("MyObject");
 *
 * builder.stringField("fieldA", "A");
 * build.intField("fieldB", "B");
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
 * NOTE: you don't need to convert typed objects to binary format before storing
 * them in cache, Ignite will do that automatically.
 * <h1 class="header">Binary Metadata</h1>
 * Even though Ignite binary protocol only works with hash codes for type and field names
 * to achieve better performance, Ignite provides metadata for all binary types which
 * can be queried ar runtime via any of the {@link IgniteBinary#type(Class)}
 * methods. Having metadata also allows for proper formatting of {@code BinaryObject#toString()} method,
 * even when binary objects are kept in binary format only, which may be necessary for audit reasons.
 * <h1 class="header">Dynamic Structure Changes</h1>
 * Since objects are always cached in the binary binary format, server does not need to
 * be aware of the class definitions. Moreover, if class definitions are not present or not
 * used on the server, then clients can continuously change the structure of the binary
 * objects without having to restart the cluster. For example, if one client stores a
 * certain class with fields A and B, and another client stores the same class with
 * fields B and C, then the server-side binary object will have the fields A, B, and C.
 * As the structure of a binary object changes, the new fields become available for SQL queries
 * automatically.
 * <h1 class="header">Configuration</h1>
 * By default all your objects are considered as binary and no specific configuration is needed.
 * The only requirement Ignite imposes is that your object has an empty
 * constructor. Note, that since server side does not have to know the class definition,
 * you only need to list binary objects in configuration on the client side. However, if you
 * list them on the server side as well, then you get the ability to deserialize binary objects
 * into concrete types on the server as well as on the client.
 * <p>
 * Here is an example of binary configuration (note that star (*) notation is supported):
 * <pre name=code class=xml>
 * ...
 * &lt;!-- Explicit binary objects configuration. --&gt;
 * &lt;property name="marshaller"&gt;
 *     &lt;bean class="org.apache.ignite.marshaller.binary.BinaryMarshaller"&gt;
 *         &lt;property name="classNames"&gt;
 *             &lt;list&gt;
 *                 &lt;value&gt;my.package.for.binary.objects.*&lt;/value&gt;
 *                 &lt;value&gt;org.apache.ignite.examples.client.binary.Employee&lt;/value&gt;
 *             &lt;/list&gt;
 *         &lt;/property&gt;
 *     &lt;/bean&gt;
 * &lt;/property&gt;
 * ...
 * </pre>
 * or from code:
 * <pre name=code class=java>
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * BinaryMarshaller marsh = new BinaryMarshaller();
 *
 * marsh.setClassNames(Arrays.asList(
 *     Employee.class.getName(),
 *     Address.class.getName())
 * );
 *
 * cfg.setMarshaller(marsh);
 * </pre>
 * You can also specify class name for a binary object via {@link org.apache.ignite.binary.BinaryTypeConfiguration}.
 * Do it in case if you need to override other configuration properties on per-type level, like
 * ID-mapper, or serializer.
 * <h1 class="header">Custom Affinity Keys</h1>
 * Often you need to specify an alternate key (not the cache key) for affinity routing whenever
 * storing objects in cache. For example, if you are caching {@code Employee} object with
 * {@code Organization}, and want to colocate employees with organization they work for,
 * so you can process them together, you need to specify an alternate affinity key.
 * With binary objects you would have to do it as following:
 * <pre name=code class=xml>
 * &lt;property name="marshaller"&gt;
 *     &lt;bean class="org.gridgain.grid.marshaller.binary.BinaryMarshaller"&gt;
 *         ...
 *         &lt;property name="typeConfigurations"&gt;
 *             &lt;list&gt;
 *                 &lt;bean class="org.apache.ignite.binary.BinaryTypeConfiguration"&gt;
 *                     &lt;property name="className" value="org.apache.ignite.examples.client.binary.EmployeeKey"/&gt;
 *                     &lt;property name="affinityKeyFieldName" value="organizationId"/&gt;
 *                 &lt;/bean&gt;
 *             &lt;/list&gt;
 *         &lt;/property&gt;
 *         ...
 *     &lt;/bean&gt;
 * &lt;/property&gt;
 * </pre>
 * <h1 class="header">Serialization</h1>
 * Serialization and deserialization works out-of-the-box in Ignite. However, you can provide your own custom
 * serialization logic by optionally implementing {@link org.apache.ignite.binary.Binarylizable} interface, like so:
 * <pre name=code class=java>
 * public class Address implements BinaryMarshalAware {
 *     private String street;
 *     private int zip;
 *
 *     // Empty constructor required for binary deserialization.
 *     public Address() {}
 *
 *     &#64;Override public void writeBinary(BinaryWriter writer) throws BinaryException {
 *         writer.writeString("street", street);
 *         writer.writeInt("zip", zip);
 *     }
 *
 *     &#64;Override public void readBinary(BinaryReader reader) throws BinaryException {
 *         street = reader.readString("street");
 *         zip = reader.readInt("zip");
 *     }
 * }
 * </pre>
 * Alternatively, if you cannot change class definitions, you can provide custom serialization
 * logic in {@link org.apache.ignite.binary.BinarySerializer} either globally in
 * {@link org.apache.ignite.configuration.BinaryConfiguration} or
 * for a specific type via {@link org.apache.ignite.binary.BinaryTypeConfiguration} instance.
 * <p>
 * Similar to java serialization you can use {@code writeReplace()} and {@code readResolve()} methods.
 * <ul>
 *     <li>
 *         {@code readResolve} is defined as follows: {@code ANY-ACCESS-MODIFIER Object readResolve()}.
 *         It may be used to replace the de-serialized object by another one of your choice.
 *     </li>
 *     <li>
 *          {@code writeReplace} is defined as follows: {@code ANY-ACCESS-MODIFIER Object writeReplace()}. This method
 *          allows the developer to provide a replacement object that will be serialized instead of the original one.
 *     </li>
 * </ul>
 *
 * <h1 class="header">Custom ID Mappers</h1>
 * Ignite implementation uses name hash codes to generate IDs for class names or field names
 * internally. However, in cases when you want to provide your own ID mapping schema,
 * you can provide your own {@link org.apache.ignite.binary.BinaryIdMapper} implementation.
 * <p>
 * ID-mapper may be provided either globally in {@link org.apache.ignite.configuration.BinaryConfiguration},
 * or for a specific type via {@link org.apache.ignite.binary.BinaryTypeConfiguration} instance.
 * <h1 class="header">Query Indexing</h1>
 * Binary objects can be indexed for querying by specifying index fields in
 * {@link org.apache.ignite.cache.QueryEntity} inside of specific
 * {@link org.apache.ignite.configuration.CacheConfiguration} instance,
 * like so:
 * <pre name=code class=xml>
 * ...
 * &lt;bean class="org.apache.ignite.cache.CacheConfiguration"&gt;
 *     ...
 *     &lt;property name="queryEntities"&gt;
 *         &lt;list&gt;
 *             &lt;bean class="QueryEntity"&gt;
 *                 &lt;property name="type" value="Employee"/&gt;
 *
 *                 &lt;!-- Fields available from query. --&gt;
 *                 &lt;property name="fields"&gt;
 *                     &lt;map&gt;
 *                     &lt;entry key="name" value="java.lang.String"/&gt;
 *
 *                     &lt;!-- Nested binary objects can also be indexed. --&gt;
 *                     &lt;entry key="address.zip" value="java.lang.Integer" /&gt;
 *                     &lt;/map&gt;
 *                 &lt;/property&gt;
 *
 *                 &lt;!-- Aliases for full property name in dot notation. --&gt;
 *                 &lt;property name="fields"&gt;
 *                     &lt;map&gt;
 *                     &lt;entry key="address.zip" value="zip" /&gt;
 *                     &lt;/map&gt;
 *                 &lt;/property&gt;
 *
 *                 &lt;!-- Indexes configuration. --&gt;
 *                 &lt;property name="indexes"&gt;
 *                 &lt;list&gt;
 *                 &lt;bean class="org.apache.ignite.cache.QueryIndex"&gt;
 *                     &lt;property name="fields"&gt;
 *                          &lt;map&gt;
 *                          &lt;!-- The boolean value is the acceding flag. --&gt;
 *                          &lt;entry key="name" value="true"/&gt;
 *                          &lt;/map&gt;
 *                     &lt;/property&gt;
 *                 &lt;/bean&gt;
 *                 &lt;/list&gt;
 *                 &lt;/property&gt;
 *             &lt;/bean&gt;
 *         &lt;/list&gt;
 *     &lt;/property&gt;
 * &lt;/bean&gt;
 * </pre>
 */
public interface IgniteBinary {
    /**
     * Gets type ID for given type name.
     * If no user defined {@link org.apache.ignite.binary.BinaryIdMapper} is configured
     * via {@link org.apache.ignite.configuration.BinaryConfiguration}, then system mapper will be used.
     *
     *
     * @param typeName Type name.
     * @return Type ID which a type would have had if it has been registered in Ignite.
     */
    public int typeId(@NotNull String typeName);

    /**
     * Converts provided object to instance of {@link org.apache.ignite.binary.BinaryObject}.
     *
     * @param obj Object to convert.
     * @return Converted object or {@code null} if obj is null.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    public <T> T toBinary(@Nullable Object obj) throws BinaryObjectException;

    /**
     * Creates new binary builder.
     *
     * @param typeName Type name.
     * @return Newly binary builder.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    public BinaryObjectBuilder builder(String typeName) throws BinaryObjectException;

    /**
     * Creates binary builder initialized by existing binary object.
     *
     * @param binaryObj Binary object to initialize builder.
     * @return Binary builder.
     */
    public BinaryObjectBuilder builder(BinaryObject binaryObj) throws BinaryObjectException;

    /**
     * Gets metadata for provided class.
     *
     * @param cls Class.
     * @return Metadata.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    public BinaryType type(Class<?> cls) throws BinaryObjectException;

    /**
     * Gets metadata for provided class name.
     *
     * @param typeName Type name.
     * @return Metadata.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    public BinaryType type(String typeName) throws BinaryObjectException;

    /**
     * Gets metadata for provided type ID.
     *
     * @param typeId Type ID.
     * @return Metadata.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    public BinaryType type(int typeId) throws BinaryObjectException;

    /**
     * Gets metadata for all known types.
     *
     * @return Metadata.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    public Collection<BinaryType> types() throws BinaryObjectException;

    /**
     * Create enum object using value.
     *
     * @param typeName Type name.
     * @param ord Ordinal.
     * @return Enum object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    public BinaryObject buildEnum(String typeName, int ord) throws BinaryObjectException;

    /**
     * Create enum object using name.
     *
     * @param typeName Type name.
     * @param name Name.
     * @return Enum object.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    public BinaryObject buildEnum(String typeName, String name) throws BinaryObjectException;

    /**
     * Register enum type.
     *
     * @param typeName Type name.
     * @param vals Mapping of enum constant names to ordinals.
     * @return Binary type for registered enum.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    public BinaryType registerEnum(String typeName, Map<String, Integer> vals) throws BinaryObjectException;
}
