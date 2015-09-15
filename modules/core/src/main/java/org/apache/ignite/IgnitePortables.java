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
import java.util.TreeMap;
import java.util.UUID;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.portable.PortableBuilder;
import org.apache.ignite.portable.PortableException;
import org.apache.ignite.portable.PortableIdMapper;
import org.apache.ignite.portable.PortableMarshalAware;
import org.apache.ignite.portable.PortableMetadata;
import org.apache.ignite.portable.PortableObject;
import org.apache.ignite.portable.PortableSerializer;
import org.apache.ignite.portable.PortableTypeConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Defines portable objects functionality. With portable objects you are able to:
 * <ul>
 * <li>Seamlessly interoperate between Java, .NET, and C++.</li>
 * <li>Make any object portable with zero code change to your existing code.</li>
 * <li>Nest portable objects within each other.</li>
 * <li>Automatically handle {@code circular} or {@code null} references.</li>
 * <li>Automatically convert collections and maps between Java, .NET, and C++.</li>
 * <li>
 *      Optionally avoid deserialization of objects on the server side
 *      (objects are stored in {@link PortableObject} format).
 * </li>
 * <li>Avoid need to have concrete class definitions on the server side.</li>
 * <li>Dynamically change structure of the classes without having to restart the cluster.</li>
 * <li>Index into portable objects for querying purposes.</li>
 * </ul>
 * <h1 class="header">Working With Portables Directly</h1>
 * Once an object is defined as portable,
 * Ignite will always store it in memory in the portable (i.e. binary) format.
 * User can choose to work either with the portable format or with the deserialized form
 * (assuming that class definitions are present in the classpath).
 * <p>
 * To work with the portable format directly, user should create a special cache projection
 * using {@link IgniteCache#withKeepPortable()} method and then retrieve individual fields as needed:
 * <pre name=code class=java>
 * IgniteCache&lt;PortableObject, PortableObject&gt; prj = cache.withKeepPortable();
 *
 * // Convert instance of MyKey to portable format.
 * // We could also use PortableBuilder to create the key in portable format directly.
 * PortableObject key = grid.portables().toPortable(new MyKey());
 *
 * PortableObject val = prj.get(key);
 *
 * String field = val.field("myFieldName");
 * </pre>
 * Alternatively, if we have class definitions in the classpath, we may choose to work with deserialized
 * typed objects at all times. In this case we do incur the deserialization cost. However, if
 * {@link PortableMarshaller#isKeepDeserialized()} is {@code true} then Ignite will only deserialize on the first access
 * and will cache the deserialized object, so it does not have to be deserialized again:
 * <pre name=code class=java>
 * IgniteCache&lt;MyKey.class, MyValue.class&gt; cache = grid.cache(null);
 *
 * MyValue val = cache.get(new MyKey());
 *
 * // Normal java getter.
 * String fieldVal = val.getMyFieldName();
 * </pre>
 * If we used, for example, one of the automatically handled portable types for a key, like integer,
 * and still wanted to work with binary portable format for values, then we would declare cache projection
 * as follows:
 * <pre name=code class=java>
 * IgniteCache&lt;Integer.class, PortableObject&gt; prj = cache.withKeepPortable();
 * </pre>
 * <h1 class="header">Automatic Portable Types</h1>
 * Note that only portable classes are converted to {@link PortableObject} format. Following
 * classes are never converted (e.g., {@link #toPortable(Object)} method will return original
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
 *         them will still be converted if they are portable)
 *     </li>
 * </ul>
 * <h1 class="header">Working With Maps and Collections</h1>
 * All maps and collections in the portable objects are serialized automatically. When working
 * with different platforms, e.g. C++ or .NET, Ignite will automatically pick the most
 * adequate collection or map in either language. For example, {@link ArrayList} in Java will become
 * {@code List} in C#, {@link LinkedList} in Java is {@link LinkedList} in C#, {@link HashMap}
 * in Java is {@code Dictionary} in C#, and {@link TreeMap} in Java becomes {@code SortedDictionary}
 * in C#, etc.
 * <h1 class="header">Building Portable Objects</h1>
 * Ignite comes with {@link PortableBuilder} which allows to build portable objects dynamically:
 * <pre name=code class=java>
 * PortableBuilder builder = Ignition.ignite().portables().builder();
 *
 * builder.typeId("MyObject");
 *
 * builder.stringField("fieldA", "A");
 * build.intField("fieldB", "B");
 *
 * PortableObject portableObj = builder.build();
 * </pre>
 * For the cases when class definition is present
 * in the class path, it is also possible to populate a standard POJO and then
 * convert it to portable format, like so:
 * <pre name=code class=java>
 * MyObject obj = new MyObject();
 *
 * obj.setFieldA("A");
 * obj.setFieldB(123);
 *
 * PortableObject portableObj = Ignition.ignite().portables().toPortable(obj);
 * </pre>
 * NOTE: you don't need to convert typed objects to portable format before storing
 * them in cache, Ignite will do that automatically.
 * <h1 class="header">Portable Metadata</h1>
 * Even though Ignite portable protocol only works with hash codes for type and field names
 * to achieve better performance, Ignite provides metadata for all portable types which
 * can be queried ar runtime via any of the {@link IgnitePortables#metadata(Class)}
 * methods. Having metadata also allows for proper formatting of {@code PortableObject#toString()} method,
 * even when portable objects are kept in binary format only, which may be necessary for audit reasons.
 * <h1 class="header">Dynamic Structure Changes</h1>
 * Since objects are always cached in the portable binary format, server does not need to
 * be aware of the class definitions. Moreover, if class definitions are not present or not
 * used on the server, then clients can continuously change the structure of the portable
 * objects without having to restart the cluster. For example, if one client stores a
 * certain class with fields A and B, and another client stores the same class with
 * fields B and C, then the server-side portable object will have the fields A, B, and C.
 * As the structure of a portable object changes, the new fields become available for SQL queries
 * automatically.
 * <h1 class="header">Configuration</h1>
 * By default all your objects are considered as portables and no specific configuration is needed.
 * However, in some cases, like when an object is used by both Java and .Net, you may need to specify portable objects
 * explicitly by calling {@link PortableMarshaller#setClassNames(Collection)}.
 * The only requirement Ignite imposes is that your object has an empty
 * constructor. Note, that since server side does not have to know the class definition,
 * you only need to list portable objects in configuration on the client side. However, if you
 * list them on the server side as well, then you get the ability to deserialize portable objects
 * into concrete types on the server as well as on the client.
 * <p>
 * Here is an example of portable configuration (note that star (*) notation is supported):
 * <pre name=code class=xml>
 * ...
 * &lt;!-- Explicit portable objects configuration. --&gt;
 * &lt;property name="marshaller"&gt;
 *     &lt;bean class="org.apache.ignite.marshaller.portable.PortableMarshaller"&gt;
 *         &lt;property name="classNames"&gt;
 *             &lt;list&gt;
 *                 &lt;value&gt;my.package.for.portable.objects.*&lt;/value&gt;
 *                 &lt;value&gt;org.apache.ignite.examples.client.portable.Employee&lt;/value&gt;
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
 * PortableMarshaller marsh = new PortableMarshaller();
 *
 * marsh.setClassNames(Arrays.asList(
 *     Employee.class.getName(),
 *     Address.class.getName())
 * );
 *
 * cfg.setMarshaller(marsh);
 * </pre>
 * You can also specify class name for a portable object via {@link PortableTypeConfiguration}.
 * Do it in case if you need to override other configuration properties on per-type level, like
 * ID-mapper, or serializer.
 * <h1 class="header">Custom Affinity Keys</h1>
 * Often you need to specify an alternate key (not the cache key) for affinity routing whenever
 * storing objects in cache. For example, if you are caching {@code Employee} object with
 * {@code Organization}, and want to colocate employees with organization they work for,
 * so you can process them together, you need to specify an alternate affinity key.
 * With portable objects you would have to do it as following:
 * <pre name=code class=xml>
 * &lt;property name="marshaller"&gt;
 *     &lt;bean class="org.gridgain.grid.marshaller.portable.PortableMarshaller"&gt;
 *         ...
 *         &lt;property name="typeConfigurations"&gt;
 *             &lt;list&gt;
 *                 &lt;bean class="org.apache.ignite.portable.PortableTypeConfiguration"&gt;
 *                     &lt;property name="className" value="org.apache.ignite.examples.client.portable.EmployeeKey"/&gt;
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
 * serialization logic by optionally implementing {@link PortableMarshalAware} interface, like so:
 * <pre name=code class=java>
 * public class Address implements PortableMarshalAware {
 *     private String street;
 *     private int zip;
 *
 *     // Empty constructor required for portable deserialization.
 *     public Address() {}
 *
 *     &#64;Override public void writePortable(PortableWriter writer) throws PortableException {
 *         writer.writeString("street", street);
 *         writer.writeInt("zip", zip);
 *     }
 *
 *     &#64;Override public void readPortable(PortableReader reader) throws PortableException {
 *         street = reader.readString("street");
 *         zip = reader.readInt("zip");
 *     }
 * }
 * </pre>
 * Alternatively, if you cannot change class definitions, you can provide custom serialization
 * logic in {@link PortableSerializer} either globally in {@link PortableMarshaller} or
 * for a specific type via {@link PortableTypeConfiguration} instance.
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
 * you can provide your own {@link PortableIdMapper} implementation.
 * <p>
 * ID-mapper may be provided either globally in {@link PortableMarshaller},
 * or for a specific type via {@link PortableTypeConfiguration} instance.
 * <h1 class="header">Query Indexing</h1>
 * Portable objects can be indexed for querying by specifying index fields in
 * {@link org.apache.ignite.cache.CacheTypeMetadata} inside of specific
 * {@link org.apache.ignite.configuration.CacheConfiguration} instance,
 * like so:
 * <pre name=code class=xml>
 * ...
 * &lt;bean class="org.apache.ignite.cache.CacheConfiguration"&gt;
 *     ...
 *     &lt;property name="typeMetadata"&gt;
 *         &lt;list&gt;
 *             &lt;bean class="CacheTypeMetadata"&gt;
 *                 &lt;property name="type" value="Employee"/&gt;
 *
 *                 &lt;!-- Fields to index in ascending order. --&gt;
 *                 &lt;property name="ascendingFields"&gt;
 *                     &lt;map&gt;
 *                     &lt;entry key="name" value="java.lang.String"/&gt;
 *
 *                         &lt;!-- Nested portable objects can also be indexed. --&gt;
 *                         &lt;entry key="address.zip" value="java.lang.Integer"/&gt;
 *                     &lt;/map&gt;
 *                 &lt;/property&gt;
 *             &lt;/bean&gt;
 *         &lt;/list&gt;
 *     &lt;/property&gt;
 * &lt;/bean&gt;
 * </pre>
 */
public interface IgnitePortables {
    /**
     * Gets type ID for given type name.
     *
     * @param typeName Type name.
     * @return Type ID.
     */
    public int typeId(String typeName);

    /**
     * Converts provided object to instance of {@link PortableObject}.
     *
     * @param obj Object to convert.
     * @return Converted object.
     * @throws PortableException In case of error.
     */
    public <T> T toPortable(@Nullable Object obj) throws PortableException;

    /**
     * Creates new portable builder.
     *
     * @param typeId ID of the type.
     * @return Newly portable builder.
     */
    public PortableBuilder builder(int typeId);

    /**
     * Creates new portable builder.
     *
     * @param typeName Type name.
     * @return Newly portable builder.
     */
    public PortableBuilder builder(String typeName);

    /**
     * Creates portable builder initialized by existing portable object.
     *
     * @param portableObj Portable object to initialize builder.
     * @return Portable builder.
     */
    public PortableBuilder builder(PortableObject portableObj);

    /**
     * Gets metadata for provided class.
     *
     * @param cls Class.
     * @return Metadata.
     * @throws PortableException In case of error.
     */
    @Nullable public PortableMetadata metadata(Class<?> cls) throws PortableException;

    /**
     * Gets metadata for provided class name.
     *
     * @param typeName Type name.
     * @return Metadata.
     * @throws PortableException In case of error.
     */
    @Nullable public PortableMetadata metadata(String typeName) throws PortableException;

    /**
     * Gets metadata for provided type ID.
     *
     * @param typeId Type ID.
     * @return Metadata.
     * @throws PortableException In case of error.
     */
    @Nullable public PortableMetadata metadata(int typeId) throws PortableException;

    /**
     * Gets metadata for all known types.
     *
     * @return Metadata.
     * @throws PortableException In case of error.
     */
    public Collection<PortableMetadata> metadata() throws PortableException;
}
