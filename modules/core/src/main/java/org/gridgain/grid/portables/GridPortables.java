/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.portables;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.jetbrains.annotations.*;

import java.util.*;

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
 *      (objects are stored in {@link GridPortableObject} format).
 * </li>
 * <li>Avoid need to have concrete class definitions on the server side.</li>
 * <li>Dynamically change structure of the classes without having to restart the cluster.</li>
 * <li>Index into portable objects for querying purposes.</li>
 * </ul>
 * <h1 class="header">Working With Portables Directly</h1>
 * Once an object is defined as portable,
 * GridGain will always store it in memory in the portable (i.e. binary) format.
 * User can choose to work either with the portable format or with the deserialized form
 * (assuming that class definitions are present in the classpath).
 * <p>
 * To work with the portable format directly, user should create a cache projection
 * over {@link GridPortableObject} class and then retrieve individual fields as needed:
 * <pre name=code class=java>
 * GridCacheProjection&lt;GridPortableObject.class, GridPortableObject.class&gt; prj =
 *     cache.projection(GridPortableObject.class, GridPortableObject.class);
 *
 * // Convert instance of MyKey to portable format.
 * // We could also use GridPortableBuilder to create
 * // the key in portable format directly.
 * GridPortableObject key = grid.portables().toPortable(new MyKey());
 *
 * GridPortableObject val = prj.get(key);
 *
 * String field = val.field("myFieldName");
 * </pre>
 * Alternatively, we could also choose a hybrid approach, where, for example,
 * the keys are concrete deserialized objects and the values are returned in portable
 * format, like so:
 * <pre name=code class=java>
 * GridCacheProjection&lt;MyKey.class, GridPortableObject.class&gt; prj =
 *     cache.projection(MyKey.class, GridPortableObject.class);
 *
 * GridPortableObject val = prj.get(new MyKey());
 *
 * String field = val.field("myFieldName");
 * </pre>
 * We could also have the values as concrete deserialized objects and the keys in portable format,
 * but such use case is a lot less common because cache keys are usually a lot smaller than values, and
 * it may be very cheap to deserialize the keys, but not the values.
 * <p>
 * And finally, if we have class definitions in the classpath, we may choose to work with deserialized
 * typed objects at all times. In this case we do incur the deserialization cost, however,
 * GridGain will only deserialize on the first access and will cache the deserialized object,
 * so it does not have to be deserialized again:
 * <pre name=code class=java>
 * GridCacheProjection&lt;MyKey.class, MyValue.class&gt; prj =
 *     cache.projection(MyKey.class, MyValue.class);
 *
 * MyValue val = prj.get(new MyKey());
 *
 * // Normal java getter.
 * String fieldVal = val.getMyFieldName();
 * </pre>
 * <h1 class="header">Working With Maps and Collections</h1>
 * All maps and collections in the portable objects are serialized automatically. When working
 * with different platforms, e.g. C++ or .NET, GridGain will automatically pick the most
 * adequate collection or map in either language. For example, {@link ArrayList} in Java will become
 * {@code List} in C#, {@link LinkedList} in Java is {@link LinkedList} in C#, {@link HashMap}
 * in Java is {@code Dictionary} in C#, and {@link TreeMap} in Java becomes {@code SortedDictionary}
 * in C#, etc.
 * <h1 class="header">Building Portable Objects</h1>
 * GridGain comes with {@link GridPortableBuilder} which allows to build portable objects dynamically:
 * <pre name=code class=java>
 * GridPortableBuilder builder = GridGain.grid().portables().builder();
 *
 * builder.typeId("MyObject");
 *
 * builder.stringField("fieldA", "A");
 * build.intField("fieldB", "B");
 *
 * GridPortableObject portableObj = builder.build();
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
 * GridPortableObject portableObj = GridGain.grid().portables().toPortable(obj);
 * </pre>
 * NOTE: you don't need to convert typed objects to portable format before storing
 * them in cache, GridGain will do that automatically.
 * <h1 class="header">Portable Metadata</h1>
 * Even though GridGain portable protocol only works with hash codes for type and field names
 * to achieve better performance, GridGain provides metadata for all portable types which
 * can be queried ar runtime via any of the {@link GridPortables#metadata(Class) GridPortables.metadata(...)}
 * methods. Having metadata also allows for proper formatting of {@code GridPortableObject.toString()} method,
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
 * To make any object portable, you have to specify it in {@link GridPortableConfiguration}
 * at startup. The only requirement GridGain imposes is that your object has an empty
 * constructor. Note, that since server side does not have to know the class definition,
 * you only need to list portable objects in configuration on client side. However, if you
 * list them on server side as well, then you get ability to deserialize portable objects
 * into concrete types on the server as well.
 * <p>
 * Here is an example of portable configuration (note that star (*) notation is supported):
 * <pre name=code class=xml>
 *     ...
 *     &lt;!-- Portable objects configuration. --&gt;
 *     &lt;property name="portableConfiguration"&gt;
 *         &lt;bean class="org.gridgain.grid.portables.GridPortableConfiguration"&gt;
 *             &lt;property name="classNames"&gt;
 *                 &lt;list&gt;
 *                     &lt;value&gt;my.package.for.portable.objects.*&lt;/value&gt;
 *                     &lt;value&gt;org.gridgain.examples.client.portable.Employee&lt;/value&gt;
 *                     &lt;value&gt;org.gridgain.examples.client.portable.Address&lt;/value&gt;
 *                 &lt;/list&gt;
 *             &lt;/property&gt;
 *         &lt;/bean&gt;
 *     &lt;/property&gt;
 *     ...
 * </pre>
 * or from code:
 * <pre name=code class=java>
 * GridConfiguration gridCfg = new GridConfiguration();
 *
 * GridPortableConfiguration portCfg = new GridPortableConfiguration();
 *
 * portCfg.setClassNames(Arrays.asList(
 *     Employee.class.getName(),
 *     Address.class.getName())
 * );
 *
 * gridCfg.setPortableConfiguration(portCfg);
 * </pre>
 * You can also specify class name for a portable object via {@link GridPortableTypeConfiguration}.
 * Do it in case if you need to override other configuration properties on per-type level, like
 * ID-mapper, or serializer.
 * <h1 class="header">Serialization</h1>
 * Once portable object is specified in {@link GridPortableConfiguration}, GridGain will
 * be able to serialize and deserialize it. However, you can provide your own custom
 * serialization logic by optionally implementing {@link GridPortableMarshalAware} interface, like so:
 * <pre name=code class=java>
 * public class Address implements GridPortableMarshalAware {
 *     private String street;
 *     private int zip;
 *
 *     // Empty constructor required for portable deserialization.
 *     public Address() {}
 *
 *     &#64;Override public void writePortable(GridPortableWriter writer) throws GridPortableException {
 *         writer.writeString("street", street);
 *         writer.writeInt("zip", zip);
 *     }
 *
 *     &#64;Override public void readPortable(GridPortableReader reader) throws GridPortableException {
 *         street = reader.readString("street");
 *         zip = reader.readInt("zip");
 *     }
 * }
 * </pre>
 * Alternatively, if you cannot change class definitions, you can provide custom serialization
 * logic in {@link GridPortableSerializer} either globally in {@link GridPortableConfiguration} or
 * for a specific type via {@link GridPortableTypeConfiguration} instance.
 * <h1 class="header">Custom ID Mappers</h1>
 * GridGain implementation uses name hash codes to generate IDs for class names or field names
 * internally. However, in cases when you want to provide your own ID mapping schema,
 * you can provide your own {@link GridPortableIdMapper} implementation.
 * <p>
 * ID-mapper may be provided either globally in {@link GridPortableConfiguration},
 * or for a specific type via {@link GridPortableTypeConfiguration} instance.
 * <h1 class="header">Query Indexing</h1>
 * Portable objects can be indexed for querying by specifying index fields in
 * {@link GridCacheQueryTypeMetadata} inside of specific {@link GridCacheConfiguration} instance,
 * like so:
 * <pre name=code class=xml>
 * ...
 * &lt;bean class="org.gridgain.grid.cache.GridCacheConfiguration"&gt;
 *     ...
 *     &lt;property name="queryConfiguration"&gt;
 *         &lt;bean class="org.gridgain.grid.cache.query.GridCacheQueryConfiguration"&gt;
 *             &lt;property name="typeMetadata"&gt;
 *                 &lt;list&gt;
 *                     &lt;bean class="org.gridgain.grid.cache.query.GridCacheQueryTypeMetadata"&gt;
 *                         &lt;property name="type" value="Employee"/&gt;
 *
 *                         &lt;!-- Fields to index in ascending order. --&gt;
 *                         &lt;property name="ascendingFields"&gt;
 *                             &lt;map&gt;
 *                             	&lt;entry key="name" value="java.lang.String"/&gt;
 *
 *                                 &lt;!-- Nested portable objects can also be indexed. --&gt;
 *                                 &lt;entry key="address.zip" value="java.lang.Integer"/&gt;
 *                             &lt;/map&gt;
 *                         &lt;/property&gt;
 *                     &lt;/bean&gt;
 *                 &lt;/list&gt;
 *             &lt;/property&gt;
 *         &lt;/bean&gt;
 *     &lt;/property&gt;
 * &lt;/bean&gt;
 * </pre>
 */
public interface GridPortables {
    /**
     * Gets type ID for given type name.
     *
     * @param typeName Type name.
     * @return Type ID.
     */
    public int typeId(String typeName);

    /**
     * Converts provided object to instance of {@link GridPortableObject}.
     * <p>
     * Note that object's type needs to be configured in {@link GridPortableConfiguration}.
     *
     * @param obj Object to convert.
     * @return Converted object.
     * @throws GridPortableException In case of error.
     */
    public <T> T toPortable(@Nullable Object obj) throws GridPortableException;

    /**
     * Gets portable builder.
     *
     * @return Portable builder.
     */
    public GridPortableBuilder builder();

    /**
     * Gets meta data for provided class.
     *
     * @param cls Class.
     * @return Meta data.
     * @throws GridPortableException In case of error.
     */
    @Nullable public GridPortableMetadata metadata(Class<?> cls) throws GridPortableException;

    /**
     * Gets meta data for provided class name.
     *
     * @param typeName Type name.
     * @return Meta data.
     * @throws GridPortableException In case of error.
     */
    @Nullable public GridPortableMetadata metadata(String typeName) throws GridPortableException;

    /**
     * Gets meta data for provided type ID.
     *
     * @param typeId Type ID.
     * @return Meta data.
     * @throws GridPortableException In case of error.
     */
    @Nullable public GridPortableMetadata metadata(int typeId) throws GridPortableException;
}
