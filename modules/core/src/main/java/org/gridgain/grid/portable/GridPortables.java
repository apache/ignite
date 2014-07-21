/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.portable;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.portable.*;
import org.jetbrains.annotations.*;

/**
 * Defines portable objects functionality. With portable objects you are able to:
 * <ul>
 * <li>Seamlessly interoperate between Java, .NET, and C++.</li>
 * <li>Make any object portable with zero code change to your existing code.</li>
 * <li>Nest portable objects within each other.</li>
 * <li>Automatically handle {@code circular} or {@code null} references.</li>
 * <li>
 *      Optionally avoid deserialization of objects on the server side
 *      (objects are stored in {@link GridPortableObject} format).
 * </li>
 * <li>Avoid need to have concrete class definitions on the server side.</li>
 * <li>Dynamically change structure of the classes without having to restart the cluster.</li>
 * <li>Index into portable objects for querying purposes.</li>
 * </ul>
 * <h1 class="header">Configuration</h1>
 * To make any object portable, you have to specify it in {@link GridPortableConfiguration}
 * at startup. The only requirement GridGain imposes is that your object has an empty
 * constructor. Note, that since server side does not have to know the class definition,
 * you only need to list portable objects in configuration on client side. However, if you
 * list them on server side as well, then you get ability to deserialize portable objects
 * into concrete types on the server as well.
 * <p>
 * Here is an example of portable configuration:
 * <pre name=code class=xml>
 *     ...
 *     &lt;!-- Portable objects configuration. --&gt;
 *     &lt;property name="portableConfiguration"&gt;
 *         &lt;bean class="org.gridgain.portable.GridPortableConfiguration"&gt;
 *             &lt;property name="classNames"&gt;
 *                 &lt;list&gt;
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
     */
    public <T> T toPortable(@Nullable Object obj) throws GridPortableException;

    /**
     * Gets portable builder.
     *
     * @return Portable builder.
     */
    public <T> GridPortableBuilder<T> builder();

    /**
     * Gets meta data for provided class.
     *
     * @param cls Class.
     * @return Meta data.
     * @throws GridPortableException In case of error.
     */
    @Nullable public GridPortableMetaData metaData(Class<?> cls) throws GridPortableException;

    /**
     * Gets meta data for provided class name.
     *
     * @param clsName Class name.
     * @return Meta data.
     * @throws GridPortableException In case of error.
     */
    @Nullable public GridPortableMetaData metaData(String clsName) throws GridPortableException;

    /**
     * Gets meta data for provided type ID.
     *
     * @param typeId Type ID.
     * @return Meta data.
     * @throws GridPortableException In case of error.
     */
    @Nullable public GridPortableMetaData metaData(int typeId) throws GridPortableException;
}
