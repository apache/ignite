/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.marshaller.optimized;

import org.apache.ignite.marshaller.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;
import sun.misc.*;

import java.io.*;
import java.net.*;
import java.util.*;

import static org.gridgain.grid.marshaller.optimized.GridOptimizedMarshallerUtils.*;

/**
 * Optimized implementation of {@link GridMarshaller}. Unlike {@link org.apache.ignite.marshaller.jdk.GridJdkMarshaller},
 * which is based on standard {@link ObjectOutputStream}, this marshaller does not
 * enforce that all serialized objects implement {@link Serializable} interface. It is also
 * about 20 times faster as it removes lots of serialization overhead that exists in
 * default JDK implementation.
 * <p>
 * {@code GridOptimizedMarshaller} is tested only on Java HotSpot VM on other VMs
 * it could yield unexpected results. It is the default marshaller on Java HotSpot VMs
 * and will be used if no other marshaller was explicitly configured.
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This marshaller has no mandatory configuration parameters.
 * <h2 class="header">Java Example</h2>
 * <pre name="code" class="java">
 * GridOptimizedMarshaller marshaller = new GridOptimizedMarshaller();
 *
 * // Enforce Serializable interface.
 * marshaller.setRequireSerializable(true);
 *
 * GridConfiguration cfg = new GridConfiguration();
 *
 * // Override marshaller.
 * cfg.setMarshaller(marshaller);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * GridOptimizedMarshaller can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.gridgain.grid.GridConfiguration" singleton="true"&gt;
 *     ...
 *     &lt;property name="marshaller"&gt;
 *         &lt;bean class="org.gridgain.grid.marshaller.optimized.GridOptimizedMarshaller"&gt;
 *             &lt;property name="requireSerializable"&gt;true&lt;/property&gt;
 *         &lt;/bean&gt;
 *     &lt;/property&gt;
 *     ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * <h2 class="header">Injection Example</h2>
 * GridMarshaller can be injected in users task, job or SPI as following:
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *     ...
 *     &#64;GridMarshallerResource
 *     private GridMarshaller marshaller;
 *     ...
 * }
 * </pre>
 * or
 * <pre name="code" class="java">
 * public class MyGridJob implements GridComputeJob {
 *     ...
 *     private GridMarshaller marshaller;
 *     ...
 *     &#64;GridMarshallerResource
 *     public void setMarshaller(GridMarshaller marshaller) {
 *         this.marshaller = marshaller;
 *     }
 *     ...
 * }
 * </pre>
 */
public class GridOptimizedMarshaller extends GridAbstractMarshaller {
    /** Whether or not to require an object to be serializable in order to be marshalled. */
    private boolean requireSer = true;

    /** Default class loader. */
    private final ClassLoader dfltClsLdr = getClass().getClassLoader();

    /**
     * Initializes marshaller not to enforce {@link Serializable} interface.
     *
     * @throws GridRuntimeException If this marshaller is not supported on the current JVM.
     */
    public GridOptimizedMarshaller() {
        if (!available())
            throw new GridRuntimeException("Using GridOptimizedMarshaller on unsupported JVM version (some of " +
                "JVM-private APIs required for the marshaller to work are missing).");
    }

    /**
     * Initializes marshaller with given serialization flag. If {@code true},
     * then objects will be required to implement {@link Serializable} in order
     * to be serialize.
     *
     * @param requireSer Flag to enforce {@link Serializable} interface or not. If {@code true},
     *      then objects will be required to implement {@link Serializable} in order to be
     *      marshalled, if {@code false}, then such requirement will be relaxed.
     * @throws GridRuntimeException If this marshaller is not supported on the current JVM.
     */
    public GridOptimizedMarshaller(boolean requireSer) {
        this();

        this.requireSer = requireSer;
    }

    /**
     * Initializes marshaller with given serialization flag. If {@code true},
     * then objects will be required to implement {@link Serializable} in order
     * to be serialize.
     *
     * @param requireSer Flag to enforce {@link Serializable} interface or not. If {@code true},
     *      then objects will be required to implement {@link Serializable} in order to be
     *      marshalled, if {@code false}, then such requirement will be relaxed.
     * @param clsNames User preregistered class names.
     * @param clsNamesPath Path to a file with user preregistered class names.
     * @param poolSize Object streams pool size.
     * @throws GridException If an I/O error occurs while writing stream header.
     * @throws GridRuntimeException If this marshaller is not supported on the current JVM.
     */
    public GridOptimizedMarshaller(boolean requireSer, @Nullable List<String> clsNames,
        @Nullable String clsNamesPath, int poolSize) throws GridException {
        this(requireSer);

        setClassNames(clsNames);
        setClassNamesPath(clsNamesPath);
        setPoolSize(poolSize);
    }

    /**
     * Adds provided class names for marshalling optimization.
     * <p>
     * <b>NOTE</b>: these collections of classes must be identical on all nodes and in the same order.
     *
     * @param clsNames User preregistered class names to add.
     */
    @SuppressWarnings("unchecked")
    public void setClassNames(@Nullable List<String> clsNames) {
        if (clsNames != null && !clsNames.isEmpty()) {
            String[] clsNamesArr = clsNames.toArray(new String[clsNames.size()]);

            Arrays.sort(clsNamesArr);

            Map<String, Integer> name2id = U.newHashMap(clsNamesArr.length);
            T3<String, Class<?>, GridOptimizedClassDescriptor>[] id2name = new T3[clsNamesArr.length];

            int i = 0;

            for (String name : clsNamesArr) {
                name2id.put(name, i);
                id2name[i++] = new T3<>(name, null, null);
            }

            GridOptimizedClassResolver.userClasses(name2id, id2name);
        }
    }

    /**
     * Specifies a name of the file which lists all class names to be optimized.
     * The file path can either be absolute path, relative to {@code GRIDGAIN_HOME},
     * or specify a resource file on the class path.
     * <p>
     * The format of the file is class name per line, like this:
     * <pre>
     * ...
     * com.example.Class1
     * com.example.Class2
     * ...
     * </pre>
     * <p>
     * <b>NOTE</b>: this class list must be identical on all nodes and in the same order.
     *
     * @param path Path to a file with user preregistered class names.
     * @throws GridException If an error occurs while writing stream header.
     */
    public void setClassNamesPath(@Nullable String path) throws GridException {
        if (path == null)
            return;

        URL url = GridUtils.resolveGridGainUrl(path, false);

        if (url == null)
            throw new GridException("Failed to find resource for name: " + path);

        List<String> clsNames;

        try {
            clsNames = new LinkedList<>();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), UTF_8))) {
                String clsName;

                while ((clsName = reader.readLine()) != null)
                    clsNames.add(clsName);
            }
        }
        catch (IOException e) {
            throw new GridException("Failed to read class names from path: " + path, e);
        }

        setClassNames(clsNames);
    }

    /**
     * Specifies size of cached object streams used by marshaller. Object streams are cached for
     * performance reason to avoid costly recreation for every serialization routine. If {@code 0} (default),
     * pool is not used and each thread has its own cached object stream which it keeps reusing.
     * <p>
     * Since each stream has an internal buffer, creating a stream for each thread can lead to
     * high memory consumption if many large messages are marshalled or unmarshalled concurrently.
     * Consider using pool in this case. This will limit number of streams that can be created and,
     * therefore, decrease memory consumption.
     * <p>
     * NOTE: Using streams pool can decrease performance since streams will be shared between
     * different threads which will lead to more frequent context switching.
     *
     * @param poolSize Streams pool size. If {@code 0}, pool is not used.
     */
    public void setPoolSize(int poolSize) {
        GridOptimizedObjectStreamRegistry.poolSize(poolSize);
    }

    /**
     * @return Whether to enforce {@link Serializable} interface.
     */
    public boolean isRequireSerializable() {
        return requireSer;
    }

    /**
     * Sets flag to enforce {@link Serializable} interface or not.
     *
     * @param requireSer Flag to enforce {@link Serializable} interface or not. If {@code true},
     *      then objects will be required to implement {@link Serializable} in order to be
     *      marshalled, if {@code false}, then such requirement will be relaxed.
     */
    public void setRequireSerializable(boolean requireSer) {
        this.requireSer = requireSer;
    }

    /** {@inheritDoc} */
    @Override public void marshal(@Nullable Object obj, OutputStream out) throws GridException {
        assert out != null;

        GridOptimizedObjectOutputStream objOut = null;

        try {
            objOut = GridOptimizedObjectStreamRegistry.out();

            objOut.requireSerializable(requireSer);

            objOut.out().outputStream(out);

            objOut.writeObject(obj);
        }
        catch (IOException e) {
            throw new GridException("Failed to serialize object: " + obj, e);
        }
        finally {
            GridOptimizedObjectStreamRegistry.closeOut(objOut);
        }
    }

    /** {@inheritDoc} */
    @Override public byte[] marshal(@Nullable Object obj) throws GridException {
        GridOptimizedObjectOutputStream objOut = null;

        try {
            objOut = GridOptimizedObjectStreamRegistry.out();

            objOut.requireSerializable(requireSer);

            objOut.writeObject(obj);

            return objOut.out().array();
        }
        catch (IOException e) {
            throw new GridException("Failed to serialize object: " + obj, e);
        }
        finally {
            GridOptimizedObjectStreamRegistry.closeOut(objOut);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(InputStream in, @Nullable ClassLoader clsLdr) throws GridException {
        assert in != null;

        GridOptimizedObjectInputStream objIn = null;

        try {
            objIn = GridOptimizedObjectStreamRegistry.in();

            objIn.classLoader(clsLdr != null ? clsLdr : dfltClsLdr);

            objIn.in().inputStream(in);

            return (T)objIn.readObject();
        }
        catch (IOException e) {
            throw new GridException("Failed to deserialize object with given class loader: " + clsLdr, e);
        }
        catch (ClassNotFoundException e) {
            throw new GridException("Failed to find class with given class loader for unmarshalling " +
                "(make sure same versions of all classes are available on all nodes or enable peer-class-loading): " +
                clsLdr, e);
        }
        finally {
            GridOptimizedObjectStreamRegistry.closeIn(objIn);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T unmarshal(byte[] arr, @Nullable ClassLoader clsLdr) throws GridException {
        assert arr != null;

        GridOptimizedObjectInputStream objIn = null;

        try {
            objIn = GridOptimizedObjectStreamRegistry.in();

            objIn.classLoader(clsLdr != null ? clsLdr : dfltClsLdr);

            objIn.in().bytes(arr, arr.length);

            return (T)objIn.readObject();
        }
        catch (IOException e) {
            throw new GridException("Failed to deserialize object with given class loader: " + clsLdr, e);
        }
        catch (ClassNotFoundException e) {
            throw new GridException("Failed to find class with given class loader for unmarshalling " +
                "(make sure same version of all classes are available on all nodes or enable peer-class-loading): " +
                clsLdr, e);
        }
        finally {
            GridOptimizedObjectStreamRegistry.closeIn(objIn);
        }
    }

    /**
     * Checks whether {@code GridOptimizedMarshaller} is able to work on the current JVM.
     * <p>
     * As long as {@code GridOptimizedMarshaller} uses JVM-private API, which is not guaranteed
     * to be available on all JVM, this method should be called to ensure marshaller could work properly.
     * <p>
     * Result of this method is automatically checked in constructor.
     *
     * @return {@code true} if {@code GridOptimizedMarshaller} can work on the current JVM or
     *  {@code false} if it can't.
     */
    @SuppressWarnings({"TypeParameterExtendsFinalClass", "ErrorNotRethrown"})
    public static boolean available() {
        try {
            Unsafe unsafe = GridUnsafe.unsafe();

            Class<? extends Unsafe> unsafeCls = unsafe.getClass();

            unsafeCls.getMethod("allocateInstance", Class.class);
            unsafeCls.getMethod("copyMemory", Object.class, long.class, Object.class, long.class, long.class);

            return true;
        }
        catch (Exception ignored) {
            return false;
        }
        catch (NoClassDefFoundError ignored) {
            return false;
        }
    }

    /**
     * Undeployment callback invoked when class loader is being undeployed.
     *
     * @param ldr Class loader being undeployed.
     */
    public static void onUndeploy(ClassLoader ldr) {
        GridOptimizedMarshallerUtils.onUndeploy(ldr);
    }

    /**
     * Clears internal caches and frees memory. Usually called on system stop.
     */
    public static void clearCache() {
        GridOptimizedMarshallerUtils.clearCache();
    }
}
