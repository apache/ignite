/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.marshaller.jdk;

import org.apache.ignite.marshaller.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Implementation of {@link GridMarshaller} based on JDK serialization mechanism.
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This marshaller has no mandatory configuration parameters.
 * <h2 class="header">Java Example</h2>
 * {@code GridJdkMarshaller} needs to be explicitly configured to override default {@link org.apache.ignite.marshaller.optimized.GridOptimizedMarshaller}.
 * <pre name="code" class="java">
 * GridJdkMarshaller marshaller = new GridJdkMarshaller();
 *
 * GridConfiguration cfg = new GridConfiguration();
 *
 * // Override default marshaller.
 * cfg.setMarshaller(marshaller);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * GridJdkMarshaller can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.gridgain.grid.GridConfiguration" singleton="true"&gt;
 *     ...
 *     &lt;property name="marshaller"&gt;
 *         &lt;bean class="org.gridgain.grid.marshaller.jdk.GridJdkMarshaller"/&gt;
 *     &lt;/property&gt;
 *     ...
 * &lt;/bean&gt;
 * </pre>
 *  <p>
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
 * <br>
 *
 */
public class IgniteJdkMarshaller extends GridAbstractMarshaller {
    /** {@inheritDoc} */
    @Override public void marshal(@Nullable Object obj, OutputStream out) throws GridException {
        assert out != null;

        ObjectOutputStream objOut = null;

        try {
            objOut = new IgniteJdkMarshallerObjectOutputStream(new IgniteJdkMarshallerOutputStreamWrapper(out));

            // Make sure that we serialize only task, without class loader.
            objOut.writeObject(obj);

            objOut.flush();
        }
        catch (IOException e) {
            throw new GridException("Failed to serialize object: " + obj, e);
        }
        finally{
            U.closeQuiet(objOut);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <T> T unmarshal(InputStream in, @Nullable ClassLoader clsLdr) throws GridException {
        assert in != null;

        if (clsLdr == null)
            clsLdr = getClass().getClassLoader();

        ObjectInputStream objIn = null;

        try {
            objIn = new IgniteJdkMarshallerObjectInputStream(new IgniteJdkMarshallerInputStreamWrapper(in), clsLdr);

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
        finally{
            U.closeQuiet(objIn);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteJdkMarshaller.class, this);
    }
}
