// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.marshaller.xstream;

import com.thoughtworks.xstream.*;
import org.gridgain.grid.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Implementation of {@link GridMarshaller} based on <a href="http://xstream.codehaus.org/">XStream</a>.
 * This marshaller does not require objects to implement {@link Serializable}.
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This marshaller has no mandatory configuration parameters.
 * <h2 class="header">Java Example</h2>
 * {@code GridXstreamMarshaller} has to be explicitly configured to override default {@link GridOptimizedMarshaller}.
 * <pre name="code" class="java">
 * GridXstreamMarshaller marshaller = new GridXstreamMarshaller();
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
 * GridXstreamMarshaller can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.gridgain.grid.GridConfiguration" singleton="true"&gt;
 *     ...
 *     &lt;property name="marshaller"&gt;
 *         &lt;bean class="org.gridgain.grid.marshaller.xstream.GridXstreamMarshaller"/&gt;
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
 *
 * @author @java.author
 * @version @java.version
 */
public class GridXstreamMarshaller extends GridAbstractMarshaller {
    /** XStream instance to use with system class loader. */
    @GridToStringExclude
    private final XStream dfltXs;

    /**
     * Initializes {@code XStream} marshaller.
     */
    public GridXstreamMarshaller() {
        dfltXs = createXstream(getClass().getClassLoader());
    }

    /**
     * @param ldr Class loader for created XStream object.
     * @return created Xstream object.
     */
    private XStream createXstream(ClassLoader ldr) {
        XStream xs = new XStream();

        xs.registerConverter(new GridXstreamMarshallerExternalizableConverter(xs.getMapper()));
        xs.registerConverter(new GridXstreamMarshallerResourceConverter());
        xs.registerConverter(new GridXstreamMarshallerObjectConverter());

        xs.setClassLoader(ldr);

        return xs;
    }

    /** {@inheritDoc} */
    @Override public void marshal(@Nullable Object obj, OutputStream out) throws GridException {
        assert out != null;

        dfltXs.toXML(obj, out);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <T> T unmarshal(InputStream in, @Nullable ClassLoader clsLdr) throws GridException {
        assert in != null;

        if (clsLdr == null)
            clsLdr = getClass().getClassLoader();

        if (getClass().getClassLoader().equals(clsLdr)) {
            return (T)dfltXs.fromXML(in);
        }

        return (T)createXstream(clsLdr).fromXML(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridXstreamMarshaller.class, this);
    }
}
