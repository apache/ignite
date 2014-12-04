/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.spring;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.resource.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;

import java.net.*;
import java.util.*;

/**
 * Spring processor which can parse Spring configuration files, interface was introduced to avoid mandatory
 * runtime dependency on Spring framework.
 */
public interface GridSpringProcessor {
    /**
     * Loads all grid configurations specified within given configuration file.
     * <p>
     * Usually Spring XML configuration file will contain only one Grid definition. Note that
     * Grid configuration bean(s) is retrieved form configuration file by type, so the name of
     * the Grid configuration bean is ignored.
     *
     * @param cfgUrl Configuration file path or URL. This cannot be {@code null}.
     * @param excludedProps Properties to exclude.
     * @return Tuple containing all loaded configurations and Spring context used to load them.
     * @throws GridException If grid could not be started or configuration
     *      read. This exception will be thrown also if grid with given name has already
     *      been started or Spring XML configuration file is invalid.
     */
    public IgniteBiTuple<Collection<IgniteConfiguration>, ? extends GridSpringResourceContext> loadConfigurations(
        URL cfgUrl, String... excludedProps) throws GridException;

    /**
     * Loads bean instances that match the given types from given configuration file.
     *
     * @param cfgUrl Configuration file path or URL. This cannot be {@code null}.
     * @param beanClasses Beans classes.
     * @return Bean class -> loaded bean instance map, if configuration does not contain bean with required type the
     *       map value is {@code null}.
     * @throws GridException If failed to load configuration.
     */
    public Map<Class<?>, Object> loadBeans(URL cfgUrl, Class<?>... beanClasses) throws GridException;

    /**
     * Gets user version for given class loader by checking
     * {@code META-INF/gridgain.xml} file for {@code userVersion} attribute. If
     * {@code gridgain.xml} file is not found, or user version is not specified there,
     * then default version (empty string) is returned.
     *
     * @param ldr Class loader.
     * @param log Logger.
     * @return User version for given class loader or empty string if no version
     *      was explicitly specified.
     */
    public String userVersion(ClassLoader ldr, GridLogger log);
}
