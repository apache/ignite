/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.config;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.kernal.processors.resource.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.util.*;

/**
 * Configuration processor.
 */
public class GridConfigurationProcessor implements GridProcessor {
    @Override public void addAttributes(Map<String, Object> attrs) throws GridException {

    }

    @Override public void start() throws GridException {

    }

    @Override public void stop(boolean cancel) throws GridException {

    }

    @Override public void onKernalStart() throws GridException {

    }

    @Override public void onKernalStop(boolean cancel) {

    }

    @Nullable @Override public Object collectDiscoveryData(UUID nodeId) {
        return null;
    }

    @Override public void onDiscoveryDataReceived(Object data) {

    }

    @Override public void printMemoryStats() {

    }

    @Nullable @Override public GridNodeValidationResult validateNode(GridNode node) {
        return null;
    }

    /**
     * @return Configuration processor.
     */
    public static GridConfigurationProcessor instance() {
        // FIXME
        return new GridConfigurationProcessor();
    }

    /**
     * Loads all grid configurations specified within given configuration file.
     * <p>
     * Usually Spring XML configuration file will contain only one Grid definition. Note that
     * Grid configuration bean(s) is retrieved form configuration file by type, so the name of
     * the Grid configuration bean is ignored.
     *
     * @param cfgUrl Configuration file path or URL. This cannot be {@code null}.
     * @return Tuple containing all loaded configurations and Spring context used to load them.
     * @throws GridException If grid could not be started or configuration
     *      read. This exception will be thrown also if grid with given name has already
     *      been started or Spring XML configuration file is invalid.
     */
    public GridBiTuple<Collection<GridConfiguration>, ? extends GridResourceContext> loadConfigurations(URL cfgUrl) throws GridException {
        throw new GridException("Default configuration processor is not able to load configuration: " + cfgUrl);
    }
}
