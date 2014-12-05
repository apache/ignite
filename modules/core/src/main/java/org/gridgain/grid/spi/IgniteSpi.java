/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi;

import org.jetbrains.annotations.*;
import java.util.*;

/**
 * This interface defines life-cycle of SPI implementation. Every SPI implementation should implement
 * this interface. Kernal will not load SPI that doesn't implement this interface.
 * <p>
 * Grid SPI's can be injected using IoC (dependency injection)
 * with grid resources. Both, field and method based injection are supported.
 * The following grid resources can be injected:
 * <ul>
 * <li>{@link org.apache.ignite.resources.IgniteLoggerResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteLocalNodeIdResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteHomeResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteMBeanServerResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteExecutorServiceResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteMarshallerResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteSpringApplicationContextResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteSpringResource}</li>
 * <li>{@link org.apache.ignite.resources.IgniteAddressResolverResource}</li>
 * </ul>
 * Refer to corresponding resource documentation for more information.
 */
public interface IgniteSpi {
    /**
     * Gets SPI name.
     *
     * @return SPI name.
     */
    public String getName();

    /**
     * This method is called before SPI starts (before method {@link #spiStart(String)}
     * is called). It allows SPI implementation to add attributes to a local
     * node. Kernal collects these attributes from all SPI implementations
     * loaded up and then passes it to discovery SPI so that they can be
     * exchanged with other nodes.
     *
     * @return Map of local node attributes this SPI wants to add.
     * @throws GridSpiException Throws in case of any error.
     */
    public Map<String, Object> getNodeAttributes() throws GridSpiException;

    /**
     * This method is called to start SPI. After this method returns
     * successfully kernel assumes that SPI is fully operational.
     *
     * @param gridName Name of grid instance this SPI is being started for
     *    ({@code null} for default grid).
     * @throws GridSpiException Throws in case of any error during SPI start.
     */
    public void spiStart(@Nullable String gridName) throws GridSpiException;

    /**
     * Callback invoked when SPI context is initialized. SPI implementation
     * may store SPI context for future access.
     * <p>
     * This method is invoked after {@link #spiStart(String)} method is
     * completed, so SPI should be fully functional at this point. Use this
     * method for post-start initialization, such as subscribing a discovery
     * listener, sending a message to remote node, etc...
     *
     * @param spiCtx Spi context.
     * @throws GridSpiException If context initialization failed (grid will be stopped).
     */
    public void onContextInitialized(GridSpiContext spiCtx) throws GridSpiException;

    /**
     * Callback invoked prior to stopping grid before SPI context is destroyed.
     * Once this method is complete, grid will begin shutdown sequence. Use this
     * callback for de-initialization logic that may involve SPI context. Note that
     * invoking SPI context after this callback is complete is considered
     * illegal and may produce unknown results.
     * <p>
     * If {@link GridSpiAdapter} is used for SPI implementation, then it will
     * replace actual context with dummy no-op context which is usually good-enough
     * since grid is about to shut down.
     */
    public void onContextDestroyed();

    /**
     * This method is called to stop SPI. After this method returns kernel
     * assumes that this SPI is finished and all resources acquired by it
     * are released.
     * <p>
     * <b>
     * Note that this method can be called at any point including during
     * recovery of failed start. It should make no assumptions on what state SPI
     * will be in when this method is called.
     * </b>
     *
     * @throws GridSpiException Thrown in case of any error during SPI stop.
     */
    public void spiStop() throws GridSpiException;
}
