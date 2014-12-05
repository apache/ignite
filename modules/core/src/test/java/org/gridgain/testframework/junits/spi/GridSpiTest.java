/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework.junits.spi;

import org.apache.ignite.spi.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.*;

import java.lang.annotation.*;

/**
 * Annotates all tests in SPI test framework. Provides implementation class of the SPI and
 * optional dependencies.
 */
@SuppressWarnings({"JavaDoc"})
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface GridSpiTest {
    /**
     * Mandatory implementation class for SPI.
     */
    public Class<? extends IgniteSpi> spi();

    /**
     * Flag indicating whether SPI should be automatically started.
     */
    public boolean trigger() default true;

    /**
     * Flag indicating whether discovery SPI should be automatically started.
     */
    public boolean triggerDiscovery() default false;

    /**
     * Optional discovery SPI property to specify which SPI to use for discovering other nodes.
     * This property is ignored if the spi being tested is an implementation of {@link org.apache.ignite.spi.discovery.DiscoverySpi} or
     * {@link #triggerDiscovery()} is set to {@code false}.
     */
    public Class<? extends DiscoverySpi> discoverySpi() default TcpDiscoverySpi.class;

    /**
     * Optional group this test belongs to.
     */
    public String group() default "";
}
