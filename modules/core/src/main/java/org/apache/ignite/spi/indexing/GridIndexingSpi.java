/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.indexing;

import org.apache.ignite.spi.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Indexing SPI allows user to index cache content. Using indexing SPI user can index data in cache and run
 * Usually cache name will be used as space name, so multiple caches can write to single indexing SPI instance.
 * <p>
 * <b>NOTE:</b> this SPI (i.e. methods in this interface) should never be used directly. SPIs provide
 * internal view on the subsystem and is used internally by GridGain kernal. In rare use cases when
 * access to a specific implementation of this SPI is required - an instance of this SPI can be obtained
 * via {@link org.apache.ignite.Ignite#configuration()} method to check its configuration properties or call other non-SPI
 * methods. Note again that calling methods from this interface on the obtained instance can lead
 * to undefined behavior and explicitly not supported.
 *
 *  * Here is a Java example on how to configure grid with {@code GridH2IndexingSpi}.
 * <pre name="code" class="java">
 * GridIndexingSpi spi = new MyIndexingSpi();
 *
 * GridConfiguration cfg = new GridConfiguration();
 *
 * // Overrides default indexing SPI.
 * cfg.setIndexingSpi(spi);
 *
 * // Starts grid.
 * G.start(cfg);
 * </pre>
 * Here is an example of how to configure {@code GridH2IndexingSpi} from Spring XML configuration file.
 * <pre name="code" class="xml">
 * &lt;property name=&quot;indexingSpi&quot;&gt;
 *     &lt;bean class=&quot;com.example.MyIndexingSpi&quot;&gt;
 *     &lt;/bean&gt;
 * &lt;/property&gt;
 * </pre>
 * <p>
 * <img src="http://www.gridgain.com/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 */
public interface GridIndexingSpi extends IgniteSpi {
    /**
     * Executes query.
     *
     * @param spaceName Space name.
     * @param params Query parameters.
     * @param filters System filters.
     * @return Query result.
     * @throws IgniteSpiException If failed.
     */
    public Iterator<?> query(@Nullable String spaceName, Collection<Object> params,
        @Nullable GridIndexingQueryFilter filters) throws IgniteSpiException;

    /**
     * Updates index. Note that key is unique for space, so if space contains multiple indexes
     * the key should be removed from indexes other than one being updated.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @param val Value.
     * @param expirationTime Expiration time or 0 if never expires.
     * @throws IgniteSpiException If failed.
     */
    public void store(@Nullable String spaceName, Object key, Object val, long expirationTime) throws IgniteSpiException;

    /**
     * Removes index entry by key.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @throws IgniteSpiException If failed.
     */
    public void remove(@Nullable String spaceName, Object key) throws IgniteSpiException;

    /**
     * Will be called when entry with given key is swapped.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @throws IgniteSpiException If failed.
     */
    public void onSwap(@Nullable String spaceName, Object key) throws IgniteSpiException;

    /**
     * Will be called when entry with given key is unswapped.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @param val Value.
     * @throws IgniteSpiException If failed.
     */
    public void onUnswap(@Nullable String spaceName, Object key, Object val) throws IgniteSpiException;
}
