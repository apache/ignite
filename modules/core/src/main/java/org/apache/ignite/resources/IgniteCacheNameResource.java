/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.resources;

import java.lang.annotation.*;
import org.gridgain.grid.cache.*;

/**
 * Annotates a field or a setter method for injection of grid cache name.
 * Grid cache name is provided to cache via {@link GridCacheConfiguration#getName()} method.
 * <p>
 * Cache name can be injected into components provided in the {@link GridCacheConfiguration},
 * if {@link IgniteCacheNameResource} annotation is used in another classes it is no-op.
 * <p>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 * public class MyCacheStore implements GridCacheStore {
 *      ...
 *      &#64;GridCacheNameResource
 *      private String cacheName;
 *      ...
 *  }
 * </pre>
 * or
 * <pre name="code" class="java">
 * public class MyCacheStore implements GridCacheStore {
 *     ...
 *     private String cacheName;
 *     ...
 *     &#64;GridCacheNameResource
 *     public void setCacheName(String cacheName) {
 *          this.cacheName = cacheName;
 *     }
 *     ...
 * }
 * </pre>
 * <p>
 * See {@link GridCacheConfiguration#getName()} for cache configuration details.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface IgniteCacheNameResource {
    // No-op.
}
