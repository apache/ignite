/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.spring;

import org.springframework.cache.interceptor.*;

import java.lang.reflect.*;

/**
 * Key generator.
 */
public class GridSpringCacheTestKeyGenerator implements KeyGenerator {
    /** {@inheritDoc} */
    @Override public Object generate(Object target, Method mtd, Object... params) {
        assert params != null;
        assert params.length > 0;

        if (params.length == 1)
            return params[0];
        else {
            assert params.length == 2;

            return new GridSpringCacheTestKey((Integer)params[0], (String)params[1]);
        }
    }
}
