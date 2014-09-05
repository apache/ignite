/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.spring;

import org.springframework.cache.annotation.*;

import java.util.concurrent.atomic.*;

/**
 * Test service.
 */
public class GridSpringCacheTestService {
    /** */
    private final AtomicInteger cnt = new AtomicInteger();

    /**
     * @return How many times service was called.
     */
    public int called() {
        return cnt.get();
    }

    /**
     * Resets service.
     */
    public void reset() {
        cnt.set(0);
    }

    /**
     * @param key Key.
     * @return Value.
     */
    @Cacheable("testCache")
    public String simpleKey(Integer key) {
        assert key != null;

        cnt.incrementAndGet();

        return "value" + key;
    }

    /**
     * @param p1 Parameter 1.
     * @param p2 Parameter 2.
     * @return Value.
     */
    @Cacheable("testCache")
    public String complexKey(Integer p1, String p2) {
        assert p1 != null;
        assert p2 != null;

        cnt.incrementAndGet();

        return "value" + p1 + p2;
    }

    /**
     * @param key Key.
     * @return Value.
     */
    @CachePut("testCache")
    public String simpleKeyPut(Integer key) {
        assert key != null;

        int cnt0 = cnt.incrementAndGet();

        return "value" + key + (cnt0 % 2 == 0 ? "even" : "odd");
    }

    /**
     * @param p1 Parameter 1.
     * @param p2 Parameter 2.
     * @return Value.
     */
    @CachePut("testCache")
    public String complexKeyPut(Integer p1, String p2) {
        assert p1 != null;
        assert p2 != null;

        int cnt0 = cnt.incrementAndGet();

        return "value" + p1 + p2 + (cnt0 % 2 == 0 ? "even" : "odd");
    }
}
