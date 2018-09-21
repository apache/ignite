package org.apache.ignite.internal.processors.cache.distributed;

import org.junit.Test;

import static org.junit.Assert.*;

public class GridCachePreloadLifecycleAbstractTestEqualsTest {
    @Test
    public void equalsTest(){
        GridCachePreloadLifecycleAbstractTest.MyStringKey obj = new GridCachePreloadLifecycleAbstractTest.MyStringKey("");
        assertFalse(obj.equals(null));
    }
}
