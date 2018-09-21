package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class GridDhtPartitionMapEqualsTest {
    @Test
    public void eqaulsTest(){
        GridDhtPartitionMap obj = new GridDhtPartitionMap();
        assertFalse(obj.equals(null));
        assertFalse(obj.equals(new Object()));
    }
}
