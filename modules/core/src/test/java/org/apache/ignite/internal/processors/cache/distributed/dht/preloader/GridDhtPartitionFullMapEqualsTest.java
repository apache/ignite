package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.junit.Test;

import static org.junit.Assert.*;

public class GridDhtPartitionFullMapEqualsTest {
    @Test
    public void equalsTest(){
        GridDhtPartitionFullMap obj = new GridDhtPartitionFullMap();
        assertFalse(obj.equals(null));
        assertFalse(obj.equals(new Object()));
    }
}
