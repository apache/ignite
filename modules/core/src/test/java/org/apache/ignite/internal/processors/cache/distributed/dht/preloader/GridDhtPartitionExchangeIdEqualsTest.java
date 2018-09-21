package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import org.junit.Test;

import static org.junit.Assert.*;

public class GridDhtPartitionExchangeIdEqualsTest {
    @Test
    public void equalsTest(){
        GridDhtPartitionExchangeId obj = new GridDhtPartitionExchangeId();
        assertFalse(obj.equals(new Object()));
    }
}
