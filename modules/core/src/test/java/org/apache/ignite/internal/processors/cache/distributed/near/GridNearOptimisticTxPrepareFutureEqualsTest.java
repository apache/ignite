package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.UUID;
import org.junit.Test;

import static org.junit.Assert.*;

public class GridNearOptimisticTxPrepareFutureEqualsTest {
    @Test
    public void equalsTest(){
        GridNearOptimisticTxPrepareFuture.MappingKey obj =
            new GridNearOptimisticTxPrepareFuture.MappingKey(UUID.randomUUID(), false);
        assertFalse(obj.equals(null));
        assertFalse(obj.equals(new Object()));
    }
}
