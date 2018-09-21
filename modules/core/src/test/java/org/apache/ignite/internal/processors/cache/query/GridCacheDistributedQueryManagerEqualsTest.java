package org.apache.ignite.internal.processors.cache.query;

import java.util.UUID;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class GridCacheDistributedQueryManagerEqualsTest {
    @Test
    public void equalsTest(){
        GridCacheDistributedQueryManager.CancelMessageId obj =
            new GridCacheDistributedQueryManager.CancelMessageId(1L, UUID.randomUUID());
        assertFalse(obj.equals(null));
        assertFalse(obj.equals(new Object()));
    }
}
