package org.apache.ignite.internal.processors.cache;

import org.junit.Test;

import static org.junit.Assert.*;

public class GatewayProtectedCacheProxyEqualsTest {
    @Test
    public void equalsTest(){
        GatewayProtectedCacheProxy obj = new GatewayProtectedCacheProxy();
        assertFalse(obj.equals(null));
        assertFalse(obj.equals(new Object()));
    }
}
