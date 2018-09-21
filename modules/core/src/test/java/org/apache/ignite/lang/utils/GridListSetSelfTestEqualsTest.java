package org.apache.ignite.lang.utils;

import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class GridListSetSelfTestEqualsTest {
    @Test
    public void V1EqualsTest(){
        GridListSetSelfTest.V1 obj =
            new GridListSetSelfTest.V1(1);
        assertFalse(obj.equals(null));
        assertFalse(obj.equals(new Object()));
    }

    @Test
    public void V2EqualsTest(){
        GridListSetSelfTest.V2 obj =
            new GridListSetSelfTest.V2(1);
        assertFalse(obj.equals(null));
        assertFalse(obj.equals(new Object()));
    }
}
