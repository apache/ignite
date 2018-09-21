package org.apache.ignite.loadtests.colocation;

import org.junit.Test;

import static org.junit.Assert.*;

public class GridTestKeyEqualsTest {
    @Test
    public void equalsTest(){
        GridTestKey obj = new GridTestKey();
        assertFalse(obj.equals(null));
        assertFalse(obj.equals(new Object()));
    }
}
