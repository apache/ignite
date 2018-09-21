package org.apache.ignite.internal.processors.service;

import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class GridServiceMethodReflectKeyEqualsTest {
    @Test
    public void equalsTest(){
        GridServiceMethodReflectKey obj =
            new GridServiceMethodReflectKey("name", null);
        assertFalse(obj.equals(null));
        assertFalse(obj.equals(new Object()));
    }
}
