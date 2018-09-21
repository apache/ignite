package org.apache.ignite.internal.marshaller.optimized;

import org.junit.Test;

import static org.junit.Assert.*;

public class OptimizedMarshallerSelfTestEqualsTest {
    @Test
    public void equalsTest(){
        OptimizedMarshallerSelfTest.TestObject2 obj = new OptimizedMarshallerSelfTest.TestObject2(1);
        assertFalse(obj.equals(null));
        assertFalse(obj.equals(new Object()));
    }
}
