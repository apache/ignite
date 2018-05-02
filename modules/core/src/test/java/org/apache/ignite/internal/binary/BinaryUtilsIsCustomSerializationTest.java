package org.apache.ignite.internal.binary;

import static org.apache.ignite.internal.binary.BinaryUtils.isCustomJavaSerialization;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.Serializable;

import org.junit.Test;

import junit.framework.TestCase;

public class BinaryUtilsIsCustomSerializationTest extends TestCase {
    
    @Test
    public void testCustomReadOnly() {
        assertTrue(isCustomJavaSerialization(HasCustomReadMethod.class));
    }
    
    @Test
    public void testCustomWriteOnly() {
        assertTrue(isCustomJavaSerialization(HasCustomWriteMethod.class));
    }
    
    @Test
    public void testBothCustomMethodsOnly() {
        assertTrue(isCustomJavaSerialization(HasBothCustomMethods.class));
    }
    
    @Test
    public void testNoCustomMethods() {
        assertFalse(isCustomJavaSerialization(HasNoCustomMethods.class));
    }
    
    @Test
    public void testStaticCustomMethod() {
        assertFalse(isCustomJavaSerialization(HasStaticCustomMethod.class));
    }
    
    @Test
    public void testCustomMethodWithWrongReturnType() {
        assertFalse(isCustomJavaSerialization(HasCustomMethodWithWrongReturnType.class));
    }
    
    
    private static class HasCustomReadMethod implements Serializable {
        
        private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {}
    }
    
    private static class HasCustomWriteMethod implements Serializable {
        
        private void writeObject(java.io.ObjectOutputStream stream) throws IOException {}
    }
    
    private static class HasBothCustomMethods implements Serializable {
        
        private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {}
        private void writeObject(java.io.ObjectOutputStream stream) throws IOException {}
    }
    
    private static class HasNoCustomMethods implements Serializable {
    
    }
    
    private static class HasStaticCustomMethod implements Serializable {
        
        private static void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {}
    }
    
    private static class HasCustomMethodWithWrongReturnType implements Serializable {
        
        private static int readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
            return 0;
        }
    }
}
