package org.apache.ignite.math.impls.storage;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import org.apache.ignite.math.VectorStorage;
import org.junit.Test;

import static org.apache.ignite.math.impls.MathTestConstants.VALUE_NOT_EQUALS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Common test for externalization.
 */
public abstract class ExternalizeTest<T extends Externalizable> {
    /** */
    protected void externalizeTest(T initialObj) {
        try {
            PipedOutputStream pipedOutputStream = new PipedOutputStream();

            PipedInputStream pipedInputStream = new PipedInputStream(pipedOutputStream);

            ObjectOutputStream objectOutputStream = new ObjectOutputStream(pipedOutputStream);

            ObjectInputStream objectInputStream = new ObjectInputStream(pipedInputStream);

            objectOutputStream.writeObject(initialObj);

            T objRestored = (T) objectInputStream.readObject();

            objectInputStream.close();

            assertTrue(VALUE_NOT_EQUALS, initialObj.equals(objRestored));
        } catch (ClassNotFoundException | IOException e) {
            fail(e.getMessage());
        }
    }

    /** */
    @Test
    public abstract void externalizeTest();
}
