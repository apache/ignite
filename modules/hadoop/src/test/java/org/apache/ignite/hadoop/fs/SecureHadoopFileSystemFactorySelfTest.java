package org.apache.ignite.hadoop.fs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import junit.framework.TestCase;
import org.junit.Assert;

/**
 * Tests SecureHadoopFileSystemFactory.
 */
public class SecureHadoopFileSystemFactorySelfTest extends TestCase {
    /**
     * Checks parameters validation.
     *
     * @throws Exception
     */
    public void testParameters() throws Exception {
        SecureHadoopFileSystemFactory fac = new SecureHadoopFileSystemFactory();

        fac.setKeyTab(null);
        fac.setKeyTabPrincipal("princ");

        try {
            fac.start();

            fail("IllegalArgumentException expected because key tab is null.");
        }
        catch (IllegalArgumentException iae) {
            // okay
        }

        fac.setKeyTab("/tmp/mykeytab");
        fac.setKeyTabPrincipal(null);

        try {
            fac.start();

            fail("IllegalArgumentException expected because principal is null.");
        }
        catch (IllegalArgumentException iae) {
            // okay
        }
    }

    /**
     * Checks serializatuion and deserialization of the secure factory.
     *
     * @throws Exception If failed.
     */
    public void testSerialization() throws Exception {
        SecureHadoopFileSystemFactory fac = new SecureHadoopFileSystemFactory();

        checkSerialization(fac);

        fac = new SecureHadoopFileSystemFactory();

        fac.setUri("igfs://igfs@localhost:10500/");
        fac.setConfigPaths("/a/core-sute.xml", "/b/mapred-site.xml");
        fac.setKeyTabPrincipal("foo");
        fac.setKeyTab("/etc/krb5.keytab");
        fac.setReloginInterval(30 * 60 * 1000L);

        checkSerialization(fac);
    }

    /**
     * Serializes the factory,
     *
     * @param fac The facory to check.
     * @throws Exception If failed.
     */
    private void checkSerialization(SecureHadoopFileSystemFactory fac) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        ObjectOutput oo = new ObjectOutputStream(baos);

        oo.writeObject(fac);

        ObjectInput in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));

        SecureHadoopFileSystemFactory fac2 = (SecureHadoopFileSystemFactory)in.readObject();

        assertEquals(fac.getUri(), fac2.getUri());
        Assert.assertArrayEquals(fac.getConfigPaths(), fac2.getConfigPaths());
        assertEquals(fac.getKeyTabPrincipal(), fac2.getKeyTabPrincipal());
        assertEquals(fac.getReloginInterval(), fac2.getReloginInterval());
    }
}