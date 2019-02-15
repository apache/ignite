/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.hadoop.impl.fs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.concurrent.Callable;

import org.apache.ignite.hadoop.fs.KerberosHadoopFileSystemFactory;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopDelegateUtils;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopFileSystemFactoryDelegate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests KerberosHadoopFileSystemFactory.
 */
@RunWith(JUnit4.class)
public class KerberosHadoopFileSystemFactorySelfTest extends GridCommonAbstractTest {
    /**
     * Test parameters validation.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testParameters() throws Exception {
        checkParameters(null, null, -1);

        checkParameters(null, null, 100);
        checkParameters(null, "b", -1);
        checkParameters("a", null, -1);

        checkParameters(null, "b", 100);
        checkParameters("a", null, 100);
        checkParameters("a", "b", -1);
    }

    /**
     * Check parameters.
     *
     * @param keyTab Key tab.
     * @param keyTabPrincipal Key tab principal.
     * @param reloginInterval Re-login interval.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void checkParameters(String keyTab, String keyTabPrincipal, long reloginInterval) {
        final KerberosHadoopFileSystemFactory fac = new KerberosHadoopFileSystemFactory();

        fac.setKeyTab(keyTab);
        fac.setKeyTabPrincipal(keyTabPrincipal);
        fac.setReloginInterval(reloginInterval);

        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                HadoopFileSystemFactoryDelegate delegate = HadoopDelegateUtils.fileSystemFactoryDelegate(
                    getClass().getClassLoader(), fac);

                delegate.start();

                return null;
            }
        }, IllegalArgumentException.class, null);
    }

    /**
     * Checks serializatuion and deserialization of the secure factory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSerialization() throws Exception {
        KerberosHadoopFileSystemFactory fac = new KerberosHadoopFileSystemFactory();

        checkSerialization(fac);

        fac = new KerberosHadoopFileSystemFactory();

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
    private void checkSerialization(KerberosHadoopFileSystemFactory fac) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        ObjectOutput oo = new ObjectOutputStream(baos);

        oo.writeObject(fac);

        ObjectInput in = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));

        KerberosHadoopFileSystemFactory fac2 = (KerberosHadoopFileSystemFactory)in.readObject();

        assertEquals(fac.getUri(), fac2.getUri());
        Assert.assertArrayEquals(fac.getConfigPaths(), fac2.getConfigPaths());
        assertEquals(fac.getKeyTab(), fac2.getKeyTab());
        assertEquals(fac.getKeyTabPrincipal(), fac2.getKeyTabPrincipal());
        assertEquals(fac.getReloginInterval(), fac2.getReloginInterval());
    }
}
