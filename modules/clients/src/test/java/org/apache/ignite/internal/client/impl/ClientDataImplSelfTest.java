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

package org.apache.ignite.internal.client.impl;

import java.util.concurrent.Callable;
import org.apache.ignite.internal.client.GridClientData;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Simple unit test for GridClientDataImpl which checks method parameters.
 */
@RunWith(JUnit4.class)
public class ClientDataImplSelfTest extends GridCommonAbstractTest {
    /** Mocked client data. */
    private GridClientData data = allocateInstance0(GridClientDataImpl.class);

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPut() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                data.put(null, "val");

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: key");

        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                data.put("key", null);

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: val");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAsync() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                data.putAsync(null, "val");

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: key");

        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                data.putAsync("key", null);

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: val");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAll() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                data.putAll(null);

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: entries");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAllAsync() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                data.putAllAsync(null);

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: entries");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGet() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return data.get(null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: key");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAsync() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return data.getAsync(null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: key");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAll() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return data.getAll(null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: keys");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAllAsync() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return data.getAllAsync(null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: keys");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemove() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                data.remove(null);

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: key");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAsync() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return data.removeAsync(null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: key");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAll() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                data.removeAll(null);

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: keys");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAllAsync() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                data.removeAllAsync(null);

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: keys");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplace() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                data.replace(null, "val");

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: key");

        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                data.replace("key", null);

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: val");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplaceAsync() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return data.replaceAsync(null, "val");
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: key");

        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return data.replaceAsync("key", null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: val");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCas() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                data.cas(null, "val1", "val2");

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: key");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCasAsync() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return data.casAsync(null, "val1", "val2");
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: key");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinity() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return data.affinity(null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: key");
    }
}
