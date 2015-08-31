/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client.impl;

import java.util.concurrent.Callable;
import org.apache.ignite.internal.client.GridClientData;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Simple unit test for GridClientDataImpl which checks method parameters.
 */
public class ClientDataImplSelfTest extends GridCommonAbstractTest {
    /** Mocked client data. */
    private GridClientData data = allocateInstance0(GridClientDataImpl.class);

    /**
     * @throws Exception If failed.
     */
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
    public void testAffinity() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return data.affinity(null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: key");
    }
}