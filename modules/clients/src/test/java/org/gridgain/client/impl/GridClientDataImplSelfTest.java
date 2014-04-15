/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.impl;

import org.gridgain.client.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;

import static org.gridgain.testframework.GridTestUtils.*;

/**
 * Simple unit test for GridClientDataImpl which checks method parameters.
 */
public class GridClientDataImplSelfTest extends GridCommonAbstractTest {
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
