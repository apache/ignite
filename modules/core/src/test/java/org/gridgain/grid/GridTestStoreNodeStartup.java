/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.gridgain.grid.util.typedef.*;

import javax.swing.*;

/**
 * Starts test node.
 */
public class GridTestStoreNodeStartup {
    /**
     *
     */
    private GridTestStoreNodeStartup() {
        // No-op.
    }

    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try {
            Ignite g = G.start("modules/core/src/test/config/spring-cache-teststore.xml");

            g.cache(null).loadCache(new P2<Object, Object>() {
                @Override public boolean apply(Object o, Object o1) {
                    System.out.println("Key=" + o + ", Val=" + o1);

                    return true;
                }
            }, 0, 15, 1);

            JOptionPane.showMessageDialog(null, "Press OK to stop test node.");
        }
        finally {
            G.stopAll(false);
        }
    }
}
