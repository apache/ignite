/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.multijvm;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class PutInCache implements IgniteNodeRunner.Task {
    /** {@inheritDoc} */
    @Override public boolean execute(Ignite ignite, String... args) {
        int cnt = Integer.valueOf(args[0]);
        
        IgniteCache<Integer, String> cache = ignite.cache(null);

        for (int i = 1; i <= cnt; i++) {
            cache.put(i, "val" + i);

            X.println(">>>>> Put=" + "val" + i);
        }

        return true;
    }
}
