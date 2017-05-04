package org.apache.ignite.internal.processors.cache.database;

import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.T2;

/**
 *
 */
public interface DbCheckpointListener {
    public interface Context {
        public boolean nextSnapshot();

        public Map<T2<Integer, Integer>, T2<Integer, Integer>> partitionStatMap();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void onCheckpointBegin(Context context) throws IgniteCheckedException;
}
