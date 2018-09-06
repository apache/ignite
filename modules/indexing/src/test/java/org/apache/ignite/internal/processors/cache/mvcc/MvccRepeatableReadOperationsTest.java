package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MvccRepeatableReadOperationsTest extends MvccRepeatableReadBulkOpsTest {
    /**
     * @param cache
     * @param keys
     * @param readMode
     * @return
     */
    @Override protected Map<Integer, MvccTestAccount> getValues(
        TestCache<Integer, MvccTestAccount> cache,
        Set<Integer> keys,
        ReadMode readMode) {


        switch (readMode) {
            case GET: {
                Map<Integer, MvccTestAccount> res = new HashMap<>();

                for (Integer key : keys)
                    res.put(key, cache.cache.get(key));

                return res;
            }
            case SQL:
                return getAllSql(cache);
            default:
                fail();
        }

        return null;
    }

    protected void updateValues(
        TestCache<Integer, MvccTestAccount> cache,
        Map<Integer, MvccTestAccount> vals,
        WriteMode readMode) {
        switch (readMode) {
            case PUT: {
                for (Map.Entry<Integer, MvccTestAccount> e : vals.entrySet())
                    cache.cache.put(e.getKey(), e.getValue());

                break;
            }
            case DML: {
                for (Map.Entry<Integer, MvccTestAccount> e : vals.entrySet()) {
                    if (e.getValue() == null)
                        removeSql(cache, e.getKey());
                    else
                        mergeSql(cache, e.getKey(), e.getValue().val, e.getValue().updateCnt);

                }
                break;
            }
            default:
                fail();
        }
    }
}
