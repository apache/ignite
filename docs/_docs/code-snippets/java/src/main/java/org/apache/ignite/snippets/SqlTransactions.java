package org.apache.ignite.snippets;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;

public class SqlTransactions {

    void enableMVCC() {
        //tag::enable[]
        CacheConfiguration cacheCfg = new CacheConfiguration<>();
        cacheCfg.setName("myCache");

        cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT);

        //end::enable[]
    }
}
