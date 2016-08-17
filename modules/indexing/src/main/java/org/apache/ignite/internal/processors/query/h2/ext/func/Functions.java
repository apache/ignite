package org.apache.ignite.internal.processors.query.h2.ext.func;

import org.apache.ignite.cache.query.annotations.QuerySqlFunction;

/**
 * Created by amashenkov on 17.08.16.
 */
public class Functions {

    @QuerySqlFunction
    public static long len(String x) {
        return x.length();
    }

}
