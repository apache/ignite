package com.shard.jdbc.reader;

import java.util.List;

/**
 * Created by shun on 2015-12-16 16:22.
 */
public abstract class Reader {

    /**
     * process many kinds of files
     * @param path
     * @param tClass
     * @param <T>
     * @return
     */
    public abstract <T> List<T> process(String path, Class<T> tClass) throws Exception;

}
