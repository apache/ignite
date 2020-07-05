package com.shard.jdbc.exception;

/**
 * Created by shun on 2015-12-16 18:21.
 */
public class DbException extends Exception{

    public DbException(String msg, Object... args) {
        super(String.format(msg, args));
    }

}
