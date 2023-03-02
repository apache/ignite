package com.shard.jdbc.exception;

/**
 * Created by shun on 2015-12-16 18:20.
 */
public class NoMatchDataSourceException extends DbException {

    public NoMatchDataSourceException(String msg, Object... args) {
        super(String.format(msg, args));
    }

}
