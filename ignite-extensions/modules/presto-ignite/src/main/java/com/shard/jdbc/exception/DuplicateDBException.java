package com.shard.jdbc.exception;

/**
 * Created by shun on 2015-12-22 17:32.
 */
public class DuplicateDBException extends DbException{
    public DuplicateDBException(String msg, Object... args) {
        super(msg, args);
    }
}
