package com.shard.jdbc.shard;

import java.io.Serializable;

/**
 * Shard configuration props
 * Created by shun on 2015-12-16 18:06.
 */
public class Shard implements Serializable{

    private static final long serialVersionUID = -2405737710724475171L;
    private String clazz;
    private String column;
    private int value;

    public Shard() {}
    public Shard(String clazz, String column, int value) {
        this.clazz = clazz;
        this.column = column;
        this.value = value;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

}
