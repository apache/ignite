package com.shard.jdbc.domain;

/**
 * Created by shun on 2015-12-16 15:54.
 */
public class Teacher {

    private long id;
    private String name;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public int hashCode() {
        return Long.valueOf(id).hashCode() + name.hashCode();
    }

    public String toString() {
        return "id:" + id + ", name:" + name;
    }
}
