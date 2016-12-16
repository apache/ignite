package org.apache.ignite.internal.processors.cache.sql;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Created by yuryandreev on 28/10/2016.
 */
public class PersonTest {
    /** Name. */
    @QuerySqlField
    private String name;

    /** Age. */
    @QuerySqlField
    private int age;

    /**
     * @param name Name.
     * @param age Age.
     */
    public PersonTest(String name, int age) {
        this.name = name;
        this.age = age;
    }

    /**
     *
     */
    public String getName() {
        return name;
    }

    /**
     * @param name Name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     *
     */
    public int getAge() {
        return age;
    }

    /**
     * @param age Age.
     */
    public void setAge(int age) {
        this.age = age;
    }
}
