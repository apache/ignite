/*
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.jmh.model;

import org.apache.ignite.cache.query.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Created by GridAdmin1234 on 6/25/2015.
 */
public class Organization implements Serializable {
    @QuerySqlField (index = true)
    private UUID id;

    @QuerySqlField
    private String name;

    public Organization(String name) {
        id = UUID.randomUUID();

        this.name = name;
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}
