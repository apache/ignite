/*
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.jmh.model;

import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.query.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Created by GridAdmin1234 on 6/25/2015.
 */
public class Department implements Serializable {
    @QuerySqlField(index = true)
    private UUID id;

    @QuerySqlField (index = true)
    private UUID cityId;

    @QuerySqlField (index = true)
    private UUID orgId;

    @QuerySqlField
    private String name;

    private transient AffinityKey<UUID> key;

    public Department(String name, City city, Organization org) {
        id = UUID.randomUUID();

        cityId = city.getId();
        orgId = org.getId();

        this.name = name;
    }

    public UUID getId() {
        return id;
    }

    public UUID getCityId() {
        return cityId;
    }

    public UUID getOrgId() {
        return orgId;
    }

    public String getName() {
        return name;
    }

    public AffinityKey<UUID> getKey() {
        if (key == null)
            key = new AffinityKey<>(id, cityId);

        return key;
    }

    @Override public String toString() {
        return "Department{" +
            "id=" + id +
            ", cityId=" + cityId +
            ", orgId=" + orgId +
            ", name='" + name + '\'' +
            '}';
    }
}
