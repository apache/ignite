/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.security.auth;

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for class that maps users to their roles.
 *
 * @see org.h2.api.UserToRolesMapper
 */
public class UserToRolesMapperConfig implements HasConfigProperties {

    private String className;
    private List<PropertyConfig> properties;

    /**
     * @return Mapper class name.
     */
    public String getClassName() {
        return className;
    }

    /**
     * @param className mapper class name.
     */
    public void setClassName(String className) {
        this.className = className;
    }

    /**
     * @return Mapper properties.
     */
    @Override
    public List<PropertyConfig> getProperties() {
        if (properties == null) {
            properties = new ArrayList<>();
        }
        return properties;
    }

}
