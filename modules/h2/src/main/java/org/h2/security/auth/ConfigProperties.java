/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.security.auth;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.h2.util.Utils;

/**
 * wrapper for configuration properties
 */
public class ConfigProperties {

    private Map<String, String> properties;

    public ConfigProperties() {
        properties = new HashMap<>();
    }

    public ConfigProperties(PropertyConfig... configProperties) {
        this(configProperties == null ? null : Arrays.asList(configProperties));
    }

    public ConfigProperties(Collection<PropertyConfig> configProperties) {
        properties = new HashMap<>();
        if (properties != null) {
            for (PropertyConfig currentProperty : configProperties) {
                if (properties.put(currentProperty.getName(), currentProperty.getValue()) != null) {
                    throw new AuthConfigException("duplicate property " + currentProperty.getName());
                }
            }
        }
    }

    /**
     * Returns the string value of specified property.
     *
     * @param name property name.
     * @param defaultValue default value.
     * @return the string property value or {@code defaultValue} if the property is missing.
     */
    public String getStringValue(String name, String defaultValue) {
        String result = properties.get(name);
        if (result == null) {
            return defaultValue;
        }
        return result;
    }

    /**
     * Returns the string value of specified property.
     *
     * @param name property name.
     * @return the string property value.
     * @throws AuthConfigException if the property is missing.
     */
    public String getStringValue(String name) {
        String result = properties.get(name);
        if (result == null) {
            throw new AuthConfigException("missing config property " + name);
        }
        return result;
    }

    /**
     * Returns the integer value of specified property.
     *
     * @param name property name.
     * @param defaultValue default value.
     * @return the integer property value or {@code defaultValue} if the property is missing.
     */
    public int getIntValue(String name, int defaultValue) {
        String result = properties.get(name);
        if (result == null) {
            return defaultValue;
        }
        return Integer.parseInt(result);
    }

    /**
     * Returns the integer value of specified property.
     *
     * @param name property name.
     * @return the integer property value.
     * @throws AuthConfigException if the property is missing.
     */
    public int getIntValue(String name) {
        String result = properties.get(name);
        if (result == null) {
            throw new AuthConfigException("missing config property " + name);
        }
        return Integer.parseInt(result);
    }

    /**
     * Returns the boolean value of specified property.
     *
     * @param name property name.
     * @param defaultValue default value.
     * @return the boolean property value or {@code defaultValue} if the property is missing.
     */
    public boolean getBooleanValue(String name, boolean defaultValue) {
        String result = properties.get(name);
        if (result == null) {
            return defaultValue;
        }
        return Utils.parseBoolean(result, defaultValue, true);
    }

}
