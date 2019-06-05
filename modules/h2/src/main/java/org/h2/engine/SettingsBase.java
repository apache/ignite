/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.HashMap;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.util.Utils;

/**
 * The base class for settings.
 */
public class SettingsBase {

    private final HashMap<String, String> settings;

    protected SettingsBase(HashMap<String, String> s) {
        this.settings = s;
    }

    /**
     * Get the setting for the given key.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the setting
     */
    protected boolean get(String key, boolean defaultValue) {
        String s = get(key, Boolean.toString(defaultValue));
        try {
            return Utils.parseBoolean(s, defaultValue, true);
        } catch (IllegalArgumentException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1,
                    e, "key:" + key + " value:" + s);
        }
    }

    /**
     * Get the setting for the given key.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the setting
     */
    protected int get(String key, int defaultValue) {
        String s = get(key, "" + defaultValue);
        try {
            return Integer.decode(s);
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1,
                    e, "key:" + key + " value:" + s);
        }
    }

    /**
     * Get the setting for the given key.
     *
     * @param key the key
     * @param defaultValue the default value
     * @return the setting
     */
    protected String get(String key, String defaultValue) {
        String v = settings.get(key);
        if (v != null) {
            return v;
        }
        StringBuilder buff = new StringBuilder("h2.");
        boolean nextUpper = false;
        for (char c : key.toCharArray()) {
            if (c == '_') {
                nextUpper = true;
            } else {
                // Character.toUpperCase / toLowerCase ignores the locale
                buff.append(nextUpper ? Character.toUpperCase(c) : Character.toLowerCase(c));
                nextUpper = false;
            }
        }
        String sysProperty = buff.toString();
        v = Utils.getProperty(sysProperty, defaultValue);
        settings.put(key, v);
        return v;
    }

    /**
     * Check if the settings contains the given key.
     *
     * @param k the key
     * @return true if they do
     */
    protected boolean containsKey(String k) {
        return settings.containsKey(k);
    }

    /**
     * Get all settings.
     *
     * @return the settings
     */
    public HashMap<String, String> getSettings() {
        return settings;
    }

}
