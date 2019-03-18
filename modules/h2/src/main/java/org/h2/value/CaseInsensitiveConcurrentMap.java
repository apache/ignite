/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.value;

import java.util.concurrent.ConcurrentHashMap;
import org.h2.util.StringUtils;

/**
 * A concurrent hash map with a case-insensitive string key, that also allows
 * NULL as a key.
 *
 * @param <V> the value type
 */
public class CaseInsensitiveConcurrentMap<V> extends ConcurrentHashMap<String, V> {

    private static final long serialVersionUID = 1L;
    private static final String NULL = new String(new byte[0]);

    @Override
    public V get(Object key) {
        return super.get(toUpper(key));
    }

    @Override
    public V put(String key, V value) {
        return super.put(toUpper(key), value);
    }

    @Override
    public boolean containsKey(Object key) {
        return super.containsKey(toUpper(key));
    }

    @Override
    public V remove(Object key) {
        return super.remove(toUpper(key));
    }

    private static String toUpper(Object key) {
        return key == null ? NULL : StringUtils.toUpperEnglish(key.toString());
    }

}
