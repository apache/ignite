/*
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.jmh;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.plugin.*;
import org.jsr166.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Test marshaller context.
 */
public class MarshallerContextJMHImpl extends MarshallerContextAdapter {
    /** */
    private final static ConcurrentMap<Integer, String> map = new ConcurrentHashMap8<>();

    /**
     * Initializes context.
     *
     * @param plugins Plugins.
     */
    public MarshallerContextJMHImpl(List<PluginProvider> plugins) {
        super(plugins);
    }

    /**
     * Initializes context.
     */
    public MarshallerContextJMHImpl() {
        super(null);
    }

    /** {@inheritDoc} */
    @Override protected boolean registerClassName(int id, String clsName) throws IgniteCheckedException {
        String oldClsName = map.putIfAbsent(id, clsName);

        if (oldClsName != null && !oldClsName.equals(clsName))
            throw new IgniteCheckedException("Duplicate ID [id=" + id + ", oldClsName=" + oldClsName + ", clsName=" +
                clsName + ']');

        return true;
    }

    /** {@inheritDoc} */
    @Override protected String className(int id) {
        return map.get(id);
    }
}
