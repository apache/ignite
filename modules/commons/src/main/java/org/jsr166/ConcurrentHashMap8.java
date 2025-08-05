/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

/*
 * The latest version of the file was copied from the following CVS repository:
 * http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/
 *
 * Corresponding commit version in CVS repository is unknown (lost on our side).
 * On the other hand we can't simply synch the latest version from CVS here, because Ignite uses functionality that
 * is no longer supported.
 */

package org.jsr166;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class use only for deserialization. In code use ConcurrentHashMap.
 *
 * @deprecated need for java deserialization ConcurrentHashMap8 from old nodes.
 */
@Deprecated
@SuppressWarnings("ALL")
public class ConcurrentHashMap8<K, V> implements Serializable {
    private static final long serialVersionUID = 7249069246763182397L;

    private Map<K, V> actualMap;

    /**
     * Reconstitutes the instance from a stream (that is, deserializes it).
     *
     * @param s the stream
     */
    @SuppressWarnings("unchecked") private void readObject(java.io.ObjectInputStream s)
        throws java.io.IOException, ClassNotFoundException {
        s.defaultReadObject();

        actualMap = new ConcurrentHashMap<>();
        for (; ; ) {
            K k = (K)s.readObject();
            V v = (V)s.readObject();

            if (k != null && v != null) {
                actualMap.put(k, v);
            }
            else
                break;
        }
    }

    /**
     * @return ConcurrentHashMap instead ConcurrentHashMap8 for using in code.
     */
    Object readResolve() throws ObjectStreamException {
        return actualMap;
    }
}
