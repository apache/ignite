/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * Utility to detect AB-BA deadlocks.
 */
public class AbbaDetector {

    private static final boolean TRACE = false;

    private static final ThreadLocal<Deque<Object>> STACK =
            new ThreadLocal<Deque<Object>>() {
                @Override protected Deque<Object> initialValue() {
                    return new ArrayDeque<>();
            }
        };

    /**
     * Map of (object A) -> (
     *      map of (object locked before object A) ->
     *      (stack trace where locked) )
     */
    private static final Map<Object, Map<Object, Exception>> LOCK_ORDERING =
            new WeakHashMap<>();

    private static final Set<String> KNOWN_DEADLOCKS = new HashSet<>();

    /**
     * This method is called just before or just after an object is
     * synchronized.
     *
     * @param o the object, or null for the current class
     * @return the object that was passed
     */
    public static Object begin(Object o) {
        if (o == null) {
            o = new SecurityManager() {
                Class<?> clazz = getClassContext()[2];
            }.clazz;
        }
        Deque<Object> stack = STACK.get();
        if (!stack.isEmpty()) {
            // Ignore locks which are locked multiple times in succession -
            // Java locks are recursive
            if (stack.contains(o)) {
                // already synchronized on this
                return o;
            }
            while (!stack.isEmpty()) {
                Object last = stack.peek();
                if (Thread.holdsLock(last)) {
                    break;
                }
                stack.pop();
            }
        }
        if (TRACE) {
            String thread = "[thread " + Thread.currentThread().getId() + "]";
            String indent = new String(new char[stack.size() * 2]).replace((char) 0, ' ');
            System.out.println(thread + " " + indent +
                    "sync " + getObjectName(o));
        }
        if (!stack.isEmpty()) {
            markHigher(o, stack);
        }
        stack.push(o);
        return o;
    }

    private static Object getTest(Object o) {
        // return o.getClass();
        return o;
    }

    private static String getObjectName(Object o) {
        return o.getClass().getSimpleName() + "@" + System.identityHashCode(o);
    }

    private static synchronized void markHigher(Object o, Deque<Object> older) {
        Object test = getTest(o);
        Map<Object, Exception> map = LOCK_ORDERING.get(test);
        if (map == null) {
            map = new WeakHashMap<>();
            LOCK_ORDERING.put(test, map);
        }
        Exception oldException = null;
        for (Object old : older) {
            Object oldTest = getTest(old);
            if (oldTest == test) {
                continue;
            }
            Map<Object, Exception> oldMap = LOCK_ORDERING.get(oldTest);
            if (oldMap != null) {
                Exception e = oldMap.get(test);
                if (e != null) {
                    String deadlockType = test.getClass() + " " + oldTest.getClass();
                    if (!KNOWN_DEADLOCKS.contains(deadlockType)) {
                        String message = getObjectName(test) +
                                " synchronized after \n " + getObjectName(oldTest) +
                                ", but in the past before";
                        RuntimeException ex = new RuntimeException(message);
                        ex.initCause(e);
                        ex.printStackTrace(System.out);
                        // throw ex;
                        KNOWN_DEADLOCKS.add(deadlockType);
                    }
                }
            }
            if (!map.containsKey(oldTest)) {
                if (oldException == null) {
                    oldException = new Exception("Before");
                }
                map.put(oldTest, oldException);
            }
        }
    }

}
