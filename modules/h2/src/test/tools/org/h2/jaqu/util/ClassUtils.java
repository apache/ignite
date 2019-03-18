/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu.util;

import java.io.Reader;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.h2.util.IOUtils;

/**
 * Generic utility methods.
 */
public class ClassUtils {

    private static final AtomicLong COUNTER = new AtomicLong(0);

    private static final boolean MAKE_ACCESSIBLE = true;

    public static <A, B> IdentityHashMap<A, B> newIdentityHashMap() {
        return new IdentityHashMap<>();
    }

    @SuppressWarnings("unchecked")
    public static <T> T newObject(Class<T> clazz) {
        // must create new instances, cannot use methods like Boolean.FALSE,
        // since the caller relies on this creating unique objects
        if (clazz == Integer.class) {
            return (T) new Integer((int) COUNTER.getAndIncrement());
        } else if (clazz == String.class) {
            return (T) ("" + COUNTER.getAndIncrement());
        } else if (clazz == Long.class) {
            return (T) new Long(COUNTER.getAndIncrement());
        } else if (clazz == Short.class) {
            return (T) new Short((short) COUNTER.getAndIncrement());
        } else if (clazz == Byte.class) {
            return (T) new Byte((byte) COUNTER.getAndIncrement());
        } else if (clazz == Float.class) {
            return (T) new Float(COUNTER.getAndIncrement());
        } else if (clazz == Double.class) {
            return (T) new Double(COUNTER.getAndIncrement());
        } else if (clazz == Boolean.class) {
            return (T) new Boolean(false);
        } else if (clazz == BigDecimal.class) {
            return (T) new BigDecimal(COUNTER.getAndIncrement());
        } else if (clazz == BigInteger.class) {
            return (T) new BigInteger("" + COUNTER.getAndIncrement());
        } else if (clazz == java.sql.Date.class) {
            return (T) new java.sql.Date(COUNTER.getAndIncrement());
        } else if (clazz == java.sql.Time.class) {
            return (T) new java.sql.Time(COUNTER.getAndIncrement());
        } else if (clazz == java.sql.Timestamp.class) {
            return (T) new java.sql.Timestamp(COUNTER.getAndIncrement());
        } else if (clazz == java.util.Date.class) {
            return (T) new java.util.Date(COUNTER.getAndIncrement());
        } else if (clazz == List.class) {
            return (T) new ArrayList<>();
        }
        try {
            return clazz.newInstance();
        } catch (Exception e) {
            if (MAKE_ACCESSIBLE) {
                Constructor<?>[] constructors = clazz.getDeclaredConstructors();
                // try 0 length constructors
                for (Constructor<?> c : constructors) {
                    if (c.getParameterTypes().length == 0) {
                        c.setAccessible(true);
                        try {
                            return clazz.newInstance();
                        } catch (Exception e2) {
                            // ignore
                        }
                    }
                }
                // try 1 length constructors
                for (Constructor<?> c : constructors) {
                    if (c.getParameterTypes().length == 1) {
                        c.setAccessible(true);
                        try {
                            return (T) c.newInstance(new Object[1]);
                        } catch (Exception e2) {
                            // ignore
                        }
                    }
                }
            }
            throw new RuntimeException("Exception trying to create "
                    + clazz.getName() + ": " + e, e);
        }
    }

    public static <T> boolean isSimpleType(Class<T> clazz) {
        if (Number.class.isAssignableFrom(clazz)) {
            return true;
        } else if (clazz == String.class) {
            return true;
        }
        return false;
    }

    public static Object convert(Object o, Class<?> targetType) {
        if (o == null) {
            return null;
        }
        Class<?> currentType = o.getClass();
        if (targetType.isAssignableFrom(currentType)) {
            return o;
        }
        if (targetType == String.class) {
            if (Clob.class.isAssignableFrom(currentType)) {
                Clob c = (Clob) o;
                try {
                    Reader r = c.getCharacterStream();
                    return IOUtils.readStringAndClose(r, -1);
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Error converting CLOB to String: " + e.toString(),
                            e);
                }
            }
            return o.toString();
        }
        if (Number.class.isAssignableFrom(currentType)) {
            Number n = (Number) o;
            if (targetType == Byte.class) {
                return n.byteValue();
            } else if (targetType == Short.class) {
                return n.shortValue();
            } else if (targetType == Integer.class) {
                return n.intValue();
            } else if (targetType == Long.class) {
                return n.longValue();
            } else if (targetType == Double.class) {
                return n.doubleValue();
            } else if (targetType == Float.class) {
                return n.floatValue();
            }
        }
        throw new RuntimeException("Can not convert the value " + o + " from "
                + currentType + " to " + targetType);
    }

    @SuppressWarnings("unchecked")
    public static <X> Class<X> getClass(X x) {
        return (Class<X>) x.getClass();
    }

}
