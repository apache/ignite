/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.portable;

import org.gridgain.grid.portable.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.jdk8.backport.*;
import sun.misc.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import static java.lang.reflect.Modifier.*;
import static org.gridgain.grid.kernal.portable.GridPortableMarshaller.*;

/**
 * Portable class descriptor.
 */
class GridPortableClassDescriptor {
    /** */
    protected static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    protected static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** */
    private static final GridPortablePrimitivesWriter PRIM = GridPortablePrimitivesWriter.get();

    /** */
    private static final ConcurrentMap<Class<?>, GridPortableClassDescriptor> CACHE = new ConcurrentHashMap8<>(256);

    /**
     * @param cls Class.
     * @return Class descriptor.
     */
    public static GridPortableClassDescriptor get(Class<?> cls) {
        assert cls != null;

        GridPortableClassDescriptor desc = CACHE.get(cls);

        if (desc == null) {
            GridPortableClassDescriptor old = CACHE.putIfAbsent(cls, desc = new GridPortableClassDescriptor(cls));

            if (old != null)
                desc = old;
        }

        return desc;
    }

    /** */
    private final Mode mode;

    /** */
    private final int typeId;

    /** */
    private List<T2<Field, Integer>> fields;

    /** */
    private byte[] hdr;

    /**
     * @param cls Class.
     */
    private GridPortableClassDescriptor(Class<?> cls) {
        assert cls != null;

        typeId = cls.getSimpleName().hashCode(); // TODO

        // TODO: Other types.

        if (GridPortableEx.class.isAssignableFrom(GridPortableEx.class))
            mode = Mode.PORTABLE_EX;
        else {
            mode = Mode.PORTABLE;

            fields = new ArrayList<>();

            Collection<T2<Field, Integer>> namedFields = new ArrayList<>();

            Collection<Integer> hashCodes = new LinkedHashSet<>();
            Collection<byte[]> names = null;

            int hdrLen = 8;

            for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
                for (Field f : c.getDeclaredFields()) {
                    int mod = f.getModifiers();

                    if (!isStatic(mod) && !isTransient(mod)) {
                        f.setAccessible(true);

                        String name = f.getName();

                        if (hashCodes.add(name.hashCode())) {
                            fields.add(f);

                            hdrLen += 8;
                        }
                        else {
                            namedFields.add(f);

                            byte[] nameArr = name.getBytes(); // TODO: UTF-8

                            if (names == null)
                                names = new ArrayList<>();

                            names.add(nameArr);

                            hdrLen += nameArr.length + 4;
                        }
                    }
                }
            }

            fields.addAll(namedFields);

            hdr = new byte[hdrLen];

            int off = 0;

            PRIM.writeInt(hdr, off, hashCodes.size());

            off += 4;

            for (Integer hashCode : hashCodes) {
                PRIM.writeInt(hdr, off, hashCode);

                off += 8;
            }

            PRIM.writeInt(hdr, off, names != null ? names.size() : -1);

            if (names != null) {
                for (byte[] name : names) {
                    UNSAFE.copyMemory(name, BYTE_ARR_OFF, hdr, BYTE_ARR_OFF + off, name.length);

                    off += name.length + 4;
                }
            }
        }
    }

    /**
     * @param writer Writer.
     * @throws GridPortableException In case of error.
     */
    public void write(Object obj, GridPortableWriterAdapter writer) throws GridPortableException {
        assert obj != null;
        assert writer != null;

        writer.doWriteByte(OBJ);
        writer.doWriteInt(typeId);

        switch (mode) {
            case PORTABLE_EX:
//                writer.doWriteInt(obj.hashCode());
//
//                // Length.
//                writer.reserve(4);
//
//                ((GridPortableEx)obj).writePortable(writer);
//
//                writer.doWriteInt(-1); // TODO: Header handles.
//
//                writer.flush();

                break;

            case PORTABLE:
                writer.doWriteInt(obj.hashCode());

                // Length.
                writer.reserve(4);

                writer.doWriteInt(-1); // TODO: Header handles.
                writer.doWriteByteArray(hdr);

                for (int i = 0; i < fields.size(); i++) {
                    Field f = fields.get(i);

                    writer.writeCurrentSize();

                    try {
                        writer.doWriteObject(f.get(obj));
                    }
                    catch (IllegalAccessException e) {
                        throw new GridPortableException("Failed to get value for field: " + f, e);
                    }
                }

                break;

            default:
                assert false : "Invalid mode: " + mode;
        }
    }

    /**
     * Types.
     */
    private enum Mode {
        // TODO: Others

        /** */
        PORTABLE_EX,

        /** */
        PORTABLE
    }
}
