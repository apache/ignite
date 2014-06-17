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

    /** */
    private static final int TOTAL_LEN_POS = 9;

    /**
     * @param cls Class.
     * @return Class descriptor.
     * @throws GridPortableException In case of error.
     */
    public static GridPortableClassDescriptor get(Class<?> cls) throws GridPortableException {
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
    private List<FieldInfo> fields;

    /** */
    private byte[] hdr;

    /**
     * @param cls Class.
     */
    private GridPortableClassDescriptor(Class<?> cls) throws GridPortableException {
        assert cls != null;

        typeId = cls.getSimpleName().hashCode(); // TODO: should be taken from config

        // TODO: Other types.

        if (GridPortableEx.class.isAssignableFrom(GridPortableEx.class))
            mode = Mode.PORTABLE_EX;
        else {
            mode = Mode.PORTABLE;

            fields = new ArrayList<>();

            Collection<String> names = new HashSet<>();
            Collection<Integer> hashCodes = new LinkedHashSet<>();
            Collection<T2<byte[], Field>> namedFields = null;

            int hdrLen = 4;

            for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
                for (Field f : c.getDeclaredFields()) {
                    int mod = f.getModifiers();

                    if (!isStatic(mod) && !isTransient(mod)) {
                        f.setAccessible(true);

                        String name = f.getName();

                        if (!names.add(name))
                            throw new GridPortableException("Two fields in class " + cls.getName() +
                                " have the same name: " + name);

                        if (hashCodes.add(name.hashCode())) {
                            hdrLen += 4;

                            fields.add(new FieldInfo(f, hdrLen));

                            hdrLen += 4;
                        }
                        else {
                            if (namedFields == null)
                                namedFields = new ArrayList<>();

                            namedFields.add(new T2<>(name.getBytes(), f)); // TODO: UTF-8
                        }
                    }
                }
            }

            for (T2<byte[], Field> t : namedFields) {
                byte[] name = t.get1();
                Field f = t.get2();

                hdrLen += name.length;

                fields.add(new FieldInfo(f, hdrLen));

                hdrLen += 4;
            }

            hdr = new byte[hdrLen];

            int off = 0;

            PRIM.writeInt(hdr, off, hashCodes.size());

            off += 4;

            for (Integer hashCode : hashCodes) {
                PRIM.writeInt(hdr, off, hashCode);

                off += 8;
            }

            PRIM.writeInt(hdr, off, namedFields != null ? namedFields.size() : -1);

            off += 4;

            if (namedFields != null) {
                for (T2<byte[], Field> t : namedFields) {
                    byte[] name = t.get1();

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
                writer = new GridPortableWriterAdapter(writer);

                writer.doWriteInt(obj.hashCode());

                // Length.
                writer.reserve(4);

                // Header handles are not supported for GridPortableEx.
                writer.doWriteInt(-1);

                ((GridPortableEx)obj).writePortable(writer);

                writer.flush();

                // Length.
                writer.writeCurrentSize(TOTAL_LEN_POS);

                break;

            case PORTABLE:
                writer.doWriteInt(obj.hashCode());

                // Length.
                writer.reserve(4);

                writer.doWriteInt(-1); // TODO: Header handles.
                writer.write(hdr);

                for (FieldInfo info : fields) {
                    writer.writeCurrentSize(info.offPos);

                    try {
                        writer.doWriteObject(info.field.get(obj));
                    }
                    catch (IllegalAccessException e) {
                        throw new GridPortableException("Failed to get value for field: " + info.field, e);
                    }
                }

                // Length.
                writer.writeCurrentSize(TOTAL_LEN_POS);

                break;

            default:
                assert false : "Invalid mode: " + mode;
        }
    }

    /** */
    private static class FieldInfo {
        /** */
        private final Field field;

        /** */
        private final int offPos;

        /**
         * @param field Field.
         * @param offPos Offset position.
         */
        private FieldInfo(Field field, int offPos) {
            this.field = field;
            this.offPos = offPos;
        }
    }

    /** */
    private enum Mode {
        // TODO: Others

        /** */
        PORTABLE_EX,

        /** */
        PORTABLE
    }
}
