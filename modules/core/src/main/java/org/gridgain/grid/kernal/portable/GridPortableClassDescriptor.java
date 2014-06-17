/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.portable;

import org.gridgain.grid.portable.*;
import org.jdk8.backport.*;

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
    private static final ConcurrentMap<Class<?>, GridPortableClassDescriptor> CACHE = new ConcurrentHashMap8<>(256);

    /** */
    private static final int TOTAL_LEN_POS = 9;

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

            GridPortableHeaderWriter hdrWriter = new GridPortableHeaderWriter();

            for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
                for (Field f : c.getDeclaredFields()) {
                    int mod = f.getModifiers();

                    if (!isStatic(mod) && !isTransient(mod)) {
                        f.setAccessible(true);

                        int offPos = hdrWriter.addField(f.getName());

                        fields.add(new FieldInfo(f, offPos));
                    }
                }
            }

            hdr = hdrWriter.header();
        }
    }

    /**
     * @param cls Class.
     * @return Class descriptor.
     * @throws GridPortableException In case of error.
     */
    static GridPortableClassDescriptor get(Class<?> cls) throws GridPortableException {
        assert cls != null;

        GridPortableClassDescriptor desc = CACHE.get(cls);

        if (desc == null) {
            GridPortableClassDescriptor old = CACHE.putIfAbsent(cls, desc = new GridPortableClassDescriptor(cls));

            if (old != null)
                desc = old;
        }

        return desc;
    }

    /**
     * @param obj Object.
     * @param writer Writer.
     * @throws GridPortableException In case of error.
     */
    void write(Object obj, GridPortableWriterImpl writer) throws GridPortableException {
        assert obj != null;
        assert writer != null;

        writer.doWriteByte(OBJ);
        writer.doWriteInt(typeId);

        switch (mode) {
            case PORTABLE_EX:
                writer.doWriteInt(obj.hashCode());

                // Length.
                writer.reserve(4);

                // Header handles are not supported for GridPortableEx.
                writer.doWriteInt(-1);

                writePortableEx((GridPortableEx)obj, writer);

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

    /**
     * @param obj Object.
     * @param writer Writer.
     * @throws GridPortableException In case of error.
     */
    private void writePortableEx(GridPortableEx obj, GridPortableWriterImpl writer) throws GridPortableException {
        assert obj != null;
        assert writer != null;

        GridPortableWriterImpl writer0 = new GridPortableWriterImpl(writer);

        obj.writePortable(writer0);

        writer0.flush();
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
