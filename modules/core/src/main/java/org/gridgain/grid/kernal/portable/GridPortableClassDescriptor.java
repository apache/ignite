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
    private List<List<Field>> fields;

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

            for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
                Field[] clsFields0 = c.getDeclaredFields();

                Arrays.sort(clsFields0, new Comparator<Field>() {
                    @Override public int compare(Field f1, Field f2) {
                        return f1.getName().compareTo(f2.getName());
                    }
                });

                List<Field> clsFields = new ArrayList<>(clsFields0.length);

                for (Field f : clsFields0) {
                    int mod = f.getModifiers();

                    if (!isStatic(mod) && !isTransient(mod)) {
                        f.setAccessible(true);

                        clsFields.add(f);
                    }
                }

                fields.add(clsFields);
            }

            Collections.reverse(fields);
        }
    }

    /**
     * @param writer Writer.
     * @throws GridPortableException In case of error.
     */
    public void write(Object obj, GridPortableWriterAdapter writer) throws GridPortableException {
        assert obj != null;
        assert writer != null;

        switch (mode) {
            case PORTABLE_EX:
                GridPortableEx portable = (GridPortableEx)obj;

                portable.writePortable(writer);

                writer.doWriteByte(OBJ);
                writer.doWriteInt(typeId);
                writer.doWriteInt(obj.hashCode());

                // Length.
                writer.reserve(4);

                writer.doWriteInt(-1); // TODO: Header handles.

                writer.flush();

                break;

            case PORTABLE:

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
