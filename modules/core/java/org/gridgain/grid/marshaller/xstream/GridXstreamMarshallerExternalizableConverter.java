// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.marshaller.xstream;

import com.thoughtworks.xstream.converters.*;
import com.thoughtworks.xstream.converters.reflection.*;
import com.thoughtworks.xstream.core.util.*;
import com.thoughtworks.xstream.io.*;
import com.thoughtworks.xstream.mapper.*;
import java.io.*;
import java.lang.reflect.*;
import java.util.*;

/**
 * This converter is needed, because default XStream converter does not work
 * with non-public classes.
 *
 * @author @java.author
 * @version @java.version
 */
class GridXstreamMarshallerExternalizableConverter extends ExternalizableConverter {
    /** Invoker. */
    private SerializationMethodInvoker invoker = new SerializationMethodInvoker();

    /** Mapper. */
    private final Mapper mapper;

    /**
     * @param mapper Xstream mapper.
     */
    GridXstreamMarshallerExternalizableConverter(Mapper mapper) {
        super(mapper);

        this.mapper = mapper;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Object unmarshal(final HierarchicalStreamReader reader, final UnmarshallingContext ctx) {
        Class type = ctx.getRequiredType();

        try {
            Constructor ctor = type.getConstructor();

            // Override non-public access.
            ctor.setAccessible(true);

            final Externalizable obj = (Externalizable)ctor.newInstance();

            CustomObjectInputStream.StreamCallback callback = new CustomObjectInputStream.StreamCallback() {
                @Override public Object readFromStream() {
                    reader.moveDown();

                    Object item = ctx.convertAnother(obj, mapper.realClass(reader.getNodeName()));

                    reader.moveUp();

                    return item;
                }

                @Override public Map readFieldsFromStream() {
                    throw new UnsupportedOperationException();
                }

                @Override public void defaultReadObject() {
                    throw new UnsupportedOperationException();
                }

                @Override public void registerValidation(ObjectInputValidation val, int pri)
                    throws NotActiveException {
                    throw new NotActiveException("Stream is inactive.");
                }

                @Override public void close() {
                    throw new UnsupportedOperationException("Objects are not allowed to apply ObjectInput.close() " +
                        "from readExternal()");
                }
            };

            CustomObjectInputStream in = CustomObjectInputStream.getInstance(ctx, callback);

            obj.readExternal(in);

            Object res = invoker.callReadResolve(obj);

            in.popCallback();

            return res; 
        }
        catch (Exception e) {
            throw new ConversionException("Cannot construct " + type.getClass(), e);
        }        
    }
}
