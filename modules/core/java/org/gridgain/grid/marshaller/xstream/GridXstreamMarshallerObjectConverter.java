// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.marshaller.xstream;

import com.thoughtworks.xstream.converters.*;
import com.thoughtworks.xstream.io.*;

/**
 * Converter for {@link Object} class marshalling.
 *
 * @author @java.author
 * @version @java.version
 */
class GridXstreamMarshallerObjectConverter implements Converter {
    /** {@inheritDoc} */
    @Override public boolean canConvert(Class cls) {
        return cls.equals(Object.class);
    }

    /** {@inheritDoc} */
    @Override public void marshal(Object val, HierarchicalStreamWriter writer, MarshallingContext ctx) {
        writer.startNode(Object.class.getName());
        writer.endNode();
    }

    /** {@inheritDoc} */
    @Override public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext ctx) {
        return new Object();
    }
}
