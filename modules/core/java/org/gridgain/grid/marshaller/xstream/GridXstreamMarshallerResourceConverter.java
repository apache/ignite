// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.marshaller.xstream;

import com.thoughtworks.xstream.converters.basic.*;
import org.gridgain.grid.marshaller.*;

/**
 * Converter that makes sure that grid resources don't get marshalled.
 *
 * @author @java.author
 * @version @java.version
 */
class GridXstreamMarshallerResourceConverter extends NullConverter {
    /** {@inheritDoc} */
    @Override public boolean canConvert(Class cls) {
        return GridMarshallerExclusions.isExcluded(cls);
    }
}
