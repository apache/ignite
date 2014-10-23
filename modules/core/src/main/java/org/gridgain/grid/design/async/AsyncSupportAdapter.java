// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.async;

import org.gridgain.grid.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.resources.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class AsyncSupportAdapter implements AsyncSupport {
    /** Marshaller. */
    @GridMarshallerResource
    private GridMarshaller m;

    /** Async flag. */
    private boolean async;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public AsyncSupport enableAsync() {
        try {
            AsyncSupportAdapter clone;

            if (this instanceof Cloneable) {
                clone = (AsyncSupportAdapter)clone();
            }
            else {
                try {
                    byte[] b = m.marshal(this);

                    clone = m.unmarshal(b, this.getClass().getClassLoader());
                }
                catch (GridException e) {
                    throw new GridRuntimeException(e);
                }
            }

            clone.async = true;

            return clone;
        }
        catch (CloneNotSupportedException e) {
            throw new GridRuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        return false; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override public <R> GridFuture<R> future() {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
