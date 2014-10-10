// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design;

import org.gridgain.grid.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.resources.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridAsyncSupportAdapter<T extends GridAsyncSupportAdapter> implements GridAsyncSupport<T> {
    /** Marshaller. */
    @GridMarshallerResource
    private GridMarshaller m;

    /** Async flag. */
    private boolean async;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public T enableAsync() {
        try {
            GridAsyncSupportAdapter<T> clone;

            if (this instanceof Cloneable) {
                clone = (T)clone();
            }
            else {
                byte[] b = m.marshal(this);

                clone = m.unmarshal(b, this.getClass().getClassLoader());
            }

            clone.async = true;

            return (T)clone;
        }
        catch (CloneNotSupportedException e) {
            throw new GridRuntimeException(e);
        }
    }

    @Override public boolean isAsync() {
        return false; // TODO: CODE: implement.
    }

    @Override public <R> GridFuture<R> future() {
        return null; // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
