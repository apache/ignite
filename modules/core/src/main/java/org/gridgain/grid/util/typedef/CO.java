/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.typedef;

import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.lang.*;

/**
 * Defines {@code alias} for {@link org.gridgain.grid.lang.IgniteOutClosure} by extending it. Since Java doesn't provide type aliases
 * (like Scala, for example) we resort to these types of measures. This is intended to provide for more
 * concise code in cases when readability won't be sacrificed. For more information see {@link org.gridgain.grid.lang.IgniteOutClosure}.
 * @param <T> Type of the factory closure.
 * @see GridFunc
 * @see org.gridgain.grid.lang.IgniteOutClosure
 */
public interface CO<T> extends IgniteOutClosure<T> { /* No-op. */ }
