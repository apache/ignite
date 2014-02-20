// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.typedef;

import org.gridgain.grid.util.lang.*;

/**
 * Defines {@code alias} for {@link GridInClosure3} by extending it. Since Java doesn't provide type aliases
 * (like Scala, for example) we resort to these types of measures. This is intended to provide for more
 * concise code in cases when readability won't be sacrificed. For more information see {@link GridInClosure3}.
 *
 * @author @java.author
 * @version @java.version
 * @param <E1> Type of the first parameter.
 * @param <E2> Type of the second parameter.
 * @param <E3> Type of the third parameter.
 * @see GridFunc
 * @see GridInClosure3
 */
public abstract class CI3<E1, E2, E3> extends GridInClosure3<E1, E2, E3> { /* No-op. */ }