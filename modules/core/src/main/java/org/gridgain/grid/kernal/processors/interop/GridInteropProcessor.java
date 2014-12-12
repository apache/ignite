/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.interop;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.processors.*;
import org.jetbrains.annotations.*;

/**
 * Interop processor.
 */
public interface GridInteropProcessor extends GridProcessor {
    /**
     * Release start latch.
     */
    public void releaseStart();

    /**
     * Await start on native side.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void awaitStart() throws IgniteCheckedException;

    /**
     * @return Environment pointer.
     */
    public long environmentPointer() throws IgniteCheckedException;

    /**
     * @return Grid name.
     */
    public String gridName();

    /**
     * Gets native wrapper for default Grid projection.
     *
     * @return Native compute wrapper.
     * @throws IgniteCheckedException If failed.
     */
    public GridInteropTarget projection() throws IgniteCheckedException;

    /**
     * Gets native wrapper for cache with the given name.
     *
     * @param name Cache name ({@code null} for default cache).
     * @return Native cache wrapper.
     * @throws IgniteCheckedException If failed.
     */
    public GridInteropTarget cache(@Nullable String name) throws IgniteCheckedException;

    /**
     * Stops grid.
     *
     * @param cancel Cancel flag.
     */
    public void close(boolean cancel);
}
