/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.swapspace;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.swapspace.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 *
 */
public class GridSwapSpaceManager extends GridManagerAdapter<GridSwapSpaceSpi> {
    /** */
    private GridMarshaller marsh;

    /**
     * @param ctx Grid kernal context.
     */
    public GridSwapSpaceManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getSwapSpaceSpi());
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        if (ctx.config().isDaemon())
            return;

        getSpi().setListener(new GridSwapSpaceSpiListener() {
            @Override public void onSwapEvent(int evtType, @Nullable String spaceName, @Nullable byte[] keyBytes) {
                if (ctx.event().isRecordable(evtType)) {
                    String msg = null;

                    switch (evtType) {
                        case EVT_SWAP_SPACE_DATA_READ: {
                            msg = "Swap space data read [space=" + spaceName + ']';

                            break;
                        }

                        case EVT_SWAP_SPACE_DATA_STORED: {
                            msg = "Swap space data stored [space=" + spaceName + ']';

                            break;
                        }

                        case EVT_SWAP_SPACE_DATA_REMOVED: {
                            msg = "Swap space data removed [space=" + spaceName + ']';

                            break;
                        }

                        case EVT_SWAP_SPACE_CLEARED: {
                            msg = "Swap space cleared [space=" + spaceName + ']';

                            break;
                        }

                        case EVT_SWAP_SPACE_DATA_EVICTED: {
                            msg = "Swap entry evicted [space=" + spaceName + ']';

                            break;
                        }

                        default: {
                            assert false : "Unknown event type: " + evtType;
                        }
                    }

                    ctx.event().record(new GridSwapSpaceEvent(ctx.discovery().localNode(), msg, evtType, spaceName));
                }

                // Always notify grid cache processor.
                if (evtType == EVT_SWAP_SPACE_DATA_EVICTED && spaceName != null) {
                    assert keyBytes != null;

                    // Cache cannot use default swap space.
                    ctx.cache().onEvictFromSwap(spaceName, keyBytes);
                }
            }
        });

        startSpi();

        marsh = ctx.config().getMarshaller();

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        if (ctx.config().isDaemon())
            return;

        getSpi().setListener(null);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws GridException {
        if (ctx.config().isDaemon())
            return;

        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * Reads value from swap.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @param ldr Class loader (optional).
     * @return Value.
     * @throws GridException If failed.
     */
    @Nullable public byte[] read(@Nullable String spaceName, GridSwapKey key, @Nullable ClassLoader ldr)
        throws GridException {
        assert key != null;

        try {
            return getSpi().read(spaceName, key, context(ldr));
        }
        catch (GridSpiException e) {
            throw new GridException("Failed to read from swap space [space=" + spaceName + ", key=" + key + ']', e);
        }
    }

    /**
     * Reads value from swap.
     *
     * @param spaceName Space name.
     * @param key Swap key.
     * @param ldr Class loader (optional).
     * @return Value.
     * @throws GridException If failed.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public <T> T readValue(@Nullable String spaceName, GridSwapKey key, @Nullable ClassLoader ldr)
        throws GridException {
        assert key != null;

        return unmarshal(read(spaceName, key, ldr), ldr);
    }

    /**
     * Writes value to swap.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @param val Value.
     * @param ldr Class loader (optional).
     * @throws GridException If failed.
     */
    public void write(@Nullable String spaceName, GridSwapKey key, byte[] val, @Nullable ClassLoader ldr)
        throws GridException {
        assert key != null;
        assert val != null;

        try {
            getSpi().store(spaceName, key, val, context(ldr));
        }
        catch (GridSpiException e) {
            throw new GridException("Failed to write to swap space [space=" + spaceName + ", key=" + key +
                ", valLen=" + val.length + ']', e);
        }
    }

    /**
     * Writes batch to swap.
     *
     * @param spaceName Space name.
     * @param batch Swapped entries.
     * @param ldr Class loader (optional).
     * @throws GridException If failed.
     */
    public <K, V> void writeAll(String spaceName, Map<GridSwapKey, byte[]> batch,
        @Nullable ClassLoader ldr) throws GridException {
        getSpi().storeAll(spaceName, batch, context(ldr));
    }

    /**
     * Writes value to swap.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @param val Value.
     * @param ldr Class loader (optional).
     * @throws GridException If failed.
     */
    public void write(@Nullable String spaceName, Object key, @Nullable Object val, @Nullable ClassLoader ldr)
        throws GridException {
        assert key != null;

        write(spaceName, new GridSwapKey(key), marshal(val), ldr);
    }

    /**
     * Removes value from swap.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @param c Optional closure that takes removed value and executes after actual
     *      removing. If there was no value in storage the closure is executed given
     *      {@code null} value as parameter.
     * @param ldr Class loader (optional).
     * @throws GridException If failed.
     */
    public void remove(@Nullable String spaceName, GridSwapKey key, @Nullable IgniteInClosure<byte[]> c,
        @Nullable ClassLoader ldr) throws GridException {
        assert key != null;

        try {
            getSpi().remove(spaceName, key, c, context(ldr));
        }
        catch (GridSpiException e) {
            throw new GridException("Failed to remove from swap space [space=" + spaceName + ", key=" + key + ']', e);
        }
    }

    /**
     * Removes value from swap.
     *
     * @param spaceName Space name.
     * @param keys Collection of keys.
     * @param c Optional closure that takes removed value and executes after actual
     *      removing. If there was no value in storage the closure is executed given
     *      {@code null} value as parameter.
     * @param ldr Class loader (optional).
     * @throws GridException If failed.
     */
    public void removeAll(@Nullable String spaceName, Collection<GridSwapKey> keys,
        IgniteBiInClosure<GridSwapKey, byte[]> c, @Nullable ClassLoader ldr) throws GridException {
        assert keys != null;

        try {
            getSpi().removeAll(spaceName, keys, c, context(ldr));
        }
        catch (GridSpiException e) {
            throw new GridException("Failed to remove from swap space [space=" + spaceName + ", " +
                "keysCnt=" + keys.size() + ']', e);
        }
    }

    /**
     * Removes value from swap.
     *
     * @param spaceName Space name.
     * @param key Key.
     * @param c Optional closure that takes removed value and executes after actual
     *      removing. If there was no value in storage the closure is executed given
     *      {@code null} value as parameter.
     * @param ldr Class loader (optional).
     * @throws GridException If failed.
     */
    public void remove(@Nullable String spaceName, Object key, @Nullable IgniteInClosure<byte[]> c,
        @Nullable ClassLoader ldr) throws GridException {
        assert key != null;

        remove(spaceName, new GridSwapKey(key), c, ldr);
    }

    /**
     * Gets size in bytes for swap space.
     *
     * @param spaceName Space name.
     * @return Swap size.
     * @throws GridException If failed.
     */
    public long swapSize(@Nullable String spaceName) throws GridException {
        try {
            return getSpi().size(spaceName);
        }
        catch (GridSpiException e) {
            throw new GridException("Failed to get swap size for space: " + spaceName, e);
        }
    }

    /**
     * Gets number of swap entries (keys).
     *
     * @param spaceName Space name.
     * @return Number of stored entries in swap space.
     * @throws GridException If failed.
     */
    public long swapKeys(@Nullable String spaceName) throws GridException {
        try {
            return getSpi().count(spaceName);
        }
        catch (GridSpiException e) {
            throw new GridException("Failed to get swap keys count for space: " + spaceName, e);
        }
    }

    /**
     * @param spaceName Space name.
     * @throws GridException If failed.
     */
    public void clear(@Nullable String spaceName) throws GridException {
        try {
            getSpi().clear(spaceName);
        }
        catch (GridSpiException e) {
            throw new GridException("Failed to clear swap space [space=" + spaceName + ']', e);
        }
    }

    /**
     * Gets iterator over space entries.
     *
     * @param spaceName Space name.
     * @return Iterator over space entries or {@code null} if space is unknown.
     * @throws GridSpiException If failed.
     */
    @Nullable public GridCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator(@Nullable String spaceName)
        throws GridException {
        try {
            GridSpiCloseableIterator<Map.Entry<byte[], byte[]>> it = getSpi().rawIterator(spaceName);

            return it == null ? null : new GridSpiCloseableIteratorWrapper<>(it);
        }
        catch (GridSpiException e) {
            throw new GridException("Failed to get iterator over swap space [space=" + spaceName + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Nullable public GridCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator(@Nullable String spaceName, int part)
        throws GridException{
        try {
            GridSpiCloseableIterator<Map.Entry<byte[], byte[]>> it = getSpi().rawIterator(spaceName, part);

            return it == null ? new GridEmptyCloseableIterator<Map.Entry<byte[], byte[]>>() :
                new GridSpiCloseableIteratorWrapper<>(it);
        }
        catch (GridSpiException e) {
            throw new GridException("Failed to get iterator over swap space [space=" + spaceName + ']', e);
        }
    }

    /**
     * Gets iterator over space entries.
     *
     * @param spaceName Space name.
     * @param ldr Class loader.
     * @return Iterator over space entries or {@code null} if space is unknown.
     * @throws GridSpiException If failed.
     */
    @Nullable public <K> GridCloseableIterator<K> keysIterator(@Nullable String spaceName,
        @Nullable ClassLoader ldr) throws GridException {
        try {
            GridSpiCloseableIterator<K> it = getSpi().keyIterator(spaceName, context(ldr));

            return it == null ? null : new GridSpiCloseableIteratorWrapper<>(it);
        }
        catch (GridSpiException e) {
            throw new GridException("Failed to get iterator over swap space [space=" + spaceName + ']', e);
        }
    }

    /**
     * @param swapBytes Swap bytes to unmarshal.
     * @param ldr Class loader.
     * @return Unmarshalled value.
     * @throws GridException If failed.
     */
    @SuppressWarnings({"unchecked"})
    private <T> T unmarshal(byte[] swapBytes, @Nullable ClassLoader ldr) throws GridException {
        if (swapBytes == null)
            return null;

        return marsh.unmarshal(swapBytes, ldr != null ? ldr : U.gridClassLoader());
    }

    /**
     * Marshals object.
     *
     * @param obj Object to marshal.
     * @return Marshalled array.
     * @throws GridException If failed.
     */
    private byte[] marshal(Object obj) throws GridException {
        return ctx.config().getMarshaller().marshal(obj);
    }

    /**
     * @param clsLdr Class loader.
     * @return Swap context.
     */
    private GridSwapContext context(@Nullable ClassLoader clsLdr) {
        GridSwapContext ctx = new GridSwapContext();

        ctx.classLoader(clsLdr != null ? clsLdr : U.gridClassLoader());

        return ctx;
    }
}
