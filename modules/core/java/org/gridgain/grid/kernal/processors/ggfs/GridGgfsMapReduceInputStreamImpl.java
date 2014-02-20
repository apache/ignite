// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * Map-reduce input stream implementation.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridGgfsMapReduceInputStreamImpl extends GridGgfsMapReduceInputStream {
    /** Path to the file. */
    protected final GridGgfsPath path;

    /** Delimiters. */
    private final byte[][] delims;

    /** Data manager. */
    private final GridGgfsDataManager data;

    /** File descriptor. */
    private final GridGgfsFileInfo fileInfo;

    /** Secondary file system input stream wrapper. */
    private final GridGgfsSecondaryInputStreamWrapper inWrapper;

    /** Local block keys. */
    private final List<Long> locKeys;

    /** Whether no delimiters are set and affinity must be used to divide blocks instead. */
    private final boolean noDelims;

    /** Max delimiter length. */
    private final int maxDelimLen;

    /** Close guard. */
    private boolean closed;

    /** Current block. */
    private Block curBlock;

    /** Current position. */
    private int curBlockPos;

    /** Semaphore restricting prefetch activities. */
    private final Semaphore prefetchSem;

    /** Last created block future. */
    private volatile BlockFuture lastFut;

    /** End of stream flag. */
    private boolean eos;

    /**
     * Constructor.
     *
     * @param ggfsCtx GGFS context.
     * @param path Path to the file.
     * @param delims Delimiters.
     * @param fileInfo File descriptor.
     * @param inWrapper Secondary file system input stream wrapper.
     * @param locKeys Local keys.
     */
    GridGgfsMapReduceInputStreamImpl(GridGgfsContext ggfsCtx, GridGgfsPath path, byte[][] delims,
        GridGgfsFileInfo fileInfo, GridGgfsSecondaryInputStreamWrapper inWrapper, List<Long> locKeys) {
        assert ggfsCtx != null;
        assert path != null;
        assert fileInfo != null;

        this.path = path;
        this.fileInfo = fileInfo;
        this.inWrapper = inWrapper;
        this.locKeys = locKeys;

        data = ggfsCtx.data();

        int prefetchBlocks = ggfsCtx.configuration().getPrefetchBlocks();

        // Always prefetch at least one block because we will need it when calculating delimiter positions at the end.
        prefetchSem = new Semaphore(prefetchBlocks <= 0 ? 1 : prefetchBlocks);

        // Process delimiters.
        if (delims == null || delims.length == 0) {
            this.delims = null;

            noDelims = true;
        }
        else {
            List<byte[]> delims0 = new ArrayList<>(delims.length);

            for (byte[] delim : delims) {
                if (delim != null && delim.length > 0) {
                    boolean duplicate = false;

                    for (byte[] other : delims0) {
                        if (Arrays.equals(other, delim)) {
                            duplicate = true;

                            break;
                        }
                    }

                    if (!duplicate)
                        delims0.add(delim);
                }
            }

            if (delims0.isEmpty()) {
                this.delims = null;

                noDelims = true;
            }
            else {
                this.delims = delims0.toArray(new byte[delims0.size()][]);

                noDelims = false;
            }
        }

        if (noDelims)
            maxDelimLen = 0;
        else {
            int maxDelimLen0 = 0;

            for (byte[] delim : delims) {
                if (delim.length > maxDelimLen0)
                    maxDelimLen0 = delim.length;
            }

            maxDelimLen = maxDelimLen0;
        }

        // If no blocks provided, we consider this stream as "ended".
        if (locKeys == null || locKeys.isEmpty())
            eos = true;
    }

    /** {@inheritDoc} */
    @Override public GridGgfsFileInfo fileInfo() {
        return fileInfo;
    }

    /** {@inheritDoc} */
    @Override public synchronized int read() throws IOException {
        byte[] buf = new byte[1];

        int read = read(buf, 0, 1);

        if (read == -1)
            return -1; // EOF.

        return buf[0] & 0xFF; // Cast to int and cut to *unsigned* byte value.
    }

    /** {@inheritDoc} */
    @Override public synchronized int read(byte[] b, int off, int len) throws IOException {
        A.notNull(b, "b");

        return read0(b, off, len);
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;

            if (inWrapper != null)
                inWrapper.close();
        }
    }

    /**
     * Read data from cache.
     *
     * @param buf Byte array to write data to.
     * @param off Offset.
     * @param len Length.
     * @return Number of bytes read.
     * @throws IOException In case of exception.
     */
    private int read0(byte[] buf, int off, int len) throws IOException {
        if (closed)
            throw new IOException("Failed to read data because the stream has been closed.");

        assert buf != null;

        if (off < 0 || len < 0 || buf.length < len + off)
            throw new IndexOutOfBoundsException("Invalid buffer boundaries [buf.length=" + buf.length + ", off=" +
                off + ", len=" + len + ']');

        if (len == 0)
            return 0; // Fully read done: read zero bytes correctly.

        try {
            if (eos)
                return -1;

            // Read the very first block.
            if (curBlock == null) {
                curBlock = new BlockFuture(null).get();

                curBlockPos = curBlock.skipBytes();
            }

            assert curBlock != null;

            int readBytes = 0; // How many bytes were read so far.
            int remOff = off; // Remaining offset.

            while (readBytes < len) {
                // If we exceeded block limit, "rewind" forward until correct position is obtained.
                while (curBlockPos >= curBlock.limit()) {
                    int prevLimit = curBlock.limit(); // How far we are.

                    curBlock = curBlock.nextBlock().get();

                    if (curBlock != null) {
                        curBlockPos -= prevLimit;

                        if (curBlockPos == 0)
                            curBlockPos = curBlock.skipBytes();
                    }
                    else
                        break;
                }

                if (curBlock == null) {
                    // No more blocks, we reached end of stream.
                    if (readBytes == 0)
                        readBytes = -1;

                    eos = true;

                    break;
                }

                // There is data to read. First we determine next delimiter position.
                Delimiter nextDelim = null;

                if (!noDelims)
                    nextDelim = curBlock.nextDelimiter(curBlockPos);

                // Now we know next delimiter position, determine length of data we can read on this iteration.
                int canReadLen = nextDelim != null && nextDelim.start() < curBlock.limit() ?
                    nextDelim.end() - curBlockPos + 1 : curBlock.limit() - curBlockPos;

                if (canReadLen > 0) {
                    // Apply offset.
                    if (remOff > 0) {
                        if (remOff >= canReadLen) {
                            // Not enough data in this iteration to apply the full offset.
                            remOff -= canReadLen;

                            if (curBlock.local())
                                curBlockPos += canReadLen;
                            else
                                // Once we reached delimiter or block end, rewind to the end of the block (remote only).
                                curBlockPos = -1;

                            continue;
                        }
                        else {
                            // Just move caret further.
                            curBlockPos += remOff;

                            canReadLen -= remOff;

                            remOff = 0;
                        }
                    }

                    assert canReadLen > 0;

                    int readLen = Math.min(canReadLen, len - readBytes);

                    if (nextDelim == null || nextDelim.end() < curBlock.data().length) {
                        // Read will be performed only on current block.
                        System.arraycopy(curBlock.data(), curBlockPos, buf, readBytes, readLen);

                        readBytes += readLen;
                    }
                    else {
                        assert nextDelim != null;

                        // Read will be performed on current block and next block(s).
                        assert curBlock.limit() == curBlock.data().length;

                        int curBlockReadLen = 0;

                        if (curBlockPos < curBlock.limit()) {
                            curBlockReadLen = Math.min(readLen, curBlock.limit() - curBlockPos);

                            System.arraycopy(curBlock.data(), curBlockPos, buf, readBytes, curBlockReadLen);
                        }

                        readBytes += curBlockReadLen;

                        //  And this is read from next blocks.
                        BlockFuture nextBlockFut = curBlock.nextBlock();
                        int remainder = Math.min(readLen - curBlockReadLen, nextDelim.end() - curBlock.limit() + 1);

                        while (remainder > 0) {
                            Block nextBlock = nextBlockFut.get();

                            assert nextBlock != null;

                            int nextBlockReadLen = Math.min(remainder, nextBlock.limit()); // How much can we read now.

                            System.arraycopy(nextBlock.data(), 0, buf, readBytes, nextBlockReadLen);

                            readBytes += nextBlockReadLen;
                            remainder -= nextBlockReadLen;

                            nextBlockFut = nextBlock.nextBlock();
                        }

                        assert remainder == 0;
                    }

                    if (curBlock.local())
                        curBlockPos += readLen;
                    else
                        // Once we reached delimiter or block end, rewind to the end of the block (remote only).
                        curBlockPos = curBlock.limit();
                }
                else
                    assert false;
            }

            return readBytes;
        }
        catch (GridException e) {
            U.closeQuiet(this);

            throw new IOException("Failed to read data due to unexpected exception.", e);
        }
    }

    /**
     * Get all delimiter groups within the given byte array.
     *
     * @param prevData Data from the previous blocks.
     * @param data Data.
     * @param nextData Data from the next blocks.
     * @return List of delimiters.
     */
    private List<DelimiterGroup> delimiters(@Nullable byte[][] prevData, byte[] data, @Nullable byte[][] nextData ) {
        assert data != null;
        assert delims != null && delims.length > 0;

        List<DelimiterGroup> res = new ArrayList<>();

        int idx = 0;

        if (prevData != null) {
            for (byte[] prevDataChunk : prevData) {
                assert prevDataChunk != null;

                idx -= prevDataChunk.length;
            }
        }

        int nextDataLen = 0;

        if (nextData != null) {
            for (byte[] nextDataChunk : nextData)
                nextDataLen += nextDataChunk.length;
        }

        while (idx < data.length + nextDataLen) {
            DelimiterGroup nextDelim = delimiter(prevData, data, nextData, idx);

            if (nextDelim != null) {
                res.add(nextDelim);

                idx = nextDelim.end() + 1;
            }
            else
                break;
        }

        return res;
    }

    /**
     * Get next delimiter group.
     *
     * @param prevData Previous data chunks.
     * @param data Data.
     * @param nextData Next data chunks.
     * @param startIdx Start index. Might be negative in case we check previous data.
     * @return Delimiter group.
     */
    private DelimiterGroup delimiter(@Nullable byte[][] prevData, byte[] data,
        @Nullable byte[][] nextData, int startIdx) {
        assert data != null;
        assert delims != null && delims.length > 0;

        // Calculate next data length.
        int nextDataLen = 0;

        if (nextData != null) {
            for (byte[] nextDataChunk : nextData)
                nextDataLen += nextDataChunk.length;
        }

        // Handle special case when index points to the end.
        if (startIdx == nextDataLen + data.length)
            return null;

        assert startIdx < nextDataLen + data.length;

        // Determine previous data length (if any) which will be used to correct delimiter position.
        int prevDataLen = 0;

        if (prevData != null) {
            for (byte[] prevDataChunk : prevData)
                prevDataLen += prevDataChunk.length;
        }

        // Now we determine the block where we must start.
        int curPos = prevDataLen + startIdx;

        assert curPos >= 0 && curPos <= prevDataLen + data.length + nextDataLen;

        int remaining = curPos;

        int curChunkIdx = 0;
        int curChunkPos = 0;

        int totalPrevChunks = prevData != null ? prevData.length : 0;
        int totalNextChunks = nextData != null ? nextData.length : 0;

        for (int i = 0; i <= totalPrevChunks + totalNextChunks; i++) {
            curChunkIdx = i;
            curChunkPos = 0;

            // Determine current chunk.
            byte[] curChunk = (i < totalPrevChunks) ? prevData[i] : (i == totalPrevChunks) ? data :
                nextData[i - totalPrevChunks - 1];

            if (remaining < curChunk.length) {
                curChunkPos = remaining;

                break;
            }
            else
                remaining -= curChunk.length;
        }

        // Now as we found start position, iterate over remaining blocks looking for delimiters.
        List<Delimiter> found = null; // Found sequential delimiters.
        Map<Integer, Integer> map = null;

        boolean stop = false; // Stop flag.

        while (curChunkIdx <= totalPrevChunks + totalNextChunks && !stop) {
            byte[] curChunk = (curChunkIdx < totalPrevChunks) ? prevData[curChunkIdx] :
                (curChunkIdx == totalPrevChunks) ? data : nextData[curChunkIdx - totalPrevChunks - 1];

            assert curChunk != null;

            for (int i = curChunkPos; i < curChunk.length; i++) {
                boolean delimByte = false; // Whether current byte related to any delimiter.

                for (int j = 0; j < delims.length; j++) {
                    byte[] delim = delims[j];

                    assert delim.length > 0 : "Empty delimiters are not allowed.";

                    int pos = map != null && map.containsKey(j) ? map.get(j) : 0;

                    if (curChunk[i] == delim[pos]) {
                        if (delim.length == pos + 1) {
                            // Ok, we found matching delimiter.
                            if (found == null)
                                found = new ArrayList<>(1);

                            found.add(new Delimiter(curPos - delim.length + 1 - prevDataLen, curPos - prevDataLen));

                            map = null; // Clear all data of previously found delimiters.

                            delimByte = true;

                            break;
                        }
                        else {
                            if (map == null) {
                                map = new HashMap<>(1, 0.75f);

                                map.put(j, 1);
                            }
                            else
                                map.put(j, pos + 1);

                            delimByte = true;
                        }
                    }
                    else {
                        if (curChunk[i] == delim[0]) {
                            // Sequence is broken for the given delimiter, but current byte is it's head byte.
                            if (map == null) {
                                map = new HashMap<>(1, 0.75f);

                                map.put(j, 1);
                            }
                            else {
                                // Handle special case: all resolved delimiter bytes are equal.
                                int curRslvdLen = map.get(j);

                                for (int k = 1; k < curRslvdLen; k++) {
                                    if (curChunk[i] != delim[k]) {
                                        map.put(j, 1);

                                        break;
                                    }
                                }
                            }

                            delimByte = true;
                        }
                        else if (map != null)
                            map.remove(j);
                    }
                }

                if (found != null && !found.isEmpty() && !delimByte) {
                    // Sequential delimiter sequence is broken, stop.
                    stop = true;

                    break;
                }

                curPos++;
            }

            // Finished processing this chunk.
            curChunkIdx += 1;
            curChunkPos = 0;
        }

        return found != null ? new DelimiterGroup(data.length, found) : null;
    }

    /**
     * Block future.
     */
    private class BlockFuture extends GridFutureAdapter<Block> {
        /** Lock for various purposes. */
        private final Lock lock = new ReentrantLock();

        /** Process guard. */
        private final AtomicBoolean procGuard = new AtomicBoolean();

        /** Previous block initiated fetching of this one. */
        private Block prevBlock;

        /** Whether this future holds semaphore permit. */
        private boolean holdsSem;

        /**
         * {@link Externalizable} support.
         */
        public BlockFuture() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param prevBlock Previous block initiated fetching of this one.
         */
        private BlockFuture(@Nullable Block prevBlock) {
            this.prevBlock = prevBlock;
        }

        /** {@inheritDoc} */
        @Override public Block get() throws GridException {
            process();

            if (releasePrefetch()) {
                // Enable prefetch of the last block.
                BlockFuture nextBlockFut = lastFut;

                while (true) {
                    if (nextBlockFut.isDone()) {
                        // Go further.
                        Block nextBlock = nextBlockFut.get();

                        if (nextBlock != null)
                            nextBlockFut = nextBlock.nextBlock();
                        else
                            break;
                    }
                    // This future is not done => prefetch candidate.
                    else {
                        if (nextBlockFut.acquirePrefetch())
                            nextBlockFut.process();

                        break;
                    }
                }
            }

            return super.get();
        }

        /**
         * Run actual data block retrieval.
         */
        private void process() {
            if (procGuard.compareAndSet(false, true)) {
                try {
                    final long idx;

                    if (prevBlock == null) {
                        // Processing the very first block.
                        assert !locKeys.isEmpty();

                        idx = locKeys.get(0);
                    }
                    else {
                        // Processing not the very first block.
                        if (prevBlock.continuation()) {
                            // This is continuation, so we need to increment index of the previous block.
                            if (prevBlock.index() + 1 < fileInfo.blocksCount())
                                // Ok, block should exist.
                                idx = prevBlock.index() + 1;
                            else {
                                // Out of bounds, return null.
                                onDone();

                                return;
                            }
                        }
                        else {
                            // This is not continuation, so we look for the nearest next local block.
                            long nextLocIdx = -1;

                            for (long locIdx : locKeys) {
                                if (locIdx > prevBlock.index()) {
                                    nextLocIdx = locIdx;

                                    break;
                                }
                            }

                            if (nextLocIdx != -1)
                                // We found the next local block.
                                idx = nextLocIdx;
                            else {
                                // Next local block is not found, this is the end.
                                onDone();

                                return;
                            }
                        }
                    }

                    // Get data from cache.
                    GridFuture<byte[]> rawFut = data.dataBlock(fileInfo, path, idx, inWrapper);

                    processDataFuture(idx, rawFut);
                }
                catch (GridException e) {
                    onDone(e);
                }
            }
        }

        /**
         * Process future which queries data cache.
         *
         * @param idx Index of fetched block.
         * @param rawFut Raw future.
         */
        private void processDataFuture(final long idx, GridFuture<byte[]> rawFut) {
            rawFut.syncNotify(false);

            rawFut.listenAsync(new CI1<GridFuture<byte[]>>() {
                @Override public void apply(GridFuture<byte[]> rawFut) {
                    try {
                        byte[] rawData = rawFut.get();

                        if (rawData != null) {
                            List<DelimiterGroup> delimPoss = null;

                            boolean delimBeforeHead = false; // Whether there is a delimiter just before block head.

                            if (!noDelims) {
                                // Collect enough data from the next blocks.
                                List<byte[]> nextDataChunks = new ArrayList<>(maxDelimLen / fileInfo.blockSize() + 1);

                                int nextDataChunksLen = 0;

                                long nextIdx = idx + 1;

                                while (nextDataChunksLen < maxDelimLen - 1 && nextIdx < fileInfo.blocksCount()) {
                                    byte[] nextDataChunk = data.dataBlock(fileInfo, path, nextIdx, inWrapper).get();

                                    if (nextDataChunk == null)
                                        throw new GridException("Data block was not found [path=" + path +
                                            ", blockIdx=" + nextIdx + ']');

                                    nextDataChunks.add(nextDataChunk);

                                    nextDataChunksLen += nextDataChunk.length;

                                    nextIdx += 1;
                                }

                                byte[][] nextData = nextDataChunks.toArray(new byte[nextDataChunks.size()][]);

                                // This is very important piece of code. The idea here is that for each new block we
                                // have to go backward checking for previous delimiters. The tricky moment here,
                                // is that we must go backwards until the first found delimiter is located with
                                // [maxDelimLen -1] offset. Consider the following case:
                                // 1. Delimiters: [1,2,3] and [3,2,1]
                                // 2. Data: block0=[0,0,0,0,1,2,3,2], block1=[1,0,0,0,1,2,3,0]
                                // 3. maxDelimLen = 3.
                                // In this case we simply go back for 3 bytes, we will find the delimiter [3,2,1] and
                                // will think that the first byte in the block1 must be skipped. By this is incorrect,
                                // because [3,2,1] is not delimiter. [3] - id previous delimiter ending,
                                // and [2,1] - is business data.
                                List<DelimiterGroup> delimPoss0 = null;

                                List<byte[]> prevDataChunks = new ArrayList<>(1);

                                int prevDataChunksLen = 0;

                                long prevIdx = idx - 1;

                                while (prevIdx >= 0) {
                                    // At that point we assume the following:
                                    // 1. In case read is performed on local block, this is pretty fast operation,
                                    //     so no performance degradation here.
                                    // 2. Remote block read is extremely rare operation because blocks are grouped
                                    //    and usually delimiter length will be << than block length.
                                    byte[] prevDataChunk = data.dataBlock(fileInfo, path, prevIdx, inWrapper).get();

                                    if (prevDataChunk == null)
                                        throw new GridException("Data block was not found [path=" + path +
                                            ", blockIdx=" + prevIdx + ']');

                                    prevDataChunks.add(0, prevDataChunk);
                                    prevDataChunksLen += prevDataChunk.length;

                                    // Determine delimiter positions.
                                    delimPoss0 = delimiters(prevDataChunks.toArray(new byte[prevDataChunks.size()][]),
                                        rawData, nextData);

                                    // Find the nearest delimiter (to the current block) in previous data.
                                    int nearestPos = prevDataChunksLen * -1;
                                    int nearestDelimIdx = -1;

                                    for (int i = 0; i < delimPoss0.size(); i++) {
                                        DelimiterGroup delimPos = delimPoss0.get(i);

                                        if (delimPos.start() < 0) {
                                            if (delimPos.end() > nearestPos) {
                                                nearestDelimIdx = i;
                                                nearestPos = delimPos.end();
                                            }
                                        }
                                        else
                                            break; // No delimiters found in previous data.
                                    }

                                    // We can stop in the following cases:
                                    // 1. No nearest delimiter and distance between the current block and the
                                    //    previous data start is gte [maxDelimLen].
                                    // 2. Distance between the nearest delimiter and the current block is gte
                                    //    [maxDelimLen];
                                    // 3. Distance between the nearest delimiter and previous data start is gte
                                    //    [maxDelimLen].
                                    // Only in these cases we are sure that delimiters are either determined correctly
                                    // or their positions are no longer affect the block.

                                    if (nearestDelimIdx == -1) {
                                        // No delimiters found in previous data.
                                        if (prevDataChunksLen >= maxDelimLen)
                                            break;
                                    }
                                    else {
                                        // Delimiters was found.
                                        DelimiterGroup delimPos = delimPoss0.get(nearestDelimIdx);

                                        if ((delimPos.end() < 0 && (delimPos.end() + 1) * -1 >= maxDelimLen) ||
                                            prevDataChunksLen + delimPos.start() >= maxDelimLen)
                                            break;
                                    }

                                    prevIdx -= 1;
                                }

                                // Resolve delimiters if this is still needed.
                                if (delimPoss0 == null)
                                    delimPoss0 = delimiters(null, rawData, nextData);

                                // Remove unnecessary delimiters.
                                delimPoss = new ArrayList<>(delimPoss0.size());

                                for (DelimiterGroup delimPos : delimPoss0) {
                                    if ((delimPos.start() >= 0 && delimPos.start() < rawData.length) ||
                                        (delimPos.end() >= 0 && delimPos.end() < rawData.length) ||
                                        (delimPos.start() < 0 && delimPos.end() >= rawData.length))
                                        delimPoss.add(delimPos);

                                    for (Delimiter delim : delimPos.delimiters()) {
                                        if (delim.end() == -1)
                                            delimBeforeHead = true;
                                    }
                                }
                            }

                            // Determine whether this block is local.
                            boolean loc = locKeys.contains(idx);

                            // Determine how many bytes to skip in this block.
                            int skip = 0;

                            // Skip could potentially be applied to all blocks, but the very first one.
                            if (!noDelims && idx != 0) {
                                if (prevBlock != null && prevBlock.continuation()) {
                                    // This is continuation.
                                    if (!delimPoss.isEmpty() && delimPoss.get(0).start() <= 0)
                                        skip = delimPoss.get(0).firstRelated().end() + 1;
                                }
                                else if (!delimBeforeHead) {
                                    // This is not continuation.
                                    if (delimPoss.isEmpty())
                                        // No delimiters, skip the whole block.
                                        skip = rawData.length;
                                    else {
                                        int delimEnd = delimPoss.get(0).firstRelated().end();

                                        skip = Math.min(rawData.length, delimEnd + 1);
                                    }
                                }
                            }

                            // Determine continuation flag.
                            // For local block continuation is performed always, except of the case when the
                            // last related delimiter ends up directly on the block end and we don't skip the
                            // whole block.
                            // For remote block continuation is performed when there are either no delimiters,
                            // or the first delimiter go out of border.
                            boolean continuation = !noDelims && (loc ?
                                (skip != rawData.length && (delimPoss.isEmpty() ||
                                    delimPoss.get(delimPoss.size() - 1).lastRelated().end() != rawData.length - 1)) :
                                (delimPoss.isEmpty() || delimPoss.get(0).firstRelated().end() >= rawData.length));

                            int limit = rawData.length;

                            // In case this is remote block, we might want to set different limit.
                            if (!loc && !delimPoss.isEmpty()) {
                                Delimiter delim = delimPoss.get(0).firstRelated();

                                if (delim.end() < rawData.length - 1)
                                    limit = delim.end() + 1;
                            }

                            // Now we can create the block.
                            Block block = new Block(idx, rawData, loc, continuation, skip, limit, delimPoss);

                            // Set reference to the next block future.
                            lastFut = new BlockFuture(block);

                            block.nextBlock(lastFut);

                            // Initiate the next block future.
                            if (lastFut.acquirePrefetch())
                                lastFut.process();

                            // Mark future as done.
                            onDone(block);
                        }
                        else
                            throw new GridException("Data block was not found [path=" + path +
                                ", blockIdx=" + idx + ']');
                    }
                    catch (GridException e) {
                        onDone(e);
                    }
                }
            });
        }

        /**
         * Acquire prefetch semaphore permit.
         *
         * @return {@code True} in case permit was acquired, {@code false} otherwise.
         */
        private boolean acquirePrefetch() {
            lock.lock();

            try {
                if (!holdsSem && prefetchSem.tryAcquire()) {
                    holdsSem = true;

                    return true;
                }
                else
                    return false;
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Release prefetch semaphore permit.
         *
         * @return {@code True} in case permit was really released.
         */
        private boolean releasePrefetch() {
            lock.lock();

            try {
                if (holdsSem) {
                    prefetchSem.release();

                    holdsSem = false;

                    return true;
                }
                else
                    return false;
            }
            finally {
                lock.unlock();
            }
        }
    }

    /**
     * Convenient data block abstraction.
     */
    private static class Block {
        /** Block index. */
        private final long idx;

        /** Raw block data (with delimiters). */
        private final byte[] data;

        /** Whether this block is local. */
        private final boolean loc;

        /** Continuation flag. */
        private final boolean continuation;

        /** How many bytes to skip when reading data form that block. */
        private final int skipBytes;

        /** Maximum amount of bytes which could be read from that block. */
        private final int limit;

        /** Delimiters. */
        private final List<DelimiterGroup> delims;

        /** Next block. */
        private volatile BlockFuture nextBlock;

        /**
         * Constructor.
         *
         * @param idx Block index.
         * @param data Raw block data (with delimiters).
         * @param loc Whether this block is local.
         * @param continuation Continuation flag.
         * @param skipBytes How many bytes to skip when reading data form that block.
         * @param limit Maximum amount of bytes which could be read from that block.
         * @param delims Delimiters.
         */
        private Block(long idx, byte[] data, boolean loc, boolean continuation, int skipBytes, int limit,
            @Nullable List<DelimiterGroup> delims) {
            assert idx >= 0;
            assert limit > 0;

            this.idx = idx;
            this.data = data;
            this.loc = loc;
            this.continuation = continuation;
            this.skipBytes = skipBytes;
            this.limit = limit;
            this.delims = delims;
        }

        /**
         * @return Block index.
         */
        long index() {
            return idx;
        }

        /**
         * @return Data.
         */
        byte[] data() {
            return data;
        }

        /**
         * @return Whether this block is local.
         */
        boolean local() {
            return loc;
        }

        /**
         * @return Continuation flag.
         */
        boolean continuation() {
            return continuation;
        }

        /**
         * @return How many bytes to skip.
         */
        int skipBytes() {
            return skipBytes;
        }

        /**
         * @return Maximum amount of bytes which could be read from that block.
         */
        int limit() {
            return limit;
        }

        /**
         * @return Delimiters.
         */
        @Nullable List<DelimiterGroup> delimiters() {
            return delims;
        }

        /**
         * Get the next delimiter relative to the given position.
         *
         * @param pos Position.
         * @return Next delimiter or {@code null} in case no more delimiters can be found.
         */
        @Nullable Delimiter nextDelimiter(int pos) {
            if (!delims.isEmpty()) {
                for (DelimiterGroup delimGrp : delims) {
                    for (Delimiter delim : delimGrp.delimiters()) {
                        if (delim.start() >= pos)
                            return delim;
                    }
                }
            }

            return null;
        }

        /**
         * @return Next block.
         */
        private BlockFuture nextBlock() {
            return nextBlock;
        }

        /**
         * @param nextBlock Next block future.
         */
        private void nextBlock(BlockFuture nextBlock) {
            this.nextBlock = nextBlock;
        }
    }

    /**
     * Delimiter.
     */
    private static class Delimiter {
        /** Start. */
        private final int start;

        /** End. */
        private final int end;

        /**
         * Constructor,
         *
         * @param start Start.
         * @param end End.
         */
        private Delimiter(int start, int end) {
            this.start = start;
            this.end = end;
        }

        /**
         * @return Start.
         */
        private int start() {
            return start;
        }

        /**
         * @return End.
         */
        private int end() {
            return end;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Delimiter.class, this);
        }
    }

    /**
     * Group of sequential delimiters.
     */
    private static class DelimiterGroup {
        /** Size of analyzed block. */
        private final int blockSize;

        /** Delimiters. */
        private final List<Delimiter> delims;

        /** Start. */
        private final int start;

        /** End. */
        private final int end;

        /** First delimiter which relates to the block. */
        private Delimiter firstRelated;

        /** Last delimiter which relates to the block. */
        private Delimiter lastRelated;

        /**
         * Constructor.
         *
         * @param blockSize Size of analyzed block.
         * @param delims Delimiters.
         */
        private DelimiterGroup(int blockSize, List<Delimiter> delims) {
            assert blockSize > 0;
            assert delims != null && !delims.isEmpty();

            this.blockSize = blockSize;
            this.delims = delims;

            start = delims.get(0).start();
            end = delims.get(delims.size() - 1).end();

            Delimiter firstRelated0 = null;
            Delimiter lastRelated0 = null;

            for (Delimiter delim : delims) {
                if (relates(delim)) {
                    if (firstRelated0 == null)
                        firstRelated0 = delim;

                    lastRelated0 = delim;
                }
            }

            firstRelated = firstRelated0;
            lastRelated = lastRelated0;
        }

        /**
         * Check whether provided delimiter is located within the block.
         *
         * @param delim Delimiter.
         * @return {@code True} in case delimiter is located (at least partially) in the block.
         */
        private boolean relates(Delimiter delim) {
            return delim.start < 0 && delim.end >= 0 ||
                delim.start < blockSize && delim.end >= blockSize ||
                delim.start < 0 && delim.end >= blockSize ||
                delim.start >= 0 && delim.end < blockSize;
        }

        /**
         * @return Delimiters.
         */
        private Iterable<Delimiter> delimiters() {
            return delims;
        }

        /**
         * @return Start.
         */
        private int start() {
            return start;
        }

        /**
         * @return End.
         */
        private int end() {
            return end;
        }

        /**
         * @return First delimiter which relates to the block.
         */
        private Delimiter firstRelated() {
            return firstRelated;
        }

        /**
         * @return Last delimiter which relates to the block.
         */
        private Delimiter lastRelated() {
            return lastRelated;
        }

        /**
         * @return Length.
         */
        private int length() {
            return end - start + 1;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DelimiterGroup.class, this);
        }
    }
}
