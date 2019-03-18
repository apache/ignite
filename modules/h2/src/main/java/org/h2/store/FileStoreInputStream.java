/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.io.InputStream;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.tools.CompressTool;
import org.h2.util.Utils;

/**
 * An input stream that is backed by a file store.
 */
public class FileStoreInputStream extends InputStream {

    private FileStore store;
    private final Data page;
    private int remainingInBuffer;
    private final CompressTool compress;
    private boolean endOfFile;
    private final boolean alwaysClose;

    public FileStoreInputStream(FileStore store, DataHandler handler,
            boolean compression, boolean alwaysClose) {
        this.store = store;
        this.alwaysClose = alwaysClose;
        if (compression) {
            compress = CompressTool.getInstance();
        } else {
            compress = null;
        }
        page = Data.create(handler, Constants.FILE_BLOCK_SIZE);
        try {
            if (store.length() <= FileStore.HEADER_LENGTH) {
                close();
            } else {
                fillBuffer();
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, store.name);
        }
    }

    @Override
    public int available() {
        return remainingInBuffer <= 0 ? 0 : remainingInBuffer;
    }

    @Override
    public int read(byte[] buff) throws IOException {
        return read(buff, 0, buff.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        int read = 0;
        while (len > 0) {
            int r = readBlock(b, off, len);
            if (r < 0) {
                break;
            }
            read += r;
            off += r;
            len -= r;
        }
        return read == 0 ? -1 : read;
    }

    private int readBlock(byte[] buff, int off, int len) throws IOException {
        fillBuffer();
        if (endOfFile) {
            return -1;
        }
        int l = Math.min(remainingInBuffer, len);
        page.read(buff, off, l);
        remainingInBuffer -= l;
        return l;
    }

    private void fillBuffer() throws IOException {
        if (remainingInBuffer > 0 || endOfFile) {
            return;
        }
        page.reset();
        store.openFile();
        if (store.length() == store.getFilePointer()) {
            close();
            return;
        }
        store.readFully(page.getBytes(), 0, Constants.FILE_BLOCK_SIZE);
        page.reset();
        remainingInBuffer = page.readInt();
        if (remainingInBuffer < 0) {
            close();
            return;
        }
        page.checkCapacity(remainingInBuffer);
        // get the length to read
        if (compress != null) {
            page.checkCapacity(Data.LENGTH_INT);
            page.readInt();
        }
        page.setPos(page.length() + remainingInBuffer);
        page.fillAligned();
        int len = page.length() - Constants.FILE_BLOCK_SIZE;
        page.reset();
        page.readInt();
        store.readFully(page.getBytes(), Constants.FILE_BLOCK_SIZE, len);
        page.reset();
        page.readInt();
        if (compress != null) {
            int uncompressed = page.readInt();
            byte[] buff = Utils.newBytes(remainingInBuffer);
            page.read(buff, 0, remainingInBuffer);
            page.reset();
            page.checkCapacity(uncompressed);
            CompressTool.expand(buff, page.getBytes(), 0);
            remainingInBuffer = uncompressed;
        }
        if (alwaysClose) {
            store.closeFile();
        }
    }

    @Override
    public void close() {
        if (store != null) {
            try {
                store.close();
                endOfFile = true;
            } finally {
                store = null;
            }
        }
    }

    @Override
    protected void finalize() {
        close();
    }

    @Override
    public int read() throws IOException {
        fillBuffer();
        if (endOfFile) {
            return -1;
        }
        int i = page.readByte() & 0xff;
        remainingInBuffer--;
        return i;
    }

}
