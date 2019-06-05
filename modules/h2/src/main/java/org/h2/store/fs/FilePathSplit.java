/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.List;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;
import org.h2.util.New;

/**
 * A file system that may split files into multiple smaller files.
 * (required for a FAT32 because it only support files up to 2 GB).
 */
public class FilePathSplit extends FilePathWrapper {

    private static final String PART_SUFFIX = ".part";

    @Override
    protected String getPrefix() {
        return getScheme() + ":" + parse(name)[0] + ":";
    }

    @Override
    public FilePath unwrap(String fileName) {
        return FilePath.get(parse(fileName)[1]);
    }

    @Override
    public boolean setReadOnly() {
        boolean result = false;
        for (int i = 0;; i++) {
            FilePath f = getBase(i);
            if (f.exists()) {
                result = f.setReadOnly();
            } else {
                break;
            }
        }
        return result;
    }

    @Override
    public void delete() {
        for (int i = 0;; i++) {
            FilePath f = getBase(i);
            if (f.exists()) {
                f.delete();
            } else {
                break;
            }
        }
    }

    @Override
    public long lastModified() {
        long lastModified = 0;
        for (int i = 0;; i++) {
            FilePath f = getBase(i);
            if (f.exists()) {
                long l = f.lastModified();
                lastModified = Math.max(lastModified, l);
            } else {
                break;
            }
        }
        return lastModified;
    }

    @Override
    public long size() {
        long length = 0;
        for (int i = 0;; i++) {
            FilePath f = getBase(i);
            if (f.exists()) {
                length += f.size();
            } else {
                break;
            }
        }
        return length;
    }

    @Override
    public ArrayList<FilePath> newDirectoryStream() {
        List<FilePath> list = getBase().newDirectoryStream();
        ArrayList<FilePath> newList = New.arrayList();
        for (FilePath f : list) {
            if (!f.getName().endsWith(PART_SUFFIX)) {
                newList.add(wrap(f));
            }
        }
        return newList;
    }

    @Override
    public InputStream newInputStream() throws IOException {
        InputStream input = getBase().newInputStream();
        for (int i = 1;; i++) {
            FilePath f = getBase(i);
            if (f.exists()) {
                InputStream i2 = f.newInputStream();
                input = new SequenceInputStream(input, i2);
            } else {
                break;
            }
        }
        return input;
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        ArrayList<FileChannel> list = New.arrayList();
        list.add(getBase().open(mode));
        for (int i = 1;; i++) {
            FilePath f = getBase(i);
            if (f.exists()) {
                list.add(f.open(mode));
            } else {
                break;
            }
        }
        FileChannel[] array = list.toArray(new FileChannel[0]);
        long maxLength = array[0].size();
        long length = maxLength;
        if (array.length == 1) {
            long defaultMaxLength = getDefaultMaxLength();
            if (maxLength < defaultMaxLength) {
                maxLength = defaultMaxLength;
            }
        } else {
            if (maxLength == 0) {
                closeAndThrow(0, array, array[0], maxLength);
            }
            for (int i = 1; i < array.length - 1; i++) {
                FileChannel c = array[i];
                long l = c.size();
                length += l;
                if (l != maxLength) {
                    closeAndThrow(i, array, c, maxLength);
                }
            }
            FileChannel c = array[array.length - 1];
            long l = c.size();
            length += l;
            if (l > maxLength) {
                closeAndThrow(array.length - 1, array, c, maxLength);
            }
        }
        return new FileSplit(this, mode, array, length, maxLength);
    }

    private long getDefaultMaxLength() {
        return 1L << Integer.decode(parse(name)[0]).intValue();
    }

    private void closeAndThrow(int id, FileChannel[] array, FileChannel o,
            long maxLength) throws IOException {
        String message = "Expected file length: " + maxLength + " got: " +
                o.size() + " for " + getName(id);
        for (FileChannel f : array) {
            f.close();
        }
        throw new IOException(message);
    }

    @Override
    public OutputStream newOutputStream(boolean append) throws IOException {
        return new FileChannelOutputStream(open("rw"), append);
    }

    @Override
    public void moveTo(FilePath path, boolean atomicReplace) {
        FilePathSplit newName = (FilePathSplit) path;
        for (int i = 0;; i++) {
            FilePath o = getBase(i);
            if (o.exists()) {
                o.moveTo(newName.getBase(i), atomicReplace);
            } else {
                break;
            }
        }
    }

    /**
     * Split the file name into size and base file name.
     *
     * @param fileName the file name
     * @return an array with size and file name
     */
    private String[] parse(String fileName) {
        if (!fileName.startsWith(getScheme())) {
            DbException.throwInternalError(fileName + " doesn't start with " + getScheme());
        }
        fileName = fileName.substring(getScheme().length() + 1);
        String size;
        if (fileName.length() > 0 && Character.isDigit(fileName.charAt(0))) {
            int idx = fileName.indexOf(':');
            size = fileName.substring(0, idx);
            try {
                fileName = fileName.substring(idx + 1);
            } catch (NumberFormatException e) {
                // ignore
            }
        } else {
            size = Long.toString(SysProperties.SPLIT_FILE_SIZE_SHIFT);
        }
        return new String[] { size, fileName };
    }

    /**
     * Get the file name of a part file.
     *
     * @param id the part id
     * @return the file name including the part id
     */
    FilePath getBase(int id) {
        return FilePath.get(getName(id));
    }

    private String getName(int id) {
        return id > 0 ? getBase().name + "." + id + PART_SUFFIX : getBase().name;
    }

    @Override
    public String getScheme() {
        return "split";
    }

}

/**
 * A file that may be split into multiple smaller files.
 */
class FileSplit extends FileBase {

    private final FilePathSplit file;
    private final String mode;
    private final long maxLength;
    private FileChannel[] list;
    private long filePointer;
    private long length;

    FileSplit(FilePathSplit file, String mode, FileChannel[] list, long length,
            long maxLength) {
        this.file = file;
        this.mode = mode;
        this.list = list;
        this.length = length;
        this.maxLength = maxLength;
    }

    @Override
    public void implCloseChannel() throws IOException {
        for (FileChannel c : list) {
            c.close();
        }
    }

    @Override
    public long position() {
        return filePointer;
    }

    @Override
    public long size() {
        return length;
    }

    @Override
    public synchronized int read(ByteBuffer dst, long position)
            throws IOException {
        int len = dst.remaining();
        if (len == 0) {
            return 0;
        }
        len = (int) Math.min(len, length - position);
        if (len <= 0) {
            return -1;
        }
        long offset = position % maxLength;
        len = (int) Math.min(len, maxLength - offset);
        FileChannel channel = getFileChannel(position);
        return channel.read(dst, offset);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int len = dst.remaining();
        if (len == 0) {
            return 0;
        }
        len = (int) Math.min(len, length - filePointer);
        if (len <= 0) {
            return -1;
        }
        long offset = filePointer % maxLength;
        len = (int) Math.min(len, maxLength - offset);
        FileChannel channel = getFileChannel(filePointer);
        channel.position(offset);
        len = channel.read(dst);
        filePointer += len;
        return len;
    }

    @Override
    public FileChannel position(long pos) {
        filePointer = pos;
        return this;
    }

    private FileChannel getFileChannel(long position) throws IOException {
        int id = (int) (position / maxLength);
        while (id >= list.length) {
            int i = list.length;
            FileChannel[] newList = new FileChannel[i + 1];
            System.arraycopy(list, 0, newList, 0, i);
            FilePath f = file.getBase(i);
            newList[i] = f.open(mode);
            list = newList;
        }
        return list[id];
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        if (newLength >= length) {
            return this;
        }
        filePointer = Math.min(filePointer, newLength);
        int newFileCount = 1 + (int) (newLength / maxLength);
        if (newFileCount < list.length) {
            // delete some of the files
            FileChannel[] newList = new FileChannel[newFileCount];
            // delete backwards, so that truncating is somewhat transactional
            for (int i = list.length - 1; i >= newFileCount; i--) {
                // verify the file is writable
                list[i].truncate(0);
                list[i].close();
                try {
                    file.getBase(i).delete();
                } catch (DbException e) {
                    throw DbException.convertToIOException(e);
                }
            }
            System.arraycopy(list, 0, newList, 0, newList.length);
            list = newList;
        }
        long size = newLength - maxLength * (newFileCount - 1);
        list[list.length - 1].truncate(size);
        this.length = newLength;
        return this;
    }

    @Override
    public void force(boolean metaData) throws IOException {
        for (FileChannel c : list) {
            c.force(metaData);
        }
    }

    @Override
    public int write(ByteBuffer src, long position) throws IOException {
        if (position >= length && position > maxLength) {
            // may need to extend and create files
            long oldFilePointer = position;
            long x = length - (length % maxLength) + maxLength;
            for (; x < position; x += maxLength) {
                if (x > length) {
                    // expand the file size
                    position(x - 1);
                    write(ByteBuffer.wrap(new byte[1]));
                }
                position = oldFilePointer;
            }
        }
        long offset = position % maxLength;
        int len = src.remaining();
        FileChannel channel = getFileChannel(position);
        int l = (int) Math.min(len, maxLength - offset);
        if (l == len) {
            l = channel.write(src, offset);
        } else {
            int oldLimit = src.limit();
            src.limit(src.position() + l);
            l = channel.write(src, offset);
            src.limit(oldLimit);
        }
        length = Math.max(length, position + l);
        return l;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (filePointer >= length && filePointer > maxLength) {
            // may need to extend and create files
            long oldFilePointer = filePointer;
            long x = length - (length % maxLength) + maxLength;
            for (; x < filePointer; x += maxLength) {
                if (x > length) {
                    // expand the file size
                    position(x - 1);
                    write(ByteBuffer.wrap(new byte[1]));
                }
                filePointer = oldFilePointer;
            }
        }
        long offset = filePointer % maxLength;
        int len = src.remaining();
        FileChannel channel = getFileChannel(filePointer);
        channel.position(offset);
        int l = (int) Math.min(len, maxLength - offset);
        if (l == len) {
            l = channel.write(src);
        } else {
            int oldLimit = src.limit();
            src.limit(src.position() + l);
            l = channel.write(src);
            src.limit(oldLimit);
        }
        filePointer += l;
        length = Math.max(length, filePointer);
        return l;
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        return list[0].tryLock(position, size, shared);
    }

    @Override
    public String toString() {
        return file.toString();
    }

}
