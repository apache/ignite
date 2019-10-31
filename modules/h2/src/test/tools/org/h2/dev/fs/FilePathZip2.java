/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.store.fs.FakeFileChannel;
import org.h2.store.fs.FileBase;
import org.h2.store.fs.FileChannelInputStream;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FilePathDisk;
import org.h2.store.fs.FileUtils;
import org.h2.util.IOUtils;
import org.h2.util.New;

/**
 * This is a read-only file system that allows to access databases stored in a
 * .zip or .jar file. The problem of this file system is that data is always
 * accessed as a stream. This implementation allows to stack file systems.
 */
public class FilePathZip2 extends FilePath {

    /**
     * Register the file system.
     *
     * @return the instance
     */
    public static FilePathZip2 register() {
        FilePathZip2 instance = new FilePathZip2();
        FilePath.register(instance);
        return instance;
    }

    @Override
    public FilePathZip2 getPath(String path) {
        FilePathZip2 p = new FilePathZip2();
        p.name = path;
        return p;
    }

    @Override
    public void createDirectory() {
        // ignore
    }

    @Override
    public boolean createFile() {
        throw DbException.getUnsupportedException("write");
    }

    @Override
    public FilePath createTempFile(String suffix, boolean deleteOnExit,
            boolean inTempDir) throws IOException {
        if (!inTempDir) {
            throw new IOException("File system is read-only");
        }
        return new FilePathDisk().getPath(name).createTempFile(suffix,
                deleteOnExit, true);
    }

    @Override
    public void delete() {
        throw DbException.getUnsupportedException("write");
    }

    @Override
    public boolean exists() {
        try {
            String entryName = getEntryName();
            if (entryName.length() == 0) {
                return true;
            }
            ZipInputStream file = openZip();
            boolean result = false;
            while (true) {
                ZipEntry entry = file.getNextEntry();
                if (entry == null) {
                    break;
                }
                if (entry.getName().equals(entryName)) {
                    result = true;
                    break;
                }
                file.closeEntry();
            }
            file.close();
            return result;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public long lastModified() {
        return 0;
    }

    @Override
    public FilePath getParent() {
        int idx = name.lastIndexOf('/');
        return idx < 0 ? null : getPath(name.substring(0, idx));
    }

    @Override
    public boolean isAbsolute() {
        String fileName = translateFileName(name);
        return FilePath.get(fileName).isAbsolute();
    }

    @Override
    public FilePath unwrap() {
        return FilePath.get(name.substring(getScheme().length() + 1));
    }

    @Override
    public boolean isDirectory() {
        try {
            String entryName = getEntryName();
            if (entryName.length() == 0) {
                return true;
            }
            ZipInputStream file = openZip();
            boolean result = false;
            while (true) {
                ZipEntry entry = file.getNextEntry();
                if (entry == null) {
                    break;
                }
                String n = entry.getName();
                if (n.equals(entryName)) {
                    result = entry.isDirectory();
                    break;
                } else  if (n.startsWith(entryName)) {
                    if (n.length() == entryName.length() + 1) {
                        if (n.equals(entryName + "/")) {
                            result = true;
                            break;
                        }
                    }
                }
                file.closeEntry();
            }
            file.close();
            return result;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public boolean canWrite() {
        return false;
    }

    @Override
    public boolean setReadOnly() {
        return true;
    }

    @Override
    public long size() {
        try {
            String entryName = getEntryName();
            ZipInputStream file = openZip();
            long result = 0;
            while (true) {
                ZipEntry entry = file.getNextEntry();
                if (entry == null) {
                    break;
                }
                if (entry.getName().equals(entryName)) {
                    result = entry.getSize();
                    if (result == -1) {
                        result = 0;
                        while (true) {
                            long x = file.skip(16 * Constants.IO_BUFFER_SIZE);
                            if (x == 0) {
                                break;
                            }
                            result += x;
                        }
                    }
                    break;
                }
                file.closeEntry();
            }
            file.close();
            return result;
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    public ArrayList<FilePath> newDirectoryStream() {
        String path = name;
        try {
            if (path.indexOf('!') < 0) {
                path += "!";
            }
            if (!path.endsWith("/")) {
                path += "/";
            }
            ZipInputStream file = openZip();
            String dirName = getEntryName();
            String prefix = path.substring(0, path.length() - dirName.length());
            ArrayList<FilePath> list = New.arrayList();
            while (true) {
                ZipEntry entry = file.getNextEntry();
                if (entry == null) {
                    break;
                }
                String entryName = entry.getName();
                if (entryName.startsWith(dirName) && entryName.length() > dirName.length()) {
                    int idx = entryName.indexOf('/', dirName.length());
                    if (idx < 0 || idx >= entryName.length() - 1) {
                        list.add(getPath(prefix + entryName));
                    }
                }
                file.closeEntry();
            }
            file.close();
            return list;
        } catch (IOException e) {
            throw DbException.convertIOException(e, "listFiles " + path);
        }
    }

    @Override
    public FilePath toRealPath() {
        return this;
    }

    @Override
    public InputStream newInputStream() throws IOException {
        return new FileChannelInputStream(open("r"), true);
    }

    @Override
    public FileChannel open(String mode) throws IOException {
        String entryName = getEntryName();
        if (entryName.length() == 0) {
            throw new FileNotFoundException();
        }
        ZipInputStream in = openZip();
        while (true) {
            ZipEntry entry = in.getNextEntry();
            if (entry == null) {
                break;
            }
            if (entry.getName().equals(entryName)) {
                return new FileZip2(name, entryName, in, size());
            }
            in.closeEntry();
        }
        in.close();
        throw new FileNotFoundException(name);
    }

    @Override
    public OutputStream newOutputStream(boolean append) {
        throw DbException.getUnsupportedException("write");
    }

    @Override
    public void moveTo(FilePath newName, boolean atomicReplace) {
        throw DbException.getUnsupportedException("write");
    }

    private String getEntryName() {
        int idx = name.indexOf('!');
        String fileName;
        if (idx <= 0) {
            fileName = "";
        } else {
            fileName = name.substring(idx + 1);
        }
        fileName = fileName.replace('\\', '/');
        if (fileName.startsWith("/")) {
            fileName = fileName.substring(1);
        }
        return fileName;
    }

    private ZipInputStream openZip() throws IOException {
        String fileName = translateFileName(name);
        return new ZipInputStream(FileUtils.newInputStream(fileName));
    }

    private static String translateFileName(String fileName) {
        if (fileName.startsWith("zip2:")) {
            fileName = fileName.substring("zip2:".length());
        }
        int idx = fileName.indexOf('!');
        if (idx >= 0) {
            fileName = fileName.substring(0, idx);
        }
        return FilePathDisk.expandUserHomeDirectory(fileName);
    }

    @Override
    public String getScheme() {
        return "zip2";
    }

}

/**
 * The file is read from a stream. When reading from start to end, the same
 * input stream is re-used, however when reading from end to start, a new input
 * stream is opened for each request.
 */
class FileZip2 extends FileBase {

    private static final byte[] SKIP_BUFFER = new byte[1024];

    private final String fullName;
    private final String name;
    private final long length;
    private long pos;
    private InputStream in;
    private long inPos;
    private boolean skipUsingRead;

    FileZip2(String fullName, String name, ZipInputStream in, long length) {
        this.fullName = fullName;
        this.name = name;
        this.length = length;
        this.in = in;
    }

    @Override
    public void implCloseChannel() throws IOException {
        in.close();
    }

    @Override
    public long position() {
        return pos;
    }

    @Override
    public long size() {
        return length;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        seek();
        int len = in.read(dst.array(), dst.arrayOffset() + dst.position(),
                dst.remaining());
        if (len > 0) {
            dst.position(dst.position() + len);
            pos += len;
            inPos += len;
        }
        return len;
    }

    private void seek() throws IOException {
        if (inPos > pos) {
            if (in != null) {
                in.close();
            }
            in = null;
        }
        if (in == null) {
            in = FileUtils.newInputStream(fullName);
            inPos = 0;
        }
        if (inPos < pos) {
            long skip = pos - inPos;
            if (!skipUsingRead) {
                try {
                    IOUtils.skipFully(in, skip);
                } catch (NullPointerException e) {
                    // workaround for Android
                    skipUsingRead = true;
                }
            }
            if (skipUsingRead) {
                while (skip > 0) {
                    int s = (int) Math.min(SKIP_BUFFER.length, skip);
                    s = in.read(SKIP_BUFFER, 0, s);
                    skip -= s;
                }
            }
            inPos = pos;
        }
    }

    @Override
    public FileChannel position(long newPos) {
        this.pos = newPos;
        return this;
    }

    @Override
    public FileChannel truncate(long newLength) throws IOException {
        throw new IOException("File is read-only");
    }

    @Override
    public void force(boolean metaData) throws IOException {
        // nothing to do
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        throw new IOException("File is read-only");
    }

    @Override
    public synchronized FileLock tryLock(long position, long size,
            boolean shared) throws IOException {
        if (shared) {
            return new FileLock(new FakeFileChannel(), position, size, shared) {

                @Override
                public boolean isValid() {
                    return true;
                }

                @Override
                public void release() throws IOException {
                    // ignore
                }};
        }
        return null;
    }

    @Override
    public String toString() {
        return "zip2:" + name;
    }

}