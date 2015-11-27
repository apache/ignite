package org.apache.ignite.internal.processors.igfs;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * File or directory information backed by {@link java.io.File} instance.
 */
public final class FileIgfsFile implements IgfsFile, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** The java.io.File delegate. */
    private Path nioPath;

    /** Path as it is seen from IGFS viewpoint. */
    private String igfsPath;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public FileIgfsFile() {
        // No-op.
    }

    /**
     * Constructs directory info.
     *
     * @param fullAbsolutePath The full absolute path.
     * @param igfsPath IGFS path
     */
    public FileIgfsFile(String fullAbsolutePath, String igfsPath) {
        A.notNull(fullAbsolutePath, "fullAbsolutePath");
        A.notNull(igfsPath, "igfsPath");

        this.nioPath = Paths.get(fullAbsolutePath);
        this.igfsPath = igfsPath;
    }

    /** {@inheritDoc} */
    @Override public IgfsPath path() {
        return new IgfsPath(igfsPath);
    }

    /** {@inheritDoc} */
    @Override public final boolean isFile() {
        return Files.isRegularFile(nioPath);
    }

    /** {@inheritDoc} */
    @Override public final boolean isDirectory() {
        return Files.isDirectory(nioPath);
    }

    /** {@inheritDoc} */
    @Override public final long length() {
        try {
            BasicFileAttributes attrs = Files.readAttributes(nioPath, BasicFileAttributes.class);

            return attrs.size();
        }
        catch (IOException ioe) {
            throw new IgfsException(ioe);
        }
    }

    /** {@inheritDoc} */
    @Override public final int blockSize() {
        // NB: zero treated as a sign of directory, so we
        // must return a positive value.
        return 1;
    }

    /** {@inheritDoc} */
    @Override public final long groupBlockSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long accessTime() {
        try {
            BasicFileAttributes attrs = Files.readAttributes(nioPath, BasicFileAttributes.class);

            FileTime fileTime = attrs.lastAccessTime();

            return fileTime.toMillis();
        }
        catch (IOException ioe) {
            throw new IgfsException(ioe);
        }
    }

    /** {@inheritDoc} */
    @Override public long modificationTime() {
        try {
            BasicFileAttributes attrs = Files.readAttributes(nioPath, BasicFileAttributes.class);

            FileTime fileTime = attrs.lastModifiedTime();

            return fileTime.toMillis();
        }
        catch (IOException ioe) {
            throw new IgfsException(ioe);
        }
    }

    /** {@inheritDoc} */
    @Override public String property(String name) throws IllegalArgumentException {
        try {
            Map<String, String> props = FileIgfsSecondaryFileSystemImpl.getProperties(nioPath);

            String val = props.get(name);

            if (val == null)
                throw new IllegalArgumentException("File property not found [path=" + igfsPath + ", name=" + name + ']');

            return val;
        }
        catch (IOException ioe) {
            throw new IgniteException(ioe);
        }
    }

    /** {@inheritDoc} */
    @Override public final String property(String name, @Nullable String dfltVal) {
        String val = property(name);

        return val == null ? dfltVal : val;
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> properties() {
        try {
            return FileIgfsSecondaryFileSystemImpl.getProperties(nioPath);
        }
        catch (IOException ioe) {
            throw new IgniteException(ioe);
        }
    }

    /**
     * Writes object to data output.
     *
     * @param out Data output.
     */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(igfsPath);

        out.writeUTF(nioPath.toString());
    }

    /**
     * Reads object from data input.
     *
     * @param in Data input.
     */
    @Override public void readExternal(ObjectInput in) throws IOException {
        igfsPath = in.readUTF();

         nioPath = Paths.get(in.readUTF());
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return nioPath.toString().hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == this)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        FileIgfsFile that = (FileIgfsFile)o;

        return nioPath.toString().equals(that.nioPath.toString());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FileIgfsFile.class, this);
    }
}