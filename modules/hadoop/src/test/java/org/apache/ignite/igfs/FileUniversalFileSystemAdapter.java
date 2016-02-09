package org.apache.ignite.igfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.processors.igfs.FileIgfsSecondaryFileSystemImpl;
import org.apache.ignite.internal.processors.igfs.UniversalFileSystemAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * A {@link UniversalFileSystemAdapter} implementation based
 * on {@link FileIgfsSecondaryFileSystemImpl}.
 */
public class FileUniversalFileSystemAdapter implements UniversalFileSystemAdapter {
    /** */
    private final FileIgfsSecondaryFileSystemImpl impl;

    /**
     * Constructs a new universal adapter.
     *
     * @param impl The impl.
     */
    FileUniversalFileSystemAdapter(FileIgfsSecondaryFileSystemImpl impl) {
        assert impl != null;

        this.impl = impl;
    }

    /**
     * Utility method.
     * @param path The path to check.
     * @return The IgfsPath.
     */
    private IgfsPath igfsPath(String path) {
        return new IgfsPath(path);
    }

    /** {@inheritDoc} */
    @Override public boolean delete(String path, boolean recursive) throws IOException {
        return impl.delete(igfsPath(path), recursive);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return impl.toString();
    }

    /** {@inheritDoc} */
    @Override public boolean exists(String path) throws IOException {
        return impl.exists(igfsPath(path));
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(String path) throws IOException {
        impl.mkdirs(igfsPath(path));
    }

    /** {@inheritDoc} */
    @Override public void format() throws IOException {
        File root = new File(impl.toString());

        chmod777Recursively(root);

        if (root.exists())
            U.delete(root);

        if (root.exists())
            throw new IOException("Failed to delete root. [dir=" + root + ']');

        root.mkdirs();

        if (!root.exists() || !root.isDirectory())
            throw new IOException("Failed to re-create root. [dir=" + root + ']');
    }

    private static void chmod777Recursively(File f) {
        assert f != null;

        f.setReadable(true, false);
        f.setWritable(true, false);
        f.setExecutable(true, false);

        File[] ff = f.listFiles();

        if (ff != null) {
            for (File x: ff)
                chmod777Recursively(x);
        }
    }


    /** {@inheritDoc} */
    @Override public Map<String, String> properties(String path) throws IOException {
        IgfsFile file = impl.info(igfsPath(path));

        if (file == null)
            return null; // or throw FileNotFoundException?

        return file.properties();
    }

    /** {@inheritDoc} */
    @Override public InputStream openInputStream(String path) throws IOException {
        File root = new File(impl.toString());

        return new FileInputStream(new File(root, path));
    }

    /** {@inheritDoc} */
    @Override public OutputStream openOutputStream(String path, boolean append) throws IOException {
        File root = new File(impl.toString());

        return new FileOutputStream(new File(root, path), append);
    }

    /** {@inheritDoc} */
    @Override public <T> T getAdapter(Class<T> clazz) {
        if (clazz == FileIgfsSecondaryFileSystemImpl.class || clazz == IgfsSecondaryFileSystem.class)
            return (T)impl;

        return null;
    }
}
