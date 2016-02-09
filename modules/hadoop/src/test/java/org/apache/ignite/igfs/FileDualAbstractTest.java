package org.apache.ignite.igfs;

import java.io.File;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.processors.igfs.FileIgfsSecondaryFileSystemImpl;
import org.apache.ignite.internal.processors.igfs.IgfsDualAbstractSelfTest;

/**
 *
 */
public class FileDualAbstractTest extends IgfsDualAbstractSelfTest {
    /**
     * Constructor.
     */
    public FileDualAbstractTest(IgfsMode mode) {
        super(mode);
    }

    /**
     * Creates secondary filesystems.
     *
     * @return IgfsSecondaryFileSystem
     * @throws Exception On failure.
     */
    @Override protected final IgfsSecondaryFileSystem createSecondaryFileSystemStack() throws Exception {
        File testIgh = new File(getTestResources().getIgniteHome());

        File fsRoot = new File(testIgh, "work/fs-file");

        FileIgfsSecondaryFileSystemImpl second =
            new FileIgfsSecondaryFileSystemImpl(fsRoot.getCanonicalPath());

        igfsSecondary = new FileUniversalFileSystemAdapter(second);

        igfsSecondary.format();

        return second;
    }
}
