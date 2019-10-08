package org.apache.ignite.internal.processors.cache.preload;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface FileMetaInfo {
    /**
     * @param is The stream to read file meta info from.
     * @throws IOException If fails.
     */
    public void readMetaInfo(DataInputStream is) throws IOException;

    /**
     * @param os The stream to write file meta info at.
     * @throws IOException If fails.
     */
    public void writeMetaInfo(DataOutputStream os) throws IOException;
}