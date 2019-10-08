package org.apache.ignite.internal.processors.cache.preload;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.ignite.internal.util.typedef.internal.S;

public class PartitionFileMetaInfo implements FileMetaInfo {
    /** */
    private Integer grpId;

    /** */
    private Integer partId;

    /** */
    private String name;

    /** */
    private Long size;

    /** */
    private Integer type;

    /** */
    public PartitionFileMetaInfo() {
        this(null, null, null, null, null);
    }

    /**
     * @param grpId Cache group identifier.
     * @param name Cache partition file name.
     * @param size Cache partition file size.
     * @param type {@code 0} partition file, {@code 1} delta file.
     */
    public PartitionFileMetaInfo(Integer grpId, Integer partId, String name, Long size, Integer type) {
        this.grpId = grpId;
        this.partId = partId;
        this.name = name;
        this.size = size;
        this.type = type;
    }

    /**
     * @return Related cache group id.
     */
    public Integer getGrpId() {
        return grpId;
    }

    /**
     * @return Cache partition id.
     */
    public Integer getPartId() {
        return partId;
    }

    /**
     * @return Partition file name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Partition file size.
     */
    public Long getSize() {
        return size;
    }

    /**
     * @return {@code 0} partition file, {@code 1} delta file.
     */
    public Integer getType() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public void readMetaInfo(DataInputStream is) throws IOException {
        grpId = is.readInt();
        partId = is.readInt();
        name = is.readUTF();
        size = is.readLong();
        type = is.readInt();

        if (grpId == null || partId == null || name == null || size == null || type == null)
            throw new IOException("File meta information incorrect: " + this);
    }

    /** {@inheritDoc} */
    @Override public void writeMetaInfo(DataOutputStream os) throws IOException {
        if (grpId == null || partId == null || name == null || size == null || type == null)
            throw new IOException("File meta information incorrect: " + this);

        os.writeInt(grpId);
        os.writeInt(partId);
        os.writeUTF(name);
        os.writeLong(size);
        os.writeInt(type);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PartitionFileMetaInfo.class, this);
    }
}
