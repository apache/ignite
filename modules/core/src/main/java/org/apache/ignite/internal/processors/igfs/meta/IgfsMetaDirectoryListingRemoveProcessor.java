package org.apache.ignite.internal.processors.igfs.meta;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.igfs.IgfsEntryInfo;
import org.apache.ignite.internal.processors.igfs.IgfsListingEntry;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

/**
 * Remove entry from directory listing.
 */
public class IgfsMetaDirectoryListingRemoveProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, Void>,
    Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** File name. */
    private String fileName;

    /** Expected ID. */
    private IgniteUuid fileId;

    /**
     * Default constructor.
     */
    public IgfsMetaDirectoryListingRemoveProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param fileName File name.
     * @param fileId File ID.
     */
    public IgfsMetaDirectoryListingRemoveProcessor(String fileName, IgniteUuid fileId) {
        this.fileName = fileName;
        this.fileId = fileId;
    }

    /** {@inheritDoc} */
    @Override public Void process(MutableEntry<IgniteUuid, IgfsEntryInfo> e, Object... args)
        throws EntryProcessorException {
        IgfsEntryInfo fileInfo = e.getValue();

        assert fileInfo != null;
        assert fileInfo.isDirectory();

        Map<String, IgfsListingEntry> listing = new HashMap<>(fileInfo.listing());

        listing.putAll(fileInfo.listing());

        IgfsListingEntry oldEntry = listing.get(fileName);

        if (oldEntry == null || !oldEntry.fileId().equals(fileId))
            throw new IgniteException("Directory listing doesn't contain expected file" +
                " [listing=" + listing + ", fileName=" + fileName + "]");

        // Modify listing in-place.
        listing.remove(fileName);

        e.setValue(fileInfo.listing(listing));

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, fileName);
        U.writeGridUuid(out, fileId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fileName = U.readString(in);
        fileId = U.readGridUuid(in);
    }
}
