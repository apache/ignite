package org.apache.ignite.internal.processors.igfs.meta;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.igfs.IgfsEntryInfo;
import org.apache.ignite.internal.processors.igfs.IgfsListingEntry;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

/**
 * Update directory listing closure.
 */
public final class IgfsMetaDirectoryListingAddProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, Void>,
    Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** File name to add into parent listing. */
    private String fileName;

    /** File ID.*/
    private IgfsListingEntry entry;

    /**
     * Empty constructor required for {@link Externalizable}.
     *
     */
    public IgfsMetaDirectoryListingAddProcessor() {
        // No-op.
    }

    /**
     * Constructs update directory listing closure.
     *
     * @param fileName File name to add into parent listing.
     * @param entry Listing entry to add or remove.
     */
    public IgfsMetaDirectoryListingAddProcessor(String fileName, IgfsListingEntry entry) {
        assert fileName != null;
        assert entry != null;

        this.fileName = fileName;
        this.entry = entry;
    }

    /** {@inheritDoc} */
    @Override public Void process(MutableEntry<IgniteUuid, IgfsEntryInfo> e, Object... args) {
        IgfsEntryInfo fileInfo = e.getValue();

        assert fileInfo.isDirectory();

        Map<String, IgfsListingEntry> listing = new HashMap<>(fileInfo.listing());

        // Modify listing in-place.
        IgfsListingEntry oldEntry = listing.put(fileName, entry);

        if (oldEntry != null && !oldEntry.fileId().equals(entry.fileId()))
            throw new IgniteException("Directory listing contains unexpected file" +
                " [listing=" + listing + ", fileName=" + fileName + ", entry=" + entry +
                ", oldEntry=" + oldEntry + ']');

        e.setValue(fileInfo.listing(listing));

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, fileName);
        out.writeObject(entry);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fileName = U.readString(in);
        entry = (IgfsListingEntry)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsMetaDirectoryListingAddProcessor.class, this);
    }
}
