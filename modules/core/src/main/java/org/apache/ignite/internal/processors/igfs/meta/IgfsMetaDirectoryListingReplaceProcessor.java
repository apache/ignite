package org.apache.ignite.internal.processors.igfs.meta;

import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.igfs.IgfsEntryInfo;
import org.apache.ignite.internal.processors.igfs.IgfsListingEntry;
import org.apache.ignite.internal.util.typedef.internal.S;
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
 * Listing replace processor.
 */
public final class IgfsMetaDirectoryListingReplaceProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, Void>,
    Externalizable, Binarylizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Name. */
    private String name;

    /** New ID. */
    private IgniteUuid id;

    /**
     * Constructor.
     */
    public IgfsMetaDirectoryListingReplaceProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param name Name.
     * @param id ID.
     */
    public IgfsMetaDirectoryListingReplaceProcessor(String name, IgniteUuid id) {
        this.name = name;
        this.id = id;
    }

    /** {@inheritDoc} */
    @Override public Void process(MutableEntry<IgniteUuid, IgfsEntryInfo> e, Object... args)
        throws EntryProcessorException {
        IgfsEntryInfo fileInfo = e.getValue();

        assert fileInfo.isDirectory();

        Map<String, IgfsListingEntry> listing = new HashMap<>(fileInfo.listing());

        // Modify listing in-place.
        IgfsListingEntry oldEntry = listing.get(name);

        if (oldEntry == null)
            throw new IgniteException("Directory listing doesn't contain expected entry: " + name);

        listing.put(name, new IgfsListingEntry(id, oldEntry.isDirectory()));

        e.setValue(fileInfo.listing(listing));

        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, name);
        out.writeObject(id);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        name = U.readString(in);
        id = (IgniteUuid)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
        BinaryRawWriter out = writer.rawWriter();

        out.writeString(name);
        BinaryUtils.writeIgniteUuid(out, id);
    }

    /** {@inheritDoc} */
    @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
        BinaryRawReader in = reader.rawReader();

        name = in.readString();
        id = BinaryUtils.readIgniteUuid(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsMetaDirectoryListingReplaceProcessor.class, this);
    }
}
