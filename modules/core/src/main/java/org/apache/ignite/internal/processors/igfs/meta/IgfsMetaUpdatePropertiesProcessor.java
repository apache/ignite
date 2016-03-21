package org.apache.ignite.internal.processors.igfs.meta;

import org.apache.ignite.internal.processors.igfs.IgfsEntryInfo;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * Update properties processor.
 */
public class IgfsMetaUpdatePropertiesProcessor implements EntryProcessor<IgniteUuid, IgfsEntryInfo, IgfsEntryInfo>,
    Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Properties to be updated. */
    private Map<String, String> props;

    /**
     * Constructor.
     */
    public IgfsMetaUpdatePropertiesProcessor() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param props Properties.
     */
    public IgfsMetaUpdatePropertiesProcessor(Map<String, String> props) {
        this.props = props;
    }

    /** {@inheritDoc} */
    @Override public IgfsEntryInfo process(MutableEntry<IgniteUuid, IgfsEntryInfo> entry, Object... args)
        throws EntryProcessorException {
        IgfsEntryInfo oldInfo = entry.getValue();

        Map<String, String> tmp = oldInfo.properties();

        tmp = tmp == null ? new GridLeanMap<String, String>(props.size()) : new GridLeanMap<>(tmp);

        for (Map.Entry<String, String> e : props.entrySet()) {
            if (e.getValue() == null)
                // Remove properties with 'null' values.
                tmp.remove(e.getKey());
            else
                // Add/overwrite property.
                tmp.put(e.getKey(), e.getValue());
        }

        IgfsEntryInfo newInfo = oldInfo.properties(tmp);

        entry.setValue(newInfo);

        return newInfo;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeStringMap(out, props);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        props = U.readStringMap(in);
    }
}
