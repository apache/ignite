/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.fs.mapreduce.records;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Record resolver based on new line detection. This resolver can detect new lines based on '\n' or '\r\n' sequences.
 * <p>
 * Note that this resolver cannot be created and has one constant implementations: {@link #NEW_LINE}.
 */
public class IgniteFsNewLineRecordResolver extends IgniteFsByteDelimiterRecordResolver {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Singleton new line resolver. This resolver will resolve records based on new lines
     * regardless if they have '\n' or '\r\n' patterns.
     */
    public static final IgniteFsNewLineRecordResolver NEW_LINE = new IgniteFsNewLineRecordResolver(true);

    /** CR symbol. */
    public static final byte SYM_CR = 0x0D;

    /** LF symbol. */
    public static final byte SYM_LF = 0x0A;

    /**
     * Empty constructor required for {@link Externalizable} support.
     */
    public IgniteFsNewLineRecordResolver() {
        // No-op.
    }

    /**
     * Creates new-line record resolver.
     *
     * @param b Artificial flag to differentiate from empty constructor.
     */
    @SuppressWarnings("UnusedParameters")
    private IgniteFsNewLineRecordResolver(boolean b) {
        super(new byte[] { SYM_CR, SYM_LF }, new byte[] { SYM_LF });
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteFsNewLineRecordResolver.class, this);
    }
}
