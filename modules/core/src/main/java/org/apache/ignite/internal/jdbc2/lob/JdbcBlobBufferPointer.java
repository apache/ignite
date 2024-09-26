package org.apache.ignite.internal.jdbc2.lob;

/**
 * Keeps a pointer to some position in a {@link JdbcBlobBuffer}.
 */
public class JdbcBlobBufferPointer {
    /** Current buffer position. */
    private long pos;

    /** Additional storage-specific context to access data in the current position. */
    private JdbcBlobStorageContext context;

    /** */
    JdbcBlobBufferPointer() {
        pos = 0;
    }

    /**
     * Initialize pointer from another one.
     *
     * @param x Another pointer.
     */
    public JdbcBlobBufferPointer set(JdbcBlobBufferPointer x) {
        pos = x.pos;

        if (x.context != null)
            context = x.context.copy();

        return this;
    }

    /** */
    JdbcBlobBufferPointer setPos(long pos) {
        this.pos = pos;

        return this;
    }

    /** */
    JdbcBlobBufferPointer setContext(JdbcBlobStorageContext context) {
        this.context = context;

        return this;
    }

    /** */
    public long getPos() {
        return pos;
    }

    /** */
    public JdbcBlobStorageContext getContext() {
        return context;
    }
}
