package org.apache.ignite.internal.jdbc2.lob;

/**
 * Keeps a pointer to some position in a {@link JdbcBlobBuffer}.
 */
class JdbcBlobBufferPointer {
    /** Current buffer position. */
    private long pos;

    /** Optional storage-specific context for effective access to data in the current position. */
    private JdbcBlobStorageContext context;

    /**
     * Create new pointer to zero position.
     */
    JdbcBlobBufferPointer() {
        pos = 0;
    }

    /**
     * Initialize pointer from the another one.
     *
     * @param pointer Another pointer.
     */
    JdbcBlobBufferPointer set(JdbcBlobBufferPointer pointer) {
        pos = pointer.pos;

        if (pointer.context != null)
            context = pointer.context.deepCopy();

        return this;
    }

    /**
     * Set current buffer position.
     *
     * @param pos New position.
     */
    JdbcBlobBufferPointer setPos(long pos) {
        this.pos = pos;

        return this;
    }

    /**
     * Set context.
     *
     * @param context New context.
     */
    JdbcBlobBufferPointer setContext(JdbcBlobStorageContext context) {
        this.context = context;

        return this;
    }

    /** @return Current buffer position. */
    long getPos() {
        return pos;
    }

    /** @return Context. */
    JdbcBlobStorageContext getContext() {
        return context;
    }
}
