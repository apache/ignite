package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.UUID;

public interface Mailbox<T> extends Node<T> {
    /**
     * @return Query ID.
     */
    default UUID queryId() {
        return context().queryId();
    }

    /**
     * @return Fragment ID.
     */
    default long fragmentId() {
        return context().fragmentId();
    }

    /**
     * @return Exchange ID.
     */
    long exchangeId();
}
