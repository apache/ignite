package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

public class InvalidContext<T extends Dump> {
    public final String msg;
    public final T dump;

    public InvalidContext(String msg, T dump) {
        this.msg = msg;
        this.dump = dump;
    }

    @Override public String toString() {
        return "Error: " + msg + "\n" + dump.toString();
    }
}
