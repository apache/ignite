package org.apache.ignite.internal;

import org.apache.ignite.IgniteException;
import org.jetbrains.annotations.Nullable;

public class UnregisteredClassException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    private final Class cls;

    public UnregisteredClassException(Class cls) {
        this.cls = cls;
    }

    public UnregisteredClassException(String msg, Class cls) {
        super(msg);
        this.cls = cls;
    }

    public UnregisteredClassException(Throwable cause, Class cls) {
        super(cause);
        this.cls = cls;
    }

    public UnregisteredClassException(String msg, @Nullable Throwable cause, Class cls) {
        super(msg, cause);
        this.cls = cls;
    }

    public Class cls() {
        return cls;
    }
}
