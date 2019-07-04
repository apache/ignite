package org.apache.ignite.internal.processors.job;

import java.util.Objects;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.internal.GridKernalContext;

/** todo MY_TODO */
public class SecureComputeJob implements ComputeJob {
    /** . */
    private final GridKernalContext ctx;

    /** . */
    private final ComputeJob original;

    /** . */
    public SecureComputeJob(GridKernalContext ctx, ComputeJob original) {
        this.ctx = Objects.requireNonNull(ctx);
        this.original = Objects.requireNonNull(original);
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        original.cancel();
    }

    /** {@inheritDoc} */
    @Override public Object execute() throws IgniteException {
        try {
            return ctx.security().doAsCurrentSubject(original::execute);
        }
        catch (Exception e) {
            if (e instanceof IgniteException)
                throw (IgniteException)e;
            else
                throw new IgniteException(e);
        }
    }
}
