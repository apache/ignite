package org.apache.ignite.internal.processors.datastructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import org.apache.ignite.IgniteCacheRestartingException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridKernalState;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheGateway;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;

public abstract class GridCacheCollectionProxy<T> implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Deserialization stash. */
    private static final ThreadLocal<T3<GridKernalContext, String, String>> stash =
        new ThreadLocal<T3<GridKernalContext, String, String>>() {
            @Override protected T3<GridKernalContext, String, String> initialValue() {
                return new T3<>();
            }
        };

    /** Delegate set. */
    protected T delegate;

    /** Cache context. */
    protected GridCacheContext cctx;

    /** Cache gateway. */
    protected GridCacheGateway gate;

    /** Busy lock. */
    protected GridSpinBusyLock busyLock;

    /** Check removed flag. */
    protected volatile boolean rmvCheck;

    /** */
    private volatile GridFutureAdapter<?> suspendFut;

    /** */
    public GridCacheCollectionProxy(){

    }

    public GridCacheCollectionProxy(GridCacheContext cctx, T delegate) {
        this.cctx = cctx;
        this.delegate = delegate;

        gate = cctx.gate();

        busyLock = new GridSpinBusyLock();
    }

    public void suspend() {
        suspendFut = new GridFutureAdapter<>();
    }

    public synchronized void restart(IgniteInternalCache cache) {
        if (cache != null) {
            cctx = cache.context();
            gate = cctx.gate();
            busyLock = new GridSpinBusyLock();

            delegate = reStartDelegate();
        }

        suspendFut.onDone();
    }

    protected abstract T reStartDelegate();

    /**
     * Enters busy state.
     */
    private void enterBusy() {
        boolean rmvd;

        if (rmvCheck) {
            checkDelegateIsValid();

            checkRemove();
        }

        if (!busyLock.enterBusy())
            throw removedError();
    }

    protected abstract void checkRemove();

    protected void enter(){
        enterBusy();

        gate.enter();
    }

    protected void leave(){
        gate.leave();

        leaveBusy();
    }

    /**
     * Leaves busy state.
     */
    private void leaveBusy() {
        busyLock.leaveBusy();
    }

    public void checkDelegateIsValid() {
        if (!suspendFut.isDone()) {
            if (gate.isStopped() && cctx.kernalContext().gateway().getState() == GridKernalState.STARTED)
                restart(cctx.kernalContext().cache().cache(cctx.name()));
            else
                throw new IgniteCacheRestartingException(new IgniteFutureImpl<>(suspendFut), cctx.name());
        }
    }

    /**
     *
     */
    public void needCheckNotRemoved() {
        rmvCheck = true;

        suspend();
    }

    /**
     * @return Error.
     */
    protected IllegalStateException removedError() {
        return new IllegalStateException("Set has been removed from cache: " + delegate);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(cctx.kernalContext());
        U.writeString(out, cctx.name());
        U.writeString(out, cctx.group().name());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        T3<GridKernalContext, String, String> t = stash.get();

        t.set1((GridKernalContext)in.readObject());
        t.set2(U.readString(in));
        t.set3(U.readString(in));
    }

    /**
     * Reconstructs object on unmarshalling.
     *
     * @return Reconstructed object.
     * @throws ObjectStreamException Thrown in case of unmarshalling error.
     */
    protected Object readResolve() throws ObjectStreamException {
        try {
            T3<GridKernalContext, String, String> t = stash.get();

            return t.get1().dataStructures().set(t.get2(), t.get3(), null);
        }
        catch (IgniteCheckedException e) {
            throw U.withCause(new InvalidObjectException(e.getMessage()), e);
        }
        finally {
            stash.remove();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return delegate.toString();
    }
}
