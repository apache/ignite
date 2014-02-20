// @scala.file.header

/*
 * ________               ______                    ______   _______
 * __  ___/_____________ ____  /______ _________    __/__ \  __  __ \
 * _____ \ _  ___/_  __ `/__  / _  __ `/__  ___/    ____/ /  _  / / /
 * ____/ / / /__  / /_/ / _  /  / /_/ / _  /        _  __/___/ /_/ /
 * /____/  \___/  \__,_/  /_/   \__,_/  /_/         /____/_(_)____/
 *
 */

package org.gridgain.scalar.pimps

import org.gridgain.grid._
import org.gridgain.grid.lang._
import org.jetbrains.annotations._
import GridClosureCallMode._

/**
 * Companion object.
 *
 * @author @java.author
 * @version @java.version
 */
object ScalarProjectionPimp {
    /**
     * Creates new Scalar projection pimp with given Java-side implementation.
     *
     * @param impl Java-side implementation.
     */
    def apply(impl: GridProjection) = {
        if (impl == null)
            throw new NullPointerException("impl")

        val pimp = new ScalarProjectionPimp[GridProjection]

        pimp.impl = impl

        pimp
    }
}

/**
 * ==Overview==
 * Defines Scalar "pimp" for `GridProjection` on Java side.
 *
 * Essentially this class extends Java `GridProjection` interface with Scala specific
 * API adapters using primarily implicit conversions defined in `ScalarConversions` object. What
 * it means is that you can use functions defined in this class on object
 * of Java `GridProjection` type. Scala will automatically (implicitly) convert it into
 * Scalar's pimp and replace the original call with a call on that pimp.
 *
 * Note that Scalar provide extensive library of implicit conversion between Java and
 * Scala GridGain counterparts in `ScalarConversions` object
 *
 * ==Suffix '$' In Names==
 * Symbol `$` is used in names when they conflict with the names in the base Java class
 * that Scala pimp is shadowing or with Java package name that your Scala code is importing.
 * Instead of giving two different names to the same function we've decided to simply mark
 * Scala's side method with `$` suffix.
 *
 * @author @java.author
 * @version @java.version
 */
class ScalarProjectionPimp[A <: GridProjection] extends PimpedType[A] with Iterable[GridNode]
    with ScalarTaskThreadContext[A] {
    /** */
    lazy val value: A = impl

    /** */
    protected var impl: A = _

    /** Type alias for '() => Unit'. */
    protected type Run = () => Unit

    /** Type alias for '() => R'. */
    protected type Call[R] = () => R

    /** Type alias for '(E1) => R'. */
    protected type Call1[E1, R] = (E1) => R

    /** Type alias for '(E1, E2) => R'. */
    protected type Call2[E1, E2, R] = (E1, E2) => R

    /** Type alias for '(E1, E2, E3) => R'. */
    protected type Call3[E1, E2, E3, R] = (E1, E2, E3) => R

    /** Type alias for '() => Boolean'. */
    protected type Pred = () => Boolean

    /** Type alias for '(E1) => Boolean'. */
    protected type Pred1[E1] = (E1) => Boolean

    /** Type alias for '(E1, E2) => Boolean'. */
    protected type Pred2[E1, E2] = (E1, E2) => Boolean

    /** Type alias for '(E1, E2, E3) => Boolean'. */
    protected type Pred3[E1, E2, E3] = (E1, E2, E3) => Boolean

    /** Type alias for node filter predicate. */
    protected type NF = GridPredicate[_ >: GridNode]

    /** Type alias. */
    protected type GCM = GridClosureCallMode

    /**
     * Gets iterator for this projection's nodes.
     */
    def iterator = nodes$(null).iterator

    /**
     * Gets sequence of all nodes in this projection for given predicate.
     *
     * @param p Optional node filter predicates. It `null` provided - all nodes will be returned.
     * @see `org.gridgain.grid.GridProjection.nodes(...)`
     */
    def nodes$(@Nullable p: NF): Seq[GridNode] =
        toScalaSeq(value.forPredicate(p).nodes())

    /**
     * Gets sequence of all remote nodes in this projection for given predicate.
     *
     * @param p Optional node filter predicate. It `null` provided - all remote nodes will be returned.
     * @see `org.gridgain.grid.GridProjection.remoteNodes(...)`
     */
    def remoteNodes$(@Nullable p: NF = null): Seq[GridNode] =
        toScalaSeq(value.forRemotes().forPredicate(p).nodes())

    /**
     * <b>Alias</b> for method `send$(...)`.
     *
     * @param obj Optional object to send. If `null` - this method is no-op.
     * @param p Optional node filter predicates. If none provided or `null` -
     *      all nodes in the projection will be used.
     * @see `org.gridgain.grid.GridProjection.send(...)`
     */
    def !<(@Nullable obj: AnyRef, @Nullable p: NF) {
        value.forPredicate(p).message().send(null, obj)
    }

    /**
     * <b>Alias</b> for method `send$(...)`.
     *
     * @param seq Optional sequence of objects to send. If empty or `null` - this
     *      method is no-op.
     * @param p Optional node filter predicate. If none provided or `null` -
     *      all nodes in the projection will be used.
     * @see `org.gridgain.grid.GridProjection.send(...)`
     */
    def !<(@Nullable seq: Seq[AnyRef], @Nullable p: NF) {
        value.forPredicate(p).message().send(null, seq)
    }

    /**
     * Sends given object to the nodes in this projection.
     *
     * @param obj Optional object to send. If `null` - this method is no-op.
     * @param p Optional node filter predicate. If none provided or `null` -
     *      all nodes in the projection will be used.
     * @see `org.gridgain.grid.GridProjection.send(...)`
     */
    def send$(@Nullable obj: AnyRef, @Nullable p: NF) {
        value.forPredicate(p).message().send(null, obj)
    }

    /**
     * Sends given object to the nodes in this projection.
     *
     * @param seq Optional sequence of objects to send. If empty or `null` - this
     *      method is no-op.
     * @param p Optional node filter predicate. If  `null` provided - all nodes in the projection will be used.
     * @see `org.gridgain.grid.GridProjection.send(...)`
     */
    def send$(@Nullable seq: Seq[AnyRef], @Nullable p: NF) {
        value.forPredicate(p).message().send(null, seq)
    }

    /**
     * Synchronous closures call on this projection with return value.
     * This call will block until all results are received and ready.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or `null` - this method is no-op and returns `null`.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Sequence of result values from all nodes where given closures were executed or `null` (see above).
     */
    def call$[R](mode: GCM, @Nullable s: Seq[Call[R]], @Nullable p: NF): Seq[R] =
        toScalaSeq(callAsync$(mode, s, p).get)

    /*
    * Utility shortcuts.
    */
    def callOpt[R](mode: GCM, @Nullable s: Seq[Call[R]], @Nullable p: NF): Option[Seq[R]] = Option(call$(mode, s, p))
    def ucastCall[R](@Nullable s: Seq[Call[R]], @Nullable p: NF): Seq[R] = call$(UNICAST, s, p)
    def bcastCall[R](@Nullable s: Seq[Call[R]], @Nullable p: NF): Seq[R] = call$(BROADCAST, s, p)
    def spreadCall[R](@Nullable s: Seq[Call[R]], @Nullable p: NF): Seq[R] = call$(SPREAD, s, p)
    def balanceCall[R](@Nullable s: Seq[Call[R]], @Nullable p: NF): Seq[R] = call$(BALANCE, s, p)
    def ucastCallOpt[R](@Nullable s: Seq[Call[R]], @Nullable p: NF): Option[Seq[R]] = Option(call$(UNICAST, s, p))
    def bcastCallOpt[R](@Nullable s: Seq[Call[R]], @Nullable p: NF): Option[Seq[R]] = Option(call$(BROADCAST, s, p))
    def spreadCallOpt[R](@Nullable s: Seq[Call[R]], @Nullable p: NF): Option[Seq[R]] = Option(call$(SPREAD, s, p))
    def balanceCallOpt[R](@Nullable s: Seq[Call[R]], @Nullable p: NF): Option[Seq[R]] = Option(call$(BALANCE, s, p))

    /**
     * Synchronous closures call on this projection with return value.
     * This call will block until all results are received and ready. If this projection
     * is empty than `dflt` closure will be executed and its result returned.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or `null` - this method is no-op and returns `null`.
     * @param dflt Closure to execute if projection is empty.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Sequence of result values from all nodes where given closures were executed or `null` (see above).
     */
    def callSafe[R](mode: GCM, @Nullable s: Seq[Call[R]], dflt: () => Seq[R], @Nullable p: NF): Seq[R] = {
        assert(dflt != null)

        try
            call$(mode, s, p)
        catch {
            case _: GridEmptyProjectionException => dflt()
        }
    }

    /**
     * <b>Alias</b> for the same function `call$`.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or `null` - this method is no-op and returns `null`.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Sequence of result values from all nodes where given closures were executed or `null` (see above).
     * @see `org.gridgain.grid.GridProjection.call(...)`
     */
    def #<[R](mode: GCM, @Nullable s: Seq[Call[R]], @Nullable p: NF): Seq[R] =
        call$(mode, s, p)

    /**
     * Synchronous closure call on this projection with return value.
     * This call will block until all results are received and ready.
     *
     * @param mode Closure call mode.
     * @param s Optional closure to call. If `null` - this method is no-op and returns `null`.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Sequence of result values from all nodes where given closures were executed or `null` (see above).
     * @see `org.gridgain.grid.GridProjection.call(...)`
     */
    def call$[R](mode: GCM, @Nullable s: Call[R], @Nullable p: NF): Seq[R] =
        call$(mode, Seq(s), p)

    /*
     * Utility shortcuts.
     */
    def callOpt[R](mode: GCM, @Nullable s: Call[R], @Nullable p: NF): Option[Seq[R]] = Option(call$(mode, s, p))
    def ucastCall[R](@Nullable s: Call[R], @Nullable p: NF): Seq[R] = call$(UNICAST, s, p)
    def bcastCall[R](@Nullable s: Call[R], @Nullable p: NF): Seq[R] = call$(BROADCAST, s, p)
    def spreadCall[R](@Nullable s: Call[R], @Nullable p: NF): Seq[R] = call$(SPREAD, s, p)
    def balanceCall[R](@Nullable s: Call[R], @Nullable p: NF): Seq[R] = call$(BALANCE, s, p)
    def ucastCallOpt[R](@Nullable s: Call[R], @Nullable p: NF): Option[Seq[R]] = Option(call$(UNICAST, s, p))
    def bcastCallOpt[R](@Nullable s: Call[R], @Nullable p: NF): Option[Seq[R]] = Option(call$(BROADCAST, s, p))
    def spreadCallOpt[R](@Nullable s: Call[R], @Nullable p: NF): Option[Seq[R]] = Option(call$(SPREAD, s, p))
    def balanceCallOpt[R](@Nullable s: Call[R], @Nullable p: NF): Option[Seq[R]] = Option(call$(BALANCE, s, p))

    /**
     * Synchronous closure call on this projection with return value.
     * This call will block until all results are received and ready. If this projection
     * is empty than `dflt` closure will be executed and its result returned.
     *
     * @param mode Closure call mode.
     * @param s Optional closure to call. If `null` - this method is no-op and returns `null`.
     * @param dflt Closure to execute if projection is empty.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Sequence of result values from all nodes where given closures were executed or `null` (see above).
     * @see `org.gridgain.grid.GridProjection.call(...)`
     */
    def callSafe[R](mode: GCM, @Nullable s: Call[R], dflt: () => Seq[R], @Nullable p: NF): Seq[R] = {
        assert(dflt != null)

        try
            call$(mode, Seq(s), p)
        catch {
            case _: GridEmptyProjectionException => dflt()
        }
    }

    /**
     * <b>Alias</b> for the same function `call$`.
     *
     * @param mode Closure call mode.
     * @param s Optional closure to call. If `null` - this method is no-op and returns `null`.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Sequence of result values from all nodes where given closures were executed
     *      or `null` (see above).
     * @see `org.gridgain.grid.GridProjection.call(...)`
     */
    def #<[R](mode: GCM, @Nullable s: Call[R], @Nullable p: NF): Seq[R] =
        call$(mode, s, p)

    /**
     * Synchronous closures call on this projection without return value.
     * This call will block until all executions are complete.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or `null` - this method is no-op.
     * @param p Optional node filter predicate. If `null` provided- all nodes in projection will be used.
     * @see `org.gridgain.grid.GridProjection.run(...)`
     */
    def run$(mode: GCM, @Nullable s: Seq[Run], @Nullable p: NF) {
        runAsync$(mode, s, p).get
    }

    /*
     * Utility shortcuts.
     */
    def ucastRun(@Nullable s: Seq[Run], @Nullable p: NF) { run$(UNICAST, s, p) }
    def bcastRun(@Nullable s: Seq[Run], @Nullable p: NF) { run$(BROADCAST, s, p) }
    def spreadRun(@Nullable s: Seq[Run], @Nullable p: NF) { run$(SPREAD, s, p) }
    def balanceRun(@Nullable s: Seq[Run], @Nullable p: NF) { run$(BALANCE, s, p) }

    /**
     * Synchronous closures call on this projection without return value.
     * This call will block until all executions are complete. If this projection
     * is empty than `dflt` closure will be executed.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or `null` - this
     *      method is no-op.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @param dflt Closure to execute if projection is empty.
     * @see `org.gridgain.grid.GridProjection.run(...)`
     */
    def runSafe(mode: GCM, @Nullable s: Seq[Run], @Nullable dflt: Run, @Nullable p: NF) {
        try {
            run$(mode, s, p)
        }
        catch {
            case _: GridEmptyProjectionException => if (dflt != null) dflt() else ()
        }
    }

    /**
     * <b>Alias</b> alias for the same function `run$`.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or `null` - this method is no-op.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @see `org.gridgain.grid.GridProjection.run(...)`
     */
    def *<(mode: GCM, @Nullable s: Seq[Run], @Nullable p: NF) {
        run$(mode, s, p)
    }

    /**
     * Synchronous closure call on this projection without return value.
     * This call will block until all executions are complete.
     *
     * @param mode Closure call mode.
     * @param s Optional closure to call. If empty or `null` - this method is no-op.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @see `org.gridgain.grid.GridProjection.run(...)`
     */
    def run$(mode: GCM, @Nullable s: Run, @Nullable p: NF) {
        run$(mode, Seq(s), p)
    }

    /*
     * Utility shortcuts.
     */
    def ucastRun(@Nullable s: Run, @Nullable p: NF) { run$(UNICAST, s, p) }
    def bcastRun(@Nullable s: Run, @Nullable p: NF) { run$(BROADCAST, s, p) }
    def spreadRun(@Nullable s: Run, @Nullable p: NF) { run$(SPREAD, s, p) }
    def balanceRun(@Nullable s: Run, @Nullable p: NF) { run$(BALANCE, s, p) }

    /**
     * Synchronous closure call on this projection without return value.
     * This call will block until all executions are complete. If this projection
     * is empty than `dflt` closure will be executed.
     *
     * @param mode Closure call mode.
     * @param s Optional closure to call. If empty or `null` - this method is no-op.
     * @param dflt Closure to execute if projection is empty.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @see `org.gridgain.grid.GridProjection.run(...)`
     */
    def runSafe(mode: GCM, @Nullable s: Run, @Nullable dflt: Run, @Nullable p: NF) {
        try {
            run$(mode, s, p)
        }
        catch {
            case _: GridEmptyProjectionException => if (dflt != null) dflt() else ()
        }
    }

    /**
     * <b>Alias</b> for the same function `run$`.
     *
     * @param mode Closure call mode.
     * @param s Optional closure to call. If empty or `null` - this method is no-op.
     * @param p Optional node filter predicate. If none provided or `null` - all nodes in projection will be used.
     * @see `org.gridgain.grid.GridProjection.run(...)`
     */
    def *<(mode: GCM, @Nullable s: Run, @Nullable p: NF) {
        run$(mode, s, p)
    }

    /**
     * Asynchronous closures call on this projection with return value. This call will
     * return immediately with the future that can be used to wait asynchronously for the results.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or `null` - this method
     *      is no-op and finished future over `null` is returned.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Future of Java collection containing result values from all nodes where given
     *      closures were executed or `null` (see above).
     * @see `org.gridgain.grid.GridProjection.call(...)`
     */
    def callAsync$[R](mode: GCM, @Nullable s: Seq[Call[R]], @Nullable p: NF):
        GridFuture[java.util.Collection[R]] = {
        assert(mode != null)

        value.forPredicate(p).compute().call[R](mode, toJavaCollection(s, (f: Call[R]) => toCallable(f)))
    }

    /**
     * <b>Alias</b> for the same function `callAsync$`.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or `null` - this method
     *      is no-op and finished future over `null` is returned.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Future of Java collection containing result values from all nodes where given
     *      closures were executed or `null` (see above).
     * @see `org.gridgain.grid.GridProjection.call(...)`
     */
    def #?[R](mode: GCM, @Nullable s: Seq[Call[R]], @Nullable p: NF): GridFuture[java.util.Collection[R]] = {
        callAsync$(mode, s, p)
    }

    /**
     * Asynchronous closure call on this projection with return value. This call will
     * return immediately with the future that can be used to wait asynchronously for the results.
     *
     * @param mode Closure call mode.
     * @param s Optional closure to call. If `null` - this method is no-op and finished
     *      future over `null` is returned.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Future of Java collection containing result values from all nodes where given
     *      closures were executed or `null` (see above).
     * @see `org.gridgain.grid.GridProjection.call(...)`
     */
    def callAsync$[R](mode: GCM, @Nullable s: Call[R], @Nullable p: NF): GridFuture[java.util.Collection[R]] = {
        callAsync$(mode, Seq(s), p)
    }

    /**
     * <b>Alias</b> for the same function `callAsync$`.
     *
     * @param mode Closure call mode.
     * @param s Optional closure to call. If `null` - this method is no-op and finished
     *      future over `null` is returned.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Future of Java collection containing result values from all nodes where given
     *      closures were executed or `null` (see above).
     * @see `org.gridgain.grid.GridProjection.call(...)`
     */
    def #?[R](mode: GCM, @Nullable s: Call[R], @Nullable p: NF): GridFuture[java.util.Collection[R]] = {
        callAsync$(mode, s, p)
    }

    /**
     * Asynchronous closures call on this projection without return value. This call will
     * return immediately with the future that can be used to wait asynchronously for the results.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of absolute closures to call. If empty or `null` - this method
     *      is no-op and finished future over `null` will be returned.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @see `org.gridgain.grid.GridProjection.call(...)`
     */
    def runAsync$(mode: GCM, @Nullable s: Seq[Run], @Nullable p: NF): GridFuture[_] = {
        assert(mode != null)

        value.forPredicate(p).compute().run(mode, toJavaCollection(s, (f: Run) => toRunnable(f)))
    }

    /**
     * <b>Alias</b> for the same function `runAsync$`.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of absolute closures to call. If empty or `null` - this method
     *      is no-op and finished future over `null` will be returned.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @see `org.gridgain.grid.GridProjection.call(...)`
     */
    def *?(mode: GCM, @Nullable s: Seq[Run], @Nullable p: NF): GridFuture[_] = {
        runAsync$(mode, s, p)
    }

    /**
     * Asynchronous closure call on this projection without return value. This call will
     * return immediately with the future that can be used to wait asynchronously for the results.
     *
     * @param mode Closure call mode.
     * @param s Optional absolute closure to call. If `null` - this method
     *      is no-op and finished future over `null` will be returned.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @see `org.gridgain.grid.GridProjection.run(...)`
     */
    def runAsync$(mode: GCM, @Nullable s: Run, @Nullable p: NF): GridFuture[_] = {
        runAsync$(mode, Seq(s), p)
    }

    /**
     * <b>Alias</b> for the same function `runAsync$`.
     *
     * @param mode Closure call mode.
     * @param s Optional absolute closure to call. If `null` - this method
     *      is no-op and finished future over `null` will be returned.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @see `org.gridgain.grid.GridProjection.run(...)`
     */
    def *?(mode: GCM, @Nullable s: Run, @Nullable p: NF): GridFuture[_] = {
        runAsync$(mode, s, p)
    }

    /**
     * Asynchronous closures execution on this projection with reduction. This call will
     * return immediately with the future that can be used to wait asynchronously for the results.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or `null` - this method
     *      is no-op and will return finished future over `null`.
     * @param r Optional reduction function. If `null` - this method
     *      is no-op and will return finished future over `null`.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Future over the reduced result or `null` (see above).
     * @see `org.gridgain.grid.GridProjection.reduce(...)`
     */
    def reduceAsync$[R1, R2](mode: GCM, s: Seq[Call[R1]], r: Seq[R1] => R2, @Nullable p: NF): GridFuture[R2] = {
        assert(mode != null && s != null && r != null)

        value.forPredicate(p).compute().reduce(mode, toJavaCollection(s, (f: Call[R1]) => toCallable(f)), r)
    }

    /**
     * <b>Alias</b> for the same function `reduceAsync$`.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or `null` - this method
     *      is no-op and will return finished future over `null`.
     * @param r Optional reduction function. If `null` - this method
     *      is no-op and will return finished future over `null`.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Future over the reduced result or `null` (see above).
     * @see `org.gridgain.grid.GridProjection.reduce(...)`
     */
    def @?[R1, R2](mode: GCM, s: Seq[Call[R1]], r: Seq[R1] => R2, @Nullable p: NF): GridFuture[R2] = {
        reduceAsync$(mode, s, r, p)
    }

    /**
     * Synchronous closures execution on this projection with reduction.
     * This call will block until all results are reduced.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or `null` - this method
     *      is no-op and will return `null`.
     * @param r Optional reduction function. If `null` - this method
     *      is no-op and will return `null`.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Reduced result or `null` (see above).
     * @see `org.gridgain.grid.GridProjection.reduce(...)`
     */
    def reduce$[R1, R2](mode: GCM, @Nullable s: Seq[Call[R1]], @Nullable r: Seq[R1] => R2, @Nullable p: NF): R2 =
        reduceAsync$(mode, s, r, p).get

    /*
     * Utility shortcuts.
     */
    def reduceOpt[R1, R2](mode: GCM, @Nullable s: Seq[Call[R1]])(@Nullable r: Seq[R1] => R2, @Nullable p: NF): Option[R2] =
        Option(reduce$(mode, s, r, p))
    def ucastReduce[R1, R2](@Nullable s: Seq[Call[R1]])(@Nullable r: Seq[R1] => R2, @Nullable p: NF): R2 =
        reduce$(UNICAST, s, r, p)
    def bcastReduce[R1, R2](@Nullable s: Seq[Call[R1]])(@Nullable r: Seq[R1] => R2, @Nullable p: NF): R2 =
        reduce$(BROADCAST, s, r, p)
    def spreadReduce[R1, R2](@Nullable s: Seq[Call[R1]])(@Nullable r: Seq[R1] => R2, @Nullable p: NF): R2 =
        reduce$(SPREAD, s, r, p)
    def balanceReduce[R1, R2](@Nullable s: Seq[Call[R1]])(@Nullable r: Seq[R1] => R2, @Nullable p: NF): R2 =
        reduce$(BALANCE, s, r, p)
    def ucastReduceOpt[R1, R2](@Nullable s: Seq[Call[R1]])(@Nullable r: Seq[R1] => R2, @Nullable p: NF): Option[R2] =
        Option(reduce$(UNICAST, s, r, p))
    def bcastReduceOpt[R1, R2](@Nullable s: Seq[Call[R1]])(@Nullable r: Seq[R1] => R2, @Nullable p: NF): Option[R2] =
        Option(reduce$(BROADCAST, s, r, p))
    def spreadReduceOpt[R1, R2](@Nullable s: Seq[Call[R1]])(@Nullable r: Seq[R1] => R2, @Nullable p: NF): Option[R2] =
        Option(reduce$(SPREAD, s, r, p))
    def balanceReduceOpt[R1, R2](@Nullable s: Seq[Call[R1]])(@Nullable r: Seq[R1] => R2, @Nullable p: NF): Option[R2] =
        Option(reduce$(BALANCE, s, r, p))

    /**
     * Synchronous closures execution on this projection with reduction.
     * This call will block until all results are reduced. If this projection
     * is empty than `dflt` closure will be executed and its result returned.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or `null` - this method
     *      is no-op and will return `null`.
     * @param r Optional reduction function. If `null` - this method
     *      is no-op and will return `null`.
     * @param dflt Closure to execute if projection is empty.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Reduced result or `null` (see above).
     * @see `org.gridgain.grid.GridProjection.reduce(...)`
     */
    def reduceSafe[R1, R2](mode: GCM, @Nullable s: Seq[Call[R1]], @Nullable r: Seq[R1] => R2,
        dflt: () => R2, @Nullable p: NF): R2 = {
        assert(dflt != null)

        try
            reduceAsync$(mode, s, r, p).get
        catch {
            case _: GridEmptyProjectionException => dflt()
        }
    }

    /**
     * <b>Alias</b> for the same function `reduce$`.
     *
     * @param mode Closure call mode.
     * @param s Optional sequence of closures to call. If empty or `null` - this method
     *      is no-op and will return `null`.
     * @param r Optional reduction function. If `null` - this method is no-op and will return `null`.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Reduced result or `null` (see above).
     * @see `org.gridgain.grid.GridProjection.reduce(...)`
     */
    def @<[R1, R2](mode: GCM, @Nullable s: Seq[Call[R1]], @Nullable r: Seq[R1] => R2, @Nullable p: NF): R2 =
        reduceAsync$(mode, s, r, p).get

    /**
     * Asynchronous closures execution on this projection with mapping and reduction.
     * This call will return immediately with the future that can be used to wait asynchronously for
     * the results.
     *
     * @param m Mapping function.
     * @param s Optional sequence of closures to call. If empty or `null` - this method
     *      is no-op and will return `null`.
     * @param r Optional reduction function. If `null` - this method is no-op and will return `null`.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Future over the reduced result or `null` (see above).
     * @see `org.gridgain.grid.GridProjection.mapreduce(...)`
     */
    def mapreduceAsync$[R1, R2](m: Seq[GridNode] => (java.util.concurrent.Callable[R1] => GridNode),
        @Nullable s: Seq[Call[R1]], @Nullable r: Seq[R1] => R2, @Nullable p: NF): GridFuture[R2] = {
        assert(m != null)

        value.forPredicate(p).compute().mapreduce(toMapper(m), toJavaCollection[Call[R1], java.util.concurrent.Callable[R1]](s,
            (f: Call[R1]) => toCallable(f)), toReducer(r))
    }

    /**
     * Synchronous closures execution on this projection with mapping and reduction.
     * This call will block until all results are reduced.
     *
     * @param m Mapping function.
     * @param s Optional sequence of closures to call. If empty or `null` - this method
     *      is no-op and will return `null`.
     * @param r Optional reduction function. If `null` - this method
     *      is no-op and will return `null`.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Reduced result or `null` (see above).
     * @see `org.gridgain.grid.GridProjection.mapreduce(...)`
     */
    def mapreduce$[R1, R2](m: Seq[GridNode] => (java.util.concurrent.Callable[R1] => GridNode),
        @Nullable s: Seq[Call[R1]], @Nullable r: Seq[R1] => R2, @Nullable p: NF): R2 =
        mapreduceAsync$(m, s, r, p).get

    /**
     * Synchronous closures execution on this projection with mapping and reduction.
     * This call will block until all results are reduced. If this projection
     * is empty than `dflt` closure will be executed and its result returned.
     *
     * @param m Mapping function.
     * @param s Optional sequence of closures to call. If empty or `null` - this method
     *      is no-op and will return `null`.
     * @param r Optional reduction function. If `null` - this method
     *      is no-op and will return `null`.
     * @param dflt Closure to execute if projection is empty.
     * @param p Optional node filter predicate. If `null` provided - all nodes in projection will be used.
     * @return Reduced result or `null` (see above).
     * @see `org.gridgain.grid.GridProjection.mapreduce(...)`
     */
    def mapreduceSafe[R1, R2](m: Seq[GridNode] => (java.util.concurrent.Callable[R1] => GridNode),
        @Nullable s: Seq[Call[R1]], @Nullable r: Seq[R1] => R2, dflt: () => R2, @Nullable p: NF): R2 = {
        assert(dflt != null)

        try
            mapreduce$(m, s, r, p)
        catch {
            case _: GridEmptyProjectionException => dflt()
        }
    }

    /**
     * Executes given closure on the nodes where data for provided affinity keys are located. This
     * is known as affinity co-location between compute grid (a closure) and in-memory data grid
     * (value with affinity key). Note that implementation of multiple executions of the same closure will
     * be wrapped as a single task that splits into multiple `job`s that will be mapped to nodes
     * with provided affinity keys.
     *
     * This method will block until its execution is complete or an exception is thrown.
     * All default SPI implementations configured for this grid instance will be
     * used (i.e. failover, load balancing, collision resolution, etc.).
     * Note that if you need greater control on any aspects of Java code execution on the grid
     * you should implement `GridComputeTask` which will provide you with full control over the execution.
     *
     * Notice that `Runnable` and `Callable` implementations must support serialization as required
     * by the configured marshaller. For example, JDK marshaller will require that implementations would
     * be serializable. Other marshallers, e.g. JBoss marshaller, may not have this limitation. Please consult
     * with specific marshaller implementation for the details. Note that all closures and predicates in
     * `org.gridgain.grid.lang` package are serializable and can be freely used in the distributed
     * context with all marshallers currently shipped with GridGain.
     *
     * @param cacheName Name of the cache to use for affinity co-location.
     * @param affKeys Collection of affinity keys. All dups will be ignored. If `null` or empty
     *      this method is no-op.
     * @param r Closure to affinity co-located on the node with given affinity key and execute.
     *      If `null` - this method is no-op.
     * @param p Optional filtering predicate. If `null` provided - all nodes in this projection will be used for topology.
     * @throws GridException Thrown in case of any error.
     * @throws GridEmptyProjectionException Thrown in case when this projection is empty.
     *      Note that in case of dynamic projection this method will take a snapshot of all the
     *      nodes at the time of this call, apply all filtering predicates, if any, and if the
     *      resulting collection of nodes is empty - the exception will be thrown.
     * @throws GridInterruptedException Subclass of `GridException` thrown if the wait was interrupted.
     * @throws GridFutureCancelledException Subclass of `GridException` thrown if computation was cancelled.
     */
    def affinityRun$(cacheName: String, @Nullable affKeys: Seq[_], @Nullable r: Run,
        @Nullable p: NF) {
        value.forPredicate(p).compute().affinityRun(cacheName, toJavaCollection(affKeys), toOutClosureX(toFactory(r)))
    }

    /**
     * Executes given closure on the nodes where data for provided affinity keys are located. This
     * is known as affinity co-location between compute grid (a closure) and in-memory data grid
     * (value with affinity key). Note that implementation of multiple executions of the same closure will
     * be wrapped as a single task that splits into multiple `job`s that will be mapped to nodes
     * with provided affinity keys.
     *
     * Unlike its sibling method `affinityRun(String, Collection, Runnable, GridPredicate[])` this method does
     * not block and returns immediately with future. All default SPI implementations
     * configured for this grid instance will be used (i.e. failover, load balancing, collision resolution, etc.).
     * Note that if you need greater control on any aspects of Java code execution on the grid
     * you should implement `GridComputeTask` which will provide you with full control over the execution.
     *
     * Note that class `GridAbsClosure` implements `Runnable` and class `GridOutClosure`
     * implements `Callable` interface. Note also that class `GridFunc` and typedefs provide rich
     * APIs and functionality for closures and predicates based processing in GridGain. While Java interfaces
     * `Runnable` and `Callable` allow for lowest common denominator for APIs - it is advisable
     * to use richer Functional Programming support provided by GridGain available in `org.gridgain.grid.lang`
     * package.
     *
     * Notice that `Runnable` and `Callable` implementations must support serialization as required
     * by the configured marshaller. For example, JDK marshaller will require that implementations would
     * be serializable. Other marshallers, e.g. JBoss marshaller, may not have this limitation. Please consult
     * with specific marshaller implementation for the details. Note that all closures and predicates in
     * `org.gridgain.grid.lang` package are serializable and can be freely used in the distributed
     * context with all marshallers currently shipped with GridGain.
     *
     * @param cacheName Name of the cache to use for affinity co-location.
     * @param affKeys Collection of affinity keys. All dups will be ignored. If `null` or
     *      empty - this method is no-op.
     * @param r Closure to affinity co-located on the node with given affinity key and execute.
     *      If `null` - this method is no-op.
     * @param p Optional filtering predicate. If `null` provided - all nodes in this projection will be used for topology.
     * @throws GridException Thrown in case of any error.
     * @throws GridEmptyProjectionException Thrown in case when this projection is empty.
     *      Note that in case of dynamic projection this method will take a snapshot of all the
     *      nodes at the time of this call, apply all filtering predicates, if any, and if the
     *      resulting collection of nodes is empty - the exception will be thrown.
     * @return Non-cancellable future of this execution.
     * @throws GridInterruptedException Subclass of `GridException` thrown if the wait was interrupted.
     * @throws GridFutureCancelledException Subclass of `GridException` thrown if computation was cancelled.
     */
    def affinityRunAsync$(cacheName: String, @Nullable affKeys: Seq[_], @Nullable r: Run,
        @Nullable p: NF): GridFuture[_] = {
        value.forPredicate(p).compute().affinityRun(cacheName, toJavaCollection(affKeys), toOutClosureX(toFactory(r)))
    }
}
