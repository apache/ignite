/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.scalar

import org.apache.ignite.{IgniteCache, Ignite}
import org.apache.ignite.cluster.ClusterGroup
import org.apache.ignite.compute.ComputeJob
import org.apache.ignite.internal.util.lang._
import org.apache.ignite.lang._
import org.apache.ignite.scalar.lang._
import org.apache.ignite.scalar.pimps._
import org.jetbrains.annotations._

import java.util.TimerTask

import scala.collection._
import scala.util.control.Breaks._

/**
 * ==Overview==
 * Mixin for `scalar` object providing `implicit` and `explicit` conversions between
 * Java and Scala Ignite components.
 *
 * It is very important to review this class as it defines what `implicit` conversions
 * will take place when using Scalar. Note that object `scalar` mixes in this
 * trait and therefore brings with it all implicits into the scope.
 */
trait ScalarConversions {
    /**
     * Helper transformer from Java collection to Scala sequence.
     *
     * @param c Java collection to transform.
     * @param f Transforming function.
     */
    def toScalaSeq[A, B](@Nullable c: java.util.Collection[A], f: A => B): Seq[B] = {
        assert(f != null)

        if (c == null)
            return null

        val iter = c.iterator

        val lst = new mutable.ListBuffer[B]

        while (iter.hasNext) lst += f(iter.next)

        lst.toSeq
    }

    /**
     * Helper transformer from Java iterator to Scala sequence.
     *
     * @param i Java iterator to transform.
     * @param f Transforming function.
     */
    def toScalaSeq[A, B](@Nullable i: java.util.Iterator[A], f: A => B): Seq[B] = {
        assert(f != null)

        if (i == null)
            return null

        val lst = new mutable.ListBuffer[B]

        while (i.hasNext) lst += f(i.next)

        lst.toSeq
    }

    /**
     * Helper converter from Java iterator to Scala sequence.
     *
     * @param i Java iterator to convert.
     */
    def toScalaSeq[A](@Nullable i: java.util.Iterator[A]): Seq[A] =
        toScalaSeq(i, (e: A) => e)

    /**
     * Helper transformer from Java iterable to Scala sequence.
     *
     * @param i Java iterable to transform.
     * @param f Transforming function.
     */
    def toScalaSeq[A, B](@Nullable i: java.lang.Iterable[A], f: A => B): Seq[B] = {
        assert(f != null)

        if (i == null) return null

        toScalaSeq(i.iterator, f)
    }

    /**
     * Helper converter from Java iterable to Scala sequence.
     *
     * @param i Java iterable to convert.
     */
    def toScalaSeq[A](@Nullable i: java.lang.Iterable[A]): Seq[A] =
        toScalaSeq(i, (e: A) => e)

//    /**
//     * Helper converter from Java collection to Scala sequence.
//     *
//     * @param c Java collection to convert.
//     */
//    def toScalaSeq[A](@Nullable c: java.util.Collection[A]): Seq[A] =
//        toScalaSeq(c, (e: A) => e)

    /**
     * Helper converter from Java entry collection to Scala iterable of pair.
     *
     * @param c Java collection to convert.
     */
    def toScalaItr[K, V](@Nullable c: java.util.Collection[java.util.Map.Entry[K, V]]): Iterable[(K, V)] = {
        val lst = new mutable.ListBuffer[(K, V)]

        c.toArray().foreach {
            case f: java.util.Map.Entry[K, V] => lst += Tuple2(f.getKey(), f.getValue())
        }

        lst
    }

    /**
     * Helper transformer from Scala sequence to Java collection.
     *
     * @param s Scala sequence to transform.
     * @param f Transforming function.
     */
    def toJavaCollection[A, B](@Nullable s: Seq[A], f: A => B): java.util.Collection[B] = {
        assert(f != null)

        if (s == null) return null

        val lst = new java.util.ArrayList[B](s.length)

        s.foreach(a => lst.add(f(a)))

        lst
    }

    /**
     * Helper converter from Scala sequence to Java collection.
     *
     * @param s Scala sequence to convert.
     */
    def toJavaCollection[A](@Nullable s: Seq[A]): java.util.Collection[A] =
        toJavaCollection(s, (e: A) => e)

    /**
     * Helper transformer from Scala iterator to Java collection.
     *
     * @param i Scala iterator to transform.
     * @param f Transforming function.
     */
    def toJavaCollection[A, B](@Nullable i: Iterator[A], f: A => B): java.util.Collection[B] = {
        assert(f != null)

        if (i == null) return null

        val lst = new java.util.ArrayList[B]

        i.foreach(a => lst.add(f(a)))

        lst
    }

    /**
     * Converts from `Symbol` to `String`.
     *
     * @param s Symbol to convert.
     */
    implicit def fromSymbol(s: Symbol): String =
        if (s == null)
            null
        else
            s.toString().substring(1)

    /**
     * Introduction of `^^` operator for `Any` type that will call `break`.
     *
     * @param v `Any` value.
     */
    implicit def toReturnable(v: Any) = new {
        // Ignore the warning below.
        def ^^ {
            break()
        }
    }


    /**
     * Explicit converter for `TimerTask`. Note that since `TimerTask` implements `Runnable`
     * we can't use the implicit conversion.
     *
     * @param f Closure to convert.
     * @return Time task instance.
     */
    def timerTask(f: => Unit): TimerTask = new TimerTask {
        def run() {
            f
        }
    }

    /**
     * Extension for `Tuple2`.
     *
     * @param t Tuple to improve.
     */
    implicit def toTuple2x[T1, T2](t: (T1, T2)) = new {
        def isSome: Boolean =
            t._1 != null || t._2 != null

        def isNone: Boolean =
            !isSome

        def isAll: Boolean =
            t._1 != null && t._2 != null

        def opt1: Option[T1] =
            Option(t._1)

        def opt2: Option[T2] =
            Option(t._2)
    }

    /**
     * Extension for `Tuple3`.
     *
     * @param t Tuple to improve.
     */
    implicit def toTuple3x[T1, T2, T3](t: (T1, T2, T3)) = new {
        def isSome: Boolean =
            t._1 != null || t._2 != null || t._3 != null

        def isNone: Boolean =
            !isSome

        def isAll: Boolean =
            t._1 != null && t._2 != null && t._3 != null

        def opt1: Option[T1] =
            Option(t._1)

        def opt2: Option[T2] =
            Option(t._2)

        def opt3: Option[T3] =
            Option(t._3)
    }

//    /**
//     * Implicit converter from cache KV-pair predicate to cache entry predicate. Note that predicate
//     * will use peek()
//     *
//     * @param p Cache KV-pair predicate to convert.
//     */
//    implicit def toEntryPred[K, V](p: (K, V) => Boolean): (_ >: Cache.Entry[K, V]) => Boolean =
//        (e: Cache.Entry[K, V]) => p(e.getKey, e.getValue)

    /**
     * Implicit converter from vararg of one-argument Scala functions to Java `GridPredicate`s.
     *
     * @param s Sequence of one-argument Scala functions to convert.
     */
    implicit def toVarArgs[T](s: Seq[T => Boolean]): Seq[IgnitePredicate[_ >: T]] =
        s.map((f: T => Boolean) => toPredicate(f))

    /**
     * Implicit converter from vararg of two-argument Scala functions to Java `GridPredicate2`s.
     *
     * @param s Sequence of two-argument Scala functions to convert.
     */
    implicit def toVarArgs2[T1, T2](s: Seq[(T1, T2) => Boolean]): Seq[IgniteBiPredicate[_ >: T1, _ >: T2]] =
        s.map((f: (T1, T2) => Boolean) => toPredicate2(f))

    /**
     * Implicit converter from vararg of three-argument Scala functions to Java `GridPredicate3`s.
     *
     * @param s Sequence of three-argument Scala functions to convert.
     */
    implicit def toVarArgs3[T1, T2, T3](s: Seq[(T1, T2, T3) => Boolean]):
        Seq[GridPredicate3[_ >: T1, _ >: T2, _ >: T3]] =
        s.map((f: (T1, T2, T3) => Boolean) => toPredicate3(f))

    /**
     * Implicit converter from Scala function and Java `GridReducer`.
     *
     * @param r Scala function to convert.
     */
    implicit def toReducer[E, R](r: Seq[E] => R): IgniteReducer[E, R] =
        new ScalarReducer(r)

    /**
     * Implicit converter from Java `GridReducer` to Scala function.
     *
     * @param r Java `GridReducer` to convert.
     */
    implicit def fromReducer[E, R](r: IgniteReducer[E, R]): Seq[E] => R =
        new ScalarReducerFunction[E, R](r)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param r Java-side reducer to pimp.
      */
    implicit def reducerDotScala[E, R](r: IgniteReducer[E, R]) = new {
        def scala: Seq[E] => R =
            fromReducer(r)
    }

    /**
     * Implicit converter from Scala function and Java `GridReducer2`.
     *
     * @param r Scala function to convert.
     */
    implicit def toReducer2[E1, E2, R](r: (Seq[E1], Seq[E2]) => R): IgniteReducer2[E1, E2, R] =
        new ScalarReducer2(r)

    /**
     * Implicit converter from Java `GridReducer2` to Scala function.
     *
     * @param r Java `GridReducer2` to convert.
     */
    implicit def fromReducer2[E1, E2, R](r: IgniteReducer2[E1, E2, R]): (Seq[E1], Seq[E2]) => R =
        new ScalarReducer2Function[E1, E2, R](r)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param r Java-side reducer to pimp.
      */
    implicit def reducer2DotScala[E1, E2, R](r: IgniteReducer2[E1, E2, R]) = new {
        def scala: (Seq[E1], Seq[E2]) => R =
            fromReducer2(r)
    }

    /**
     * Implicit converter from Scala function and Java `GridReducer3`.
     *
     * @param r Scala function to convert.
     */
    implicit def toReducer3[E1, E2, E3, R](r: (Seq[E1], Seq[E2], Seq[E3]) => R): IgniteReducer3[E1, E2, E3, R] =
        new ScalarReducer3(r)

    /**
     * Implicit converter from Java `GridReducer3` to Scala function.
     *
     * @param r Java `GridReducer3` to convert.
     */
    implicit def fromReducer3[E1, E2, E3, R](r: IgniteReducer3[E1, E2, E3, R]): (Seq[E1], Seq[E2], Seq[E3]) => R =
        new ScalarReducer3Function[E1, E2, E3, R](r)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param r Java-side reducer to pimp.
      */
    implicit def reducer3DotScala[E1, E2, E3, R](r: IgniteReducer3[E1, E2, E3, R]) = new {
        def scala: (Seq[E1], Seq[E2], Seq[E3]) => R =
            fromReducer3(r)
    }

    /**
     * Implicit converter from `Grid` to `ScalarGridPimp` "pimp".
     *
     * @param impl Grid to convert.
     */
    implicit def toScalarGrid(impl: Ignite): ScalarGridPimp =
        ScalarGridPimp(impl)

    /**
     * Implicit converter from `GridProjection` to `ScalarProjectionPimp` "pimp".
     *
     * @param impl Grid projection to convert.
     */
    implicit def toScalarProjection(impl: ClusterGroup): ScalarProjectionPimp[ClusterGroup] =
        ScalarProjectionPimp(impl)

    /**
     * Implicit converter from `Cache` to `ScalarCachePimp` "pimp".
     *
     * @param impl Grid cache to convert.
     */
    implicit def toScalarCache[K, V](impl: IgniteCache[K, V]): ScalarCachePimp[K, V] =
        ScalarCachePimp[K, V](impl)

    /**
     * Implicit converter from Scala function to `ComputeJob`.
     *
     * @param f Scala function to convert.
     */
    implicit def toJob(f: () => Any): ComputeJob =
        new ScalarJob(f)

    /**
     * Implicit converter from Scala tuple to `GridTuple2`.
     *
     * @param t Scala tuple to convert.
     */
    implicit def toTuple2[A, B](t: (A, B)): IgniteBiTuple[A, B] =
        new IgniteBiTuple[A, B](t._1, t._2)

    /**
     * Implicit converter from `GridTuple2` to Scala tuple.
     *
     * @param t `GridTuple2` to convert.
     */
    implicit def fromTuple2[A, B](t: IgniteBiTuple[A, B]): (A, B) =
        (t.get1, t.get2)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param t Java-side tuple to pimp.
      */
    implicit def tuple2DotScala[A, B](t: IgniteBiTuple[A, B]) = new {
        def scala: (A, B) =
            fromTuple2(t)
    }

    /**
     * Implicit converter from Scala tuple to `GridTuple3`.
     *
     * @param t Scala tuple to convert.
     */
    implicit def toTuple3[A, B, C](t: (A, B, C)): GridTuple3[A, B, C] =
        new GridTuple3[A, B, C](t._1, t._2, t._3)

    /**
     * Implicit converter from `GridTuple3` to Scala tuple.
     *
     * @param t `GridTuple3` to convert.
     */
    implicit def fromTuple3[A, B, C](t: GridTuple3[A, B, C]): (A, B, C) =
        (t.get1, t.get2, t.get3)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param t Java-side tuple to pimp.
      */
    implicit def tuple3DotScala[A, B, C](t: GridTuple3[A, B, C]) = new {
        def scala: (A, B, C) =
            fromTuple3(t)
    }

    /**
     * Implicit converter from Scala tuple to `GridTuple4`.
     *
     * @param t Scala tuple to convert.
     */
    implicit def toTuple4[A, B, C, D](t: (A, B, C, D)): GridTuple4[A, B, C, D] =
        new GridTuple4[A, B, C, D](t._1, t._2, t._3, t._4)

    /**
     * Implicit converter from `GridTuple4` to Scala tuple.
     *
     * @param t `GridTuple4` to convert.
     */
    implicit def fromTuple4[A, B, C, D](t: GridTuple4[A, B, C, D]): (A, B, C, D) =
        (t.get1, t.get2, t.get3, t.get4)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param t Java-side tuple to pimp.
      */
    implicit def tuple4DotScala[A, B, C, D](t: GridTuple4[A, B, C, D]) = new {
        def scala: (A, B, C, D) =
            fromTuple4(t)
    }

    /**
     * Implicit converter from Scala tuple to `GridTuple5`.
     *
     * @param t Scala tuple to convert.
     */
    implicit def toTuple5[A, B, C, D, E](t: (A, B, C, D, E)): GridTuple5[A, B, C, D, E] =
        new GridTuple5[A, B, C, D, E](t._1, t._2, t._3, t._4, t._5)

    /**
     * Implicit converter from `GridTuple5` to Scala tuple.
     *
     * @param t `GridTuple5` to convert.
     */
    implicit def fromTuple5[A, B, C, D, E](t: GridTuple5[A, B, C, D, E]): (A, B, C, D, E) =
        (t.get1, t.get2, t.get3, t.get4, t.get5)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param t Java-side tuple to pimp.
      */
    implicit def tuple5DotScala[A, B, C, D, E](t: GridTuple5[A, B, C, D, E]) = new {
        def scala: (A, B, C, D, E) =
            fromTuple5(t)
    }

    /**
     * Implicit converter from Scala function to `GridInClosure`.
     *
     * @param f Scala function to convert.
     */
    implicit def toInClosure[T](f: T => Unit): IgniteInClosure[T] =
        f match {
            case (p: ScalarInClosureFunction[T]) => p.inner
            case _ => new ScalarInClosure[T](f)
        }

    /**
     * Implicit converter from Scala function to `GridInClosureX`.
     *
     * @param f Scala function to convert.
     */
    def toInClosureX[T](f: T => Unit): IgniteInClosureX[T] =
        f match {
            case (p: ScalarInClosureXFunction[T]) => p.inner
            case _ => new ScalarInClosureX[T](f)
        }

    /**
     * Implicit converter from `GridInClosure` to Scala wrapping function.
     *
     * @param f Grid closure to convert.
     */
    implicit def fromInClosure[T](f: IgniteInClosure[T]): T => Unit =
        new ScalarInClosureFunction[T](f)

    /**
     * Implicit converter from `GridInClosureX` to Scala wrapping function.
     *
     * @param f Grid closure to convert.
     */
    implicit def fromInClosureX[T](f: IgniteInClosureX[T]): T => Unit =
        new ScalarInClosureXFunction[T](f)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param f Java-side closure to pimp.
      */
    implicit def inClosureDotScala[T](f: IgniteInClosure[T]) = new {
        def scala: T => Unit =
            fromInClosure(f)
    }

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param f Java-side closure to pimp.
      */
    implicit def inClosureXDotScala[T](f: IgniteInClosureX[T]) = new {
        def scala: T => Unit =
            fromInClosureX(f)
    }

    /**
     * Implicit converter from Scala function to `GridInClosure2`.
     *
     * @param f Scala function to convert.
     */
    implicit def toInClosure2[T1, T2](f: (T1, T2) => Unit): IgniteBiInClosure[T1, T2] =
        f match {
            case (p: ScalarInClosure2Function[T1, T2]) => p.inner
            case _ => new ScalarInClosure2[T1, T2](f)
        }

    /**
     * Implicit converter from Scala function to `GridInClosure2X`.
     *
     * @param f Scala function to convert.
     */
    implicit def toInClosure2X[T1, T2](f: (T1, T2) => Unit): IgniteInClosure2X[T1, T2] =
        f match {
            case (p: ScalarInClosure2XFunction[T1, T2]) => p.inner
            case _ => new ScalarInClosure2X[T1, T2](f)
        }

    /**
     * Implicit converter from `GridInClosure2` to Scala wrapping function.
     *
     * @param f Grid closure to convert.
     */
    implicit def fromInClosure2[T1, T2](f: IgniteBiInClosure[T1, T2]): (T1, T2) => Unit =
        new ScalarInClosure2Function(f)

    /**
     * Implicit converter from `GridInClosure2X` to Scala wrapping function.
     *
     * @param f Grid closure to convert.
     */
    implicit def fromInClosure2X[T1, T2](f: IgniteInClosure2X[T1, T2]): (T1, T2) => Unit =
        new ScalarInClosure2XFunction(f)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param f Java-side closure to pimp.
      */
    implicit def inClosure2DotScala[T1, T2](f: IgniteBiInClosure[T1, T2]) = new {
        def scala: (T1, T2) => Unit =
            fromInClosure2(f)
    }

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param f Java-side closure to pimp.
      */
    implicit def inClosure2XDotScala[T1, T2](f: IgniteInClosure2X[T1, T2]) = new {
        def scala: (T1, T2) => Unit =
            fromInClosure2X(f)
    }

    /**
     * Implicit converter from Scala function to `GridInClosure3`.
     *
     * @param f Scala function to convert.
     */
    implicit def toInClosure3[T1, T2, T3](f: (T1, T2, T3) => Unit): GridInClosure3[T1, T2, T3] =
        f match {
            case (p: ScalarInClosure3Function[T1, T2, T3]) => p.inner
            case _ => new ScalarInClosure3[T1, T2, T3](f)
        }

    /**
     * Implicit converter from Scala function to `GridInClosure3X`.
     *
     * @param f Scala function to convert.
     */
    def toInClosure3X[T1, T2, T3](f: (T1, T2, T3) => Unit): GridInClosure3X[T1, T2, T3] =
        f match {
            case (p: ScalarInClosure3XFunction[T1, T2, T3]) => p.inner
            case _ => new ScalarInClosure3X[T1, T2, T3](f)
        }

    /**
     * Implicit converter from `GridInClosure3` to Scala wrapping function.
     *
     * @param f Grid closure to convert.
     */
    implicit def fromInClosure3[T1, T2, T3](f: GridInClosure3[T1, T2, T3]): (T1, T2, T3) => Unit =
        new ScalarInClosure3Function(f)

    /**
     * Implicit converter from `GridInClosure3X` to Scala wrapping function.
     *
     * @param f Grid closure to convert.
     */
    implicit def fromInClosure3X[T1, T2, T3](f: GridInClosure3X[T1, T2, T3]): (T1, T2, T3) => Unit =
        new ScalarInClosure3XFunction(f)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param f Java-side closure to pimp.
      */
    implicit def inClosure3DotScala[T1, T2, T3](f: GridInClosure3[T1, T2, T3]) = new {
        def scala: (T1, T2, T3) => Unit =
            fromInClosure3(f)
    }

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param f Java-side closure to pimp.
      */
    implicit def inClosure3XDotScala[T1, T2, T3](f: GridInClosure3X[T1, T2, T3]) = new {
        def scala: (T1, T2, T3) => Unit =
            fromInClosure3X(f)
    }

    /**
     * Implicit converter from Scala function to `GridOutClosure`.
     *
     * @param f Scala function to convert.
     */
    implicit def toCallable[R](f: () => R): IgniteCallable[R] =
        f match {
            case p: ScalarOutClosureFunction[R] => p.inner
            case _ => new ScalarOutClosure[R](f)
        }

    /**
     * Implicit converter from Scala function to `GridOutClosureX`.
     *
     * @param f Scala function to convert.
     */
    def toOutClosureX[R](f: () => R): IgniteOutClosureX[R] =
        f match {
            case (p: ScalarOutClosureXFunction[R]) => p.inner
            case _ => new ScalarOutClosureX[R](f)
        }

    /**
     * Implicit converter from `GridOutClosure` to Scala wrapping function.
     *
     * @param f Grid closure to convert.
     */
    implicit def fromOutClosure[R](f: IgniteCallable[R]): () => R =
        new ScalarOutClosureFunction[R](f)

    /**
     * Implicit converter from `GridOutClosureX` to Scala wrapping function.
     *
     * @param f Grid closure to convert.
     */
    implicit def fromOutClosureX[R](f: IgniteOutClosureX[R]): () => R =
        new ScalarOutClosureXFunction[R](f)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param f Java-side closure to pimp.
      */
    implicit def outClosureDotScala[R](f: IgniteCallable[R]) = new {
        def scala: () => R =
            fromOutClosure(f)
    }

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param f Java-side closure to pimp.
      */
    implicit def outClosureXDotScala[R](f: IgniteOutClosureX[R]) = new {
        def scala: () => R =
            fromOutClosureX(f)
    }

    /**
     * Implicit converter from Scala function to `GridAbsClosure`.
     *
     * @param f Scala function to convert.
     */
    implicit def toRunnable(f: () => Unit): IgniteRunnable =
        f match {
            case (f: ScalarAbsClosureFunction) => f.inner
            case _ => new ScalarAbsClosure(f)
        }

    /**
     * Implicit converter from Scala function to `GridAbsClosureX`.
     *
     * @param f Scala function to convert.
     */
    def toAbsClosureX(f: () => Unit): GridAbsClosureX =
        f match {
            case (f: ScalarAbsClosureXFunction) => f.inner
            case _ => new ScalarAbsClosureX(f)
        }

    /**
     * Implicit converter from `GridAbsClosure` to Scala wrapping function.
     *
     * @param f Grid closure to convert.
     */
    implicit def fromAbsClosure(f: GridAbsClosure): () => Unit =
        new ScalarAbsClosureFunction(f)

    /**
     * Implicit converter from `GridAbsClosureX` to Scala wrapping function.
     *
     * @param f Grid closure to convert.
     */
    implicit def fromAbsClosureX(f: GridAbsClosureX): () => Unit =
        new ScalarAbsClosureXFunction(f)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param f Java-side absolute closure to pimp.
      */
    implicit def absClosureDotScala(f: GridAbsClosure) = new {
        def scala: () => Unit =
            fromAbsClosure(f)
    }

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param f Java-side absolute closure to pimp.
      */
    implicit def absClosureXDotScala(f: GridAbsClosureX) = new {
        def scala: () => Unit =
            fromAbsClosureX(f)
    }

    /**
     * Implicit converter from Scala predicate to `GridAbsPredicate`.
     *
     * @param f Scala predicate to convert.
     */
    implicit def toAbsPredicate(f: () => Boolean): GridAbsPredicate =
        f match {
            case (p: ScalarAbsPredicateFunction) => p.inner
            case _ => new ScalarAbsPredicate(f)
        }

    /**
     * Implicit converter from Scala predicate to `GridAbsPredicateX`.
     *
     * @param f Scala predicate to convert.
     */
    implicit def toAbsPredicateX(f: () => Boolean): GridAbsPredicateX =
        f match {
            case (p: ScalarAbsPredicateXFunction) => p.inner
            case _ => new ScalarAbsPredicateX(f)
        }

    /**
     * Implicit converter from `GridAbsPredicate` to Scala wrapping predicate.
     *
     * @param p Grid predicate to convert.
     */
    implicit def fromAbsPredicate(p: GridAbsPredicate): () => Boolean =
        new ScalarAbsPredicateFunction(p)

    /**
     * Implicit converter from `GridAbsPredicateX` to Scala wrapping predicate.
     *
     * @param p Grid predicate to convert.
     */
    implicit def fromAbsPredicateX(p: GridAbsPredicateX): () => Boolean =
        new ScalarAbsPredicateXFunction(p)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param p Java-side predicate to pimp.
      */
    implicit def absPredicateDotScala(p: GridAbsPredicate) = new {
        def scala: () => Boolean =
            fromAbsPredicate(p)
    }

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param p Java-side predicate to pimp.
      */
    implicit def absPredicateXDotScala(p: GridAbsPredicateX) = new {
        def scala: () => Boolean =
            fromAbsPredicateX(p)
    }

    /**
     * Implicit converter from `java.lang.Runnable` to `GridAbsClosure`.
     *
     * @param r Java runnable to convert.
     */
    implicit def toAbsClosure2(r: java.lang.Runnable): GridAbsClosure =
        GridFunc.as(r)

    /**
     * Implicit converter from Scala predicate to Scala wrapping predicate.
     *
     * @param f Scala predicate to convert.
     */
    implicit def toPredicate[T](f: T => Boolean) =
        f match {
            case null => null
            case (p: ScalarPredicateFunction[T]) => p.inner
            case _ => new ScalarPredicate[T](f)
        }

    /**
     * Implicit converter from Scala predicate to Scala wrapping predicate.
     *
     * @param f Scala predicate to convert.
     */
    def toPredicateX[T](f: T => Boolean) =
        f match {
            case (p: ScalarPredicateXFunction[T]) => p.inner
            case _ => new ScalarPredicateX[T](f)
        }

    /**
     * Implicit converter from `GridPredicate` to Scala wrapping predicate.
     *
     * @param p Grid predicate to convert.
     */
    implicit def fromPredicate[T](p: IgnitePredicate[T]): T => Boolean =
        new ScalarPredicateFunction[T](p)

    /**
     * Implicit converter from `GridPredicate` to Scala wrapping predicate.
     *
     * @param p Grid predicate to convert.
     */
    implicit def fromPredicateX[T](p: IgnitePredicateX[T]): T => Boolean =
        new ScalarPredicateXFunction[T](p)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param p Java-side predicate to pimp.
      */
    implicit def predicateDotScala[T](p: IgnitePredicate[T]) = new {
        def scala: T => Boolean =
            fromPredicate(p)
    }

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param p Java-side predicate to pimp.
      */
    implicit def predicateXDotScala[T](p: IgnitePredicateX[T]) = new {
        def scala: T => Boolean =
            fromPredicateX(p)
    }

    /**
     * Implicit converter from Scala predicate to Scala wrapping predicate.
     *
     * @param f Scala predicate to convert.
     */
    implicit def toPredicate2[T1, T2](f: (T1, T2) => Boolean) =
        f match {
            case (p: ScalarPredicate2Function[T1, T2]) => p.inner
            case _ => new ScalarPredicate2[T1, T2](f)
        }

    /**
     * Implicit converter from Scala predicate to Scala wrapping predicate.
     *
     * @param f Scala predicate to convert.
     */
    def toPredicate2X[T1, T2](f: (T1, T2) => Boolean) =
        f match {
            case (p: ScalarPredicate2XFunction[T1, T2]) => p.inner
            case _ => new ScalarPredicate2X[T1, T2](f)
        }

    /**
     * Implicit converter from `GridPredicate2X` to Scala wrapping predicate.
     *
     * @param p Grid predicate to convert.
     */
    implicit def fromPredicate2[T1, T2](p: IgniteBiPredicate[T1, T2]): (T1, T2) => Boolean =
        new ScalarPredicate2Function[T1, T2](p)

    /**
     * Implicit converter from `GridPredicate2X` to Scala wrapping predicate.
     *
     * @param p Grid predicate to convert.
     */
    implicit def fromPredicate2X[T1, T2](p: IgnitePredicate2X[T1, T2]): (T1, T2) => Boolean =
        new ScalarPredicate2XFunction[T1, T2](p)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param p Java-side predicate to pimp.
      */
    implicit def predicate2DotScala[T1, T2](p: IgniteBiPredicate[T1, T2]) = new {
        def scala: (T1, T2) => Boolean =
            fromPredicate2(p)
    }

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param p Java-side predicate to pimp.
      */
    implicit def predicate2XDotScala[T1, T2](p: IgnitePredicate2X[T1, T2]) = new {
        def scala: (T1, T2) => Boolean =
            fromPredicate2X(p)
    }

    /**
     * Implicit converter from Scala predicate to Scala wrapping predicate.
     *
     * @param f Scala predicate to convert.
     */
    implicit def toPredicate3[T1, T2, T3](f: (T1, T2, T3) => Boolean) =
        f match {
            case (p: ScalarPredicate3Function[T1, T2, T3]) => p.inner
            case _ => new ScalarPredicate3[T1, T2, T3](f)
        }

    /**
     * Implicit converter from Scala predicate to Scala wrapping predicate.
     *
     * @param f Scala predicate to convert.
     */
    def toPredicate32[T1, T2, T3](f: (T1, T2, T3) => Boolean) =
        f match {
            case (p: ScalarPredicate3XFunction[T1, T2, T3]) => p.inner
            case _ => new ScalarPredicate3X[T1, T2, T3](f)
        }

    /**
     * Implicit converter from `GridPredicate3X` to Scala wrapping predicate.
     *
     * @param p Grid predicate to convert.
     */
    implicit def fromPredicate3[T1, T2, T3](p: GridPredicate3[T1, T2, T3]): (T1, T2, T3) => Boolean =
        new ScalarPredicate3Function[T1, T2, T3](p)

    /**
     * Implicit converter from `GridPredicate3X` to Scala wrapping predicate.
     *
     * @param p Grid predicate to convert.
     */
    implicit def fromPredicate3X[T1, T2, T3](p: GridPredicate3X[T1, T2, T3]): (T1, T2, T3) => Boolean =
        new ScalarPredicate3XFunction[T1, T2, T3](p)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param p Java-side predicate to pimp.
      */
    implicit def predicate3DotScala[T1, T2, T3](p: GridPredicate3[T1, T2, T3]) = new {
        def scala: (T1, T2, T3) => Boolean =
            fromPredicate3(p)
    }

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param p Java-side predicate to pimp.
      */
    implicit def predicate3XDotScala[T1, T2, T3](p: GridPredicate3X[T1, T2, T3]) = new {
        def scala: (T1, T2, T3) => Boolean =
            fromPredicate3X(p)
    }

    /**
     * Implicit converter from Scala closure to `GridClosure`.
     *
     * @param f Scala closure to convert.
     */
    implicit def toClosure[A, R](f: A => R): IgniteClosure[A, R] =
        f match {
            case (c: ScalarClosureFunction[A, R]) => c.inner
            case _ => new ScalarClosure[A, R](f)
        }

    /**
     * Implicit converter from Scala closure to `GridClosureX`.
     *
     * @param f Scala closure to convert.
     */
    def toClosureX[A, R](f: A => R): IgniteClosureX[A, R] =
        f match {
            case (c: ScalarClosureXFunction[A, R]) => c.inner
            case _ => new ScalarClosureX[A, R](f)
        }

    /**
     * Implicit converter from `GridClosure` to Scala wrapping closure.
     *
     * @param f Grid closure to convert.
     */
    implicit def fromClosure[A, R](f: IgniteClosure[A, R]): A => R =
        new ScalarClosureFunction[A, R](f)

    /**
     * Implicit converter from `GridClosureX` to Scala wrapping closure.
     *
     * @param f Grid closure to convert.
     */
    implicit def fromClosureX[A, R](f: IgniteClosureX[A, R]): A => R =
        new ScalarClosureXFunction[A, R](f)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param f Java-side closure to pimp.
      */
    implicit def closureDotScala[A, R](f: IgniteClosure[A, R]) = new {
        def scala: A => R =
            fromClosure(f)
    }

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param f Java-side closure to pimp.
      */
    implicit def closureXDotScala[A, R](f: IgniteClosureX[A, R]) = new {
        def scala: A => R =
            fromClosureX(f)
    }

    /**
     * Implicit converter from Scala closure to `GridClosure2`.
     *
     * @param f Scala closure to convert.
     */
    implicit def toClosure2[A1, A2, R](f: (A1, A2) => R): IgniteBiClosure[A1, A2, R] =
        f match {
            case (p: ScalarClosure2Function[A1, A2, R]) => p.inner
            case _ => new ScalarClosure2[A1, A2, R](f)
        }

    /**
     * Implicit converter from Scala closure to `GridClosure2X`.
     *
     * @param f Scala closure to convert.
     */
    def toClosure2X[A1, A2, R](f: (A1, A2) => R): IgniteClosure2X[A1, A2, R] =
        f match {
            case (p: ScalarClosure2XFunction[A1, A2, R]) => p.inner
            case _ => new ScalarClosure2X[A1, A2, R](f)
        }

    /**
     * Implicit converter from `GridClosure2X` to Scala wrapping closure.
     *
     * @param f Grid closure to convert.
     */
    implicit def fromClosure2[A1, A2, R](f: IgniteBiClosure[A1, A2, R]): (A1, A2) => R =
        new ScalarClosure2Function[A1, A2, R](f)

    /**
     * Implicit converter from `GridClosure2X` to Scala wrapping closure.
     *
     * @param f Grid closure to convert.
     */
    implicit def fromClosure2X[A1, A2, R](f: IgniteClosure2X[A1, A2, R]): (A1, A2) => R =
        new ScalarClosure2XFunction[A1, A2, R](f)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param f Java-side closure to pimp.
      */
    implicit def closure2DotScala[A1, A2, R](f: IgniteBiClosure[A1, A2, R]) = new {
        def scala: (A1, A2) => R =
            fromClosure2(f)
    }

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param f Java-side closure to pimp.
      */
    implicit def closure2XDotScala[A1, A2, R](f: IgniteClosure2X[A1, A2, R]) = new {
        def scala: (A1, A2) => R =
            fromClosure2X(f)
    }

    /**
     * Implicit converter from Scala closure to `GridClosure3X`.
     *
     * @param f Scala closure to convert.
     */
    implicit def toClosure3[A1, A2, A3, R](f: (A1, A2, A3) => R): GridClosure3[A1, A2, A3, R] =
        f match {
            case (p: ScalarClosure3Function[A1, A2, A3, R]) => p.inner
            case _ => new ScalarClosure3[A1, A2, A3, R](f)
        }

    /**
     * Implicit converter from Scala closure to `GridClosure3X`.
     *
     * @param f Scala closure to convert.
     */
    def toClosure3X[A1, A2, A3, R](f: (A1, A2, A3) => R): GridClosure3X[A1, A2, A3, R] =
        f match {
            case (p: ScalarClosure3XFunction[A1, A2, A3, R]) => p.inner
            case _ => new ScalarClosure3X[A1, A2, A3, R](f)
        }

    /**
     * Implicit converter from `GridClosure3` to Scala wrapping closure.
     *
     * @param f Grid closure to convert.
     */
    implicit def fromClosure3[A1, A2, A3, R](f: GridClosure3[A1, A2, A3, R]): (A1, A2, A3) => R =
        new ScalarClosure3Function[A1, A2, A3, R](f)

    /**
     * Implicit converter from `GridClosure3X` to Scala wrapping closure.
     *
     * @param f Grid closure to convert.
     */
    implicit def fromClosure3X[A1, A2, A3, R](f: GridClosure3X[A1, A2, A3, R]): (A1, A2, A3) => R =
        new ScalarClosure3XFunction[A1, A2, A3, R](f)

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param f Java-side closure to pimp.
      */
    implicit def closure3DotScala[A1, A2, A3, R](f: GridClosure3[A1, A2, A3, R]) = new {
        def scala: (A1, A2, A3) => R =
            fromClosure3(f)
    }

    /**
      * Pimp for adding explicit conversion method `scala`.
      *
      * @param f Java-side closure to pimp.
      */
    implicit def closure3XDotScala[A1, A2, A3, R](f: GridClosure3X[A1, A2, A3, R]) = new {
        def scala: (A1, A2, A3) => R =
            fromClosure3X(f)
    }
}
