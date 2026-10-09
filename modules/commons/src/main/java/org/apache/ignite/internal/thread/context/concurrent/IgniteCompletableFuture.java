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

package org.apache.ignite.internal.thread.context.concurrent;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextSnapshot;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.thread.context.function.OperationContextAwareBiConsumer;
import org.apache.ignite.internal.thread.context.function.OperationContextAwareBiFunction;
import org.apache.ignite.internal.thread.context.function.OperationContextAwareConsumer;
import org.apache.ignite.internal.thread.context.function.OperationContextAwareFunction;
import org.apache.ignite.internal.thread.context.function.OperationContextAwareRunnable;
import org.apache.ignite.internal.thread.context.function.OperationContextAwareSupplier;
import org.apache.ignite.internal.thread.context.function.OperationContextAwareWrapper;
import org.jetbrains.annotations.NotNull;

/** */
public class IgniteCompletableFuture<T> implements Future<T>, CompletionStage<T> {
    /** */
    private final CompletableFuture<T> delegate;

    /** */
    public IgniteCompletableFuture() {
        delegate = new CompletableFuture<>();
    }

    /** */
    private IgniteCompletableFuture(CompletableFuture<T> delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public <U> IgniteCompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
        return wrap(delegate.thenApply(OperationContextAwareFunction.wrap(fn)));
    }

    /** {@inheritDoc} */
    @Override public <U> IgniteCompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
        return wrap(delegate.thenApplyAsync(OperationContextAwareFunction.wrap(fn)));
    }

    /** {@inheritDoc} */
    @Override public <U> IgniteCompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
        return wrap(delegate.thenApplyAsync(OperationContextAwareFunction.wrap(fn), executor));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<Void> thenAccept(Consumer<? super T> action) {
        return wrap(delegate.thenAccept(OperationContextAwareConsumer.wrap(action)));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action) {
        return wrap(delegate.thenAcceptAsync(OperationContextAwareConsumer.wrap(action)));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
        return wrap(delegate.thenAcceptAsync(OperationContextAwareConsumer.wrap(action), executor));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<Void> thenRun(Runnable action) {
        return wrap(delegate.thenRun(OperationContextAwareRunnable.wrap(action)));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<Void> thenRunAsync(Runnable action) {
        return wrap(delegate.thenRunAsync(OperationContextAwareRunnable.wrap(action)));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
        return wrap(delegate.thenRunAsync(OperationContextAwareRunnable.wrap(action), executor));
    }

    /** {@inheritDoc} */
    @Override public <U, V> IgniteCompletableFuture<V> thenCombine(
        CompletionStage<? extends U> other,
        BiFunction<? super T, ? super U, ? extends V> fn
    ) {
        return wrap(delegate.thenCombine(unwrap(other), OperationContextAwareBiFunction.wrap(fn)));
    }

    /** {@inheritDoc} */
    @Override public <U, V> IgniteCompletableFuture<V> thenCombineAsync(
        CompletionStage<? extends U> other,
        BiFunction<? super T, ? super U, ? extends V> fn
    ) {
        return wrap(delegate.thenCombineAsync(unwrap(other), OperationContextAwareBiFunction.wrap(fn)));
    }

    /** {@inheritDoc} */
    @Override public <U, V> IgniteCompletableFuture<V> thenCombineAsync(
        CompletionStage<? extends U> other,
        BiFunction<? super T, ? super U, ? extends V> fn,
        Executor executor
    ) {
        return wrap(delegate.thenCombineAsync(unwrap(other), OperationContextAwareBiFunction.wrap(fn), executor));
    }

    /** {@inheritDoc} */
    @Override public <U> IgniteCompletableFuture<Void> thenAcceptBoth(
        CompletionStage<? extends U> other,
        BiConsumer<? super T, ? super U> action
    ) {
        return wrap(delegate.thenAcceptBoth(unwrap(other), OperationContextAwareBiConsumer.wrap(action)));
    }

    /** {@inheritDoc} */
    @Override public <U> IgniteCompletableFuture<Void> thenAcceptBothAsync(
        CompletionStage<? extends U> other,
        BiConsumer<? super T, ? super U> action
    ) {
        return wrap(delegate.thenAcceptBoth(unwrap(other), OperationContextAwareBiConsumer.wrap(action)));
    }

    /** {@inheritDoc} */
    @Override public <U> IgniteCompletableFuture<Void> thenAcceptBothAsync(
        CompletionStage<? extends U> other,
        BiConsumer<? super T, ? super U> action,
        Executor executor
    ) {
        return wrap(delegate.thenAcceptBothAsync(unwrap(other), OperationContextAwareBiConsumer.wrap(action), executor));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return wrap(delegate.runAfterBoth(unwrap(other), OperationContextAwareRunnable.wrap(action)));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return wrap(delegate.runAfterBothAsync(unwrap(other), OperationContextAwareRunnable.wrap(action)));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return wrap(delegate.runAfterBothAsync(unwrap(other), OperationContextAwareRunnable.wrap(action), executor));
    }

    /** {@inheritDoc} */
    @Override public <U> IgniteCompletableFuture<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return wrap(delegate.applyToEither(unwrap(other), OperationContextAwareFunction.wrap(fn)));
    }

    /** {@inheritDoc} */
    @Override public <U> IgniteCompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return wrap(delegate.applyToEitherAsync(unwrap(other), OperationContextAwareFunction.wrap(fn)));
    }

    /** {@inheritDoc} */
    @Override public <U> IgniteCompletableFuture<U> applyToEitherAsync(
        CompletionStage<? extends T> other,
        Function<? super T, U> fn,
        Executor executor
    ) {
        return wrap(delegate.applyToEitherAsync(unwrap(other), OperationContextAwareFunction.wrap(fn), executor));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return wrap(delegate.acceptEither(unwrap(other), OperationContextAwareConsumer.wrap(action)));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return wrap(delegate.acceptEitherAsync(unwrap(other), OperationContextAwareConsumer.wrap(action)));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<Void> acceptEitherAsync(
        CompletionStage<? extends T> other,
        Consumer<? super T> action,
        Executor executor
    ) {
        return wrap(delegate.acceptEitherAsync(unwrap(other), OperationContextAwareConsumer.wrap(action), executor));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return wrap(delegate.runAfterEither(unwrap(other), OperationContextAwareRunnable.wrap(action)));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return wrap(delegate.runAfterEitherAsync(unwrap(other), OperationContextAwareRunnable.wrap(action)));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return wrap(delegate.runAfterEitherAsync(unwrap(other), OperationContextAwareRunnable.wrap(action), executor));
    }

    /** {@inheritDoc} */
    @Override public <U> IgniteCompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return wrap(delegate.thenCompose(OperationContextAwareCompletionStageFactory.wrap(fn)));
    }

    /** {@inheritDoc} */
    @Override public <U> IgniteCompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
        return wrap(delegate.thenComposeAsync(OperationContextAwareCompletionStageFactory.wrap(fn)));
    }

    /** {@inheritDoc} */
    @Override public <U> IgniteCompletableFuture<U> thenComposeAsync(
        Function<? super T, ? extends CompletionStage<U>> fn,
        Executor executor
    ) {
        return wrap(delegate.thenComposeAsync(OperationContextAwareCompletionStageFactory.wrap(fn), executor));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return wrap(delegate.whenComplete(OperationContextAwareBiConsumer.wrap(action)));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
        return wrap(delegate.whenCompleteAsync(OperationContextAwareBiConsumer.wrap(action)));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor) {
        return wrap(delegate.whenCompleteAsync(OperationContextAwareBiConsumer.wrap(action), executor));
    }

    /** {@inheritDoc} */
    @Override public <U> IgniteCompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
        return wrap(delegate.handle(OperationContextAwareBiFunction.wrap(fn)));
    }

    /** {@inheritDoc} */
    @Override public <U> IgniteCompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
        return wrap(delegate.handleAsync(OperationContextAwareBiFunction.wrap(fn)));
    }

    /** {@inheritDoc} */
    @Override public <U> IgniteCompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
        return wrap(delegate.handleAsync(OperationContextAwareBiFunction.wrap(fn), executor));
    }

    /** {@inheritDoc} */
    @Override public IgniteCompletableFuture<T> exceptionally(Function<Throwable, ? extends T> fn) {
        return wrap(delegate.exceptionally(OperationContextAwareFunction.wrap(fn)));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<T> toCompletableFuture() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean cancel(boolean mayInterruptIfRunning) {
        return delegate.cancel(mayInterruptIfRunning);
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return delegate.isCancelled();
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        return delegate.isDone();
    }

    /** {@inheritDoc} */
    @Override public T get() throws InterruptedException, ExecutionException {
        return delegate.get();
    }

    /** {@inheritDoc} */
    @Override public T get(long timeout, @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.get(timeout, unit);
    }

    /** */
    public T join() {
        return delegate.join();
    }

    /** */
    public T getNow(T valueIfAbsent) {
        return delegate.getNow(valueIfAbsent);
    }

    /** */
    public boolean complete(T value) {
        return delegate.complete(value);
    }

    /** */
    public boolean completeExceptionally(Throwable ex) {
        return delegate.completeExceptionally(ex);
    }

    /** */
    public boolean isCompletedExceptionally() {
        return delegate.isCompletedExceptionally();
    }

    /** */
    public static IgniteCompletableFuture<Void> allOf(IgniteCompletableFuture<?>... cfs) {
        return wrap(CompletableFuture.allOf(Arrays.stream(cfs).map(f -> f.delegate).toArray(CompletableFuture[]::new)));
    }

    /** */
    public static IgniteCompletableFuture<Object> anyOf(IgniteCompletableFuture<?>... cfs) {
        return wrap(CompletableFuture.anyOf(Arrays.stream(cfs).map(f -> f.delegate).toArray(CompletableFuture[]::new)));
    }

    /** */
    public static <U> IgniteCompletableFuture<U> supplyAsync(Supplier<U> supplier) {
        return wrap(CompletableFuture.supplyAsync(OperationContextAwareSupplier.wrap(supplier)));
    }

    /** */
    public static <U> IgniteCompletableFuture<U> supplyAsync(Supplier<U> supplier, Executor executor) {
        return wrap(CompletableFuture.supplyAsync(OperationContextAwareSupplier.wrap(supplier), executor));
    }

    /** */
    public static IgniteCompletableFuture<Void> runAsync(Runnable runnable) {
        return wrap(CompletableFuture.runAsync(OperationContextAwareRunnable.wrap(runnable)));
    }

    /** */
    public static IgniteCompletableFuture<Void> runAsync(Runnable runnable, Executor executor) {
        return wrap(CompletableFuture.runAsync(OperationContextAwareRunnable.wrap(runnable), executor));
    }

    /** */
    public static <U> IgniteCompletableFuture<U> completedFuture(U value) {
        return wrap(CompletableFuture.completedFuture(value));
    }

    /** */
    private static <T> IgniteCompletableFuture<T> wrap(CompletableFuture<T> delegate) {
        return new IgniteCompletableFuture<>(delegate);
    }

    /** */
    private static <T> CompletionStage<T> unwrap(CompletionStage<T> completionStage) {
        return completionStage instanceof IgniteCompletableFuture
            ? ((IgniteCompletableFuture<T>)completionStage).delegate
            : completionStage;
    }

    /** */
    private static class OperationContextAwareCompletionStageFactory<T, U>
        extends OperationContextAwareWrapper<Function<? super T, ? extends CompletionStage<U>>>
        implements Function<T, CompletionStage<U>> {
        /** */
        public OperationContextAwareCompletionStageFactory(
            Function<? super T, ? extends CompletionStage<U>> delegate,
            OperationContextSnapshot snapshot
        ) {
            super(delegate, snapshot);
        }

        /** {@inheritDoc} */
        @Override public CompletionStage<U> apply(T t) {
            try (Scope ignored = OperationContext.restoreSnapshot(snapshot)) {
                return unwrap(delegate.apply(t));
            }
        }

        /** */
        public static <T, R> Function<? super T, ? extends CompletionStage<R>> wrap(
            Function<? super T, ? extends CompletionStage<R>> delegate
        ) {
            if (delegate == null || delegate instanceof OperationContextAwareWrapper)
                return delegate;

            return new OperationContextAwareCompletionStageFactory<>(delegate, OperationContext.createSnapshot());
        }
    }
}

