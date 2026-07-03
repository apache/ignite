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

package org.apache.ignite.internal.client.thin;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.ignite.client.IgniteClientFuture;

/**
 * Ignite thin client future - a wrapper around {@link CompletableFuture}.
 * @param <T> Result type.
 */
public class IgniteClientFutureImpl<T> implements IgniteClientFuture<T> {
    /** Wrapped completable future. */
    private final CompletableFuture<T> fut;

    /** Cancel callback. */
    private final Function<Boolean, Boolean> onCancel;

    /**
     * Ctor.
     * @param fut Future to wrap.
     */
    public IgniteClientFutureImpl(
            CompletableFuture<T> fut,
            Function<Boolean, Boolean> onCancel) {
        assert fut != null;

        this.fut = fut;
        this.onCancel = onCancel;
    }

    /**
     * Ctor.
     * @param fut Future to wrap.
     */
    public IgniteClientFutureImpl(CompletionStage<T> fut) {
        assert fut != null;

        this.fut = fut.toCompletableFuture();
        onCancel = null;
    }

    /**
     * Gets a completed future with the given result.
     * @param res Result to wrap in a future.
     * @param <T> Result type.
     * @return Completed future.
     */
    public static <T> IgniteClientFutureImpl<T> completedFuture(T res) {
        CompletableFuture<T> fut = new CompletableFuture<>();
        fut.complete(res);
        return new IgniteClientFutureImpl<>(fut);
    }

    /** {@inheritDoc} */
    @Override public boolean isDone() {
        return fut.isDone();
    }

    /** {@inheritDoc} */
    @Override public T get() throws InterruptedException, ExecutionException {
        return fut.get();
    }

    /** {@inheritDoc} */
    @Override public T get(long l, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        return fut.get(l, timeUnit);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> thenApply(Function<? super T, ? extends U> function) {
        return fut.thenApply(function);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> function) {
        return fut.thenApplyAsync(function);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> function, Executor executor) {
        return fut.thenApplyAsync(function, executor);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> thenAccept(Consumer<? super T> consumer) {
        return fut.thenAccept(consumer);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> consumer) {
        return fut.thenAcceptAsync(consumer);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> thenAcceptAsync(Consumer<? super T> consumer, Executor executor) {
        return fut.thenAcceptAsync(consumer, executor);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> thenRun(Runnable runnable) {
        return fut.thenRun(runnable);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> thenRunAsync(Runnable runnable) {
        return fut.thenRunAsync(runnable);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> thenRunAsync(Runnable runnable, Executor executor) {
        return fut.thenRunAsync(runnable, executor);
    }

    /** {@inheritDoc} */
    @Override public <U, V> CompletableFuture<V> thenCombine(
        CompletionStage<? extends U> completionStage,
        BiFunction<? super T, ? super U, ? extends V> biFunction
    ) {
        return fut.thenCombine(completionStage, biFunction);
    }

    /** {@inheritDoc} */
    @Override public <U, V> CompletableFuture<V> thenCombineAsync(
        CompletionStage<? extends U> completionStage,
        BiFunction<? super T, ? super U, ? extends V> biFunction
    ) {
        return fut.thenCombineAsync(completionStage, biFunction);
    }

    /** {@inheritDoc} */
    @Override public <U, V> CompletableFuture<V> thenCombineAsync(
        CompletionStage<? extends U> completionStage,
        BiFunction<? super T, ? super U, ? extends V> biFunction,
        Executor executor
    ) {
        return fut.thenCombineAsync(completionStage, biFunction, executor);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<Void> thenAcceptBoth(
        CompletionStage<? extends U> completionStage,
        BiConsumer<? super T, ? super U> biConsumer
    ) {
        return fut.thenAcceptBoth(completionStage, biConsumer);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<Void> thenAcceptBothAsync(
        CompletionStage<? extends U> completionStage,
        BiConsumer<? super T, ? super U> biConsumer
    ) {
        return fut.thenAcceptBothAsync(completionStage, biConsumer);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<Void> thenAcceptBothAsync(
        CompletionStage<? extends U> completionStage,
        BiConsumer<? super T, ? super U> biConsumer,
        Executor executor
    ) {
        return fut.thenAcceptBothAsync(completionStage, biConsumer, executor);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> runAfterBoth(CompletionStage<?> completionStage, Runnable runnable) {
        return fut.runAfterBoth(completionStage, runnable);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> completionStage, Runnable runnable) {
        return fut.runAfterBothAsync(completionStage, runnable);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> completionStage, Runnable runnable, Executor executor) {
        return fut.runAfterBothAsync(completionStage, runnable, executor);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends T> completionStage, Function<? super T, U> function) {
        return fut.applyToEither(completionStage, function);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> applyToEitherAsync(
        CompletionStage<? extends T> completionStage,
        Function<? super T, U> function
    ) {
        return fut.applyToEitherAsync(completionStage, function);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> applyToEitherAsync(
        CompletionStage<? extends T> completionStage,
        Function<? super T, U> function,
        Executor executor
    ) {
        return fut.applyToEitherAsync(completionStage, function, executor);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> acceptEither(CompletionStage<? extends T> completionStage, Consumer<? super T> consumer) {
        return fut.acceptEither(completionStage, consumer);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> completionStage, Consumer<? super T> consumer) {
        return fut.acceptEitherAsync(completionStage, consumer);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> acceptEitherAsync(
        CompletionStage<? extends T> completionStage,
        Consumer<? super T> consumer,
        Executor executor
    ) {
        return fut.acceptEitherAsync(completionStage, consumer, executor);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> runAfterEither(CompletionStage<?> completionStage, Runnable runnable) {
        return fut.runAfterEither(completionStage, runnable);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> completionStage, Runnable runnable) {
        return fut.runAfterEitherAsync(completionStage, runnable);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> completionStage, Runnable runnable, Executor executor) {
        return fut.runAfterEitherAsync(completionStage, runnable, executor);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> function) {
        return fut.thenCompose(function);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> function) {
        return fut.thenComposeAsync(function);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> thenComposeAsync(
        Function<? super T, ? extends CompletionStage<U>> function,
        Executor executor
    ) {
        return fut.thenComposeAsync(function, executor);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> biConsumer) {
        return fut.whenComplete(biConsumer);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> biConsumer) {
        return fut.whenCompleteAsync(biConsumer);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> biConsumer, Executor executor) {
        return fut.whenCompleteAsync(biConsumer, executor);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> biFunction) {
        return fut.handle(biFunction);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> biFunction) {
        return fut.handleAsync(biFunction);
    }

    /** {@inheritDoc} */
    @Override public <U> CompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> biFunction, Executor executor) {
        return fut.handleAsync(biFunction, executor);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<T> toCompletableFuture() {
        return fut;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<T> exceptionally(Function<Throwable, ? extends T> function) {
        return fut.exceptionally(function);
    }

    /** {@inheritDoc} */
    @Override public boolean cancel(boolean mayInterruptIfRunning) {
        if (onCancel != null)
            return onCancel.apply(mayInterruptIfRunning);

        return fut.cancel(mayInterruptIfRunning);
    }

    /** {@inheritDoc} */
    @Override public boolean isCancelled() {
        return fut.isCancelled();
    }
}
