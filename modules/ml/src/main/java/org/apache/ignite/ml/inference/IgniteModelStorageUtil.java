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

package org.apache.ignite.ml.inference;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.Ignite;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.inference.builder.AsyncModelBuilder;
import org.apache.ignite.ml.inference.builder.SingleModelBuilder;
import org.apache.ignite.ml.inference.builder.SyncModelBuilder;
import org.apache.ignite.ml.inference.parser.IgniteModelParser;
import org.apache.ignite.ml.inference.reader.ModelStorageModelReader;
import org.apache.ignite.ml.inference.storage.descriptor.ModelDescriptorStorage;
import org.apache.ignite.ml.inference.storage.descriptor.ModelDescriptorStorageFactory;
import org.apache.ignite.ml.inference.storage.model.ModelStorage;
import org.apache.ignite.ml.inference.storage.model.ModelStorageFactory;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.util.Utils;
import org.jetbrains.annotations.NotNull;

/**
 * Utils class that helps to operate with model storage and Ignite models.
 */
public class IgniteModelStorageUtil {
    /** Folder to be used to store Ignite models. */
    private static final String IGNITE_MDL_FOLDER = "/ignite_models";

    /**
     * Saved specified model with specified name.
     *
     * @param ignite Ignite instance.
     * @param mdl Model to be saved.
     * @param name Model name to be used.
     * @param <I> Type of input.
     * @param <O> Type of output.
     */
    public static <I extends Serializable, O extends Serializable> void saveModel(Ignite ignite,
        IgniteModel<I, O> mdl, String name) {
        IgniteModel<byte[], byte[]> mdlWrapper = wrapIgniteModel(mdl);
        byte[] serializedMdl = Utils.serialize(mdlWrapper);
        UUID mdlId = UUID.randomUUID();

        saveModelDescriptor(ignite, name, mdlId);

        try {
            saveModelEntity(ignite, serializedMdl, mdlId);
        }
        catch (Exception e) {
            // Here we need to do a rollback and remove descriptor from correspondent storage.
            removeModelEntity(ignite, mdlId);
            throw e;
        }
    }

    /**
     * Removes model with specified name.
     *
     * @param ignite Ignite instance.
     * @param name Mode name to be removed.
     */
    public static void removeModel(Ignite ignite, String name) {
        ModelDescriptor desc = getModelDescriptor(ignite, name);
        if (desc == null)
            return;

        UUID mdlId = UUID.fromString(desc.getName());
        removeModel(ignite, IGNITE_MDL_FOLDER + "/" + mdlId);
        removeModelDescriptor(ignite, name);
    }

    /**
     * Retrieves Ignite model by name using {@link SingleModelBuilder}.
     *
     * @param ignite Ignite instance.
     * @param name Model name.
     * @param <I> Type of input.
     * @param <O> Type of output.
     * @return Synchronous model built using {@link SingleModelBuilder}.
     */
    public static <I extends Serializable, O extends Serializable> Model<I, O> getModel(Ignite ignite, String name) {
        return getSyncModel(ignite, name, new SingleModelBuilder());
    }

    /**
     * Retrieves Ignite model by name using synchronous model builder.
     *
     * @param ignite Ignite instance.
     * @param name Model name.
     * @param mdlBldr Synchronous model builder.
     * @param <I> Type of input.
     * @param <O> Type of output.
     * @return Synchronous model built using specified model builder.
     */
    public static <I extends Serializable, O extends Serializable> Model<I, O> getSyncModel(Ignite ignite, String name,
        SyncModelBuilder mdlBldr) {
        ModelDescriptor desc = Objects.requireNonNull(getModelDescriptor(ignite, name),
            "Model not found [name=" + name + "]");

        Model<byte[], byte[]> infMdl = mdlBldr.build(desc.getReader(), desc.getParser());

        return unwrapIgniteSyncModel(infMdl);
    }

    /**
     * Retrieves Ignite model by name using asynchronous model builder.
     *
     * @param ignite Ignite instance.
     * @param name Model name.
     * @param mdlBldr Asynchronous model builder.
     * @return Asynchronous model built using specified model builder.
     */
    public static Model<Vector, Future<Double>> getAsyncModel(Ignite ignite, String name, AsyncModelBuilder mdlBldr) {
        ModelDescriptor desc = Objects.requireNonNull(getModelDescriptor(ignite, name),
            "Model not found [name=" + name + "]");

        Model<byte[], Future<byte[]>> infMdl = mdlBldr.build(desc.getReader(), desc.getParser());

        return unwrapIgniteAsyncModel(infMdl);
    }

    /**
     * Saves specified serialized model into storage as a file.
     *
     * @param ignite Ignite instance.
     * @param serializedMdl Serialized model represented as a byte array.
     * @param mdlId Model identifier.
     */
    private static void saveModelEntity(Ignite ignite, byte[] serializedMdl, UUID mdlId) {
        ModelStorage storage = new ModelStorageFactory().getModelStorage(ignite);
        storage.mkdirs(IGNITE_MDL_FOLDER);
        storage.putFile(IGNITE_MDL_FOLDER + "/" + mdlId, serializedMdl, true);
    }

    /**
     * Removes model with specified identifier from model storage.
     *
     * @param ignite Ignite instance.
     * @param mdlId Model identifier.
     */
    private static void removeModelEntity(Ignite ignite, UUID mdlId) {
        ModelStorage storage = new ModelStorageFactory().getModelStorage(ignite);
        storage.remove(IGNITE_MDL_FOLDER + "/" + mdlId);
    }

    /**
     * Saves model descriptor into descriptor storage if a model with given name is not saved yet, otherwise throws
     * exception. To save model with the same name remove old model first.
     *
     * @param ignite Ignite instance.
     * @param name Model name.
     * @param mdlId Model identifier used to find model in model storage (only with {@link ModelStorageModelReader}).
     * @throws IllegalArgumentException If model with given name was already saved.
     */
    private static void saveModelDescriptor(Ignite ignite, String name, UUID mdlId) {
        ModelDescriptorStorage descStorage = new ModelDescriptorStorageFactory().getModelDescriptorStorage(ignite);

        boolean saved = descStorage.putIfAbsent(name, new ModelDescriptor(
            mdlId.toString(),
            null,
            new ModelSignature(null, null, null),
            new ModelStorageModelReader(IGNITE_MDL_FOLDER + "/" + mdlId),
            new IgniteModelParser<>()
        ));

        if (!saved)
            throw new IllegalArgumentException("Model descriptor with given name already exists [name=" + name + "]");
    }

    /**
     * Removes model descriptor from descriptor storage.
     *
     * @param ignite Ignite instance.
     * @param name Model name.
     */
    private static void removeModelDescriptor(Ignite ignite, String name) {
        ModelDescriptorStorage descStorage = new ModelDescriptorStorageFactory().getModelDescriptorStorage(ignite);

        descStorage.remove(name);
    }

    /**
     * Retirieves model descriptor.
     *
     * @param ignite Ignite instance.
     * @param name Model name.
     * @return Model descriptor.
     */
    private static ModelDescriptor getModelDescriptor(Ignite ignite, String name) {
        ModelDescriptorStorage descStorage = new ModelDescriptorStorageFactory().getModelDescriptorStorage(ignite);

        return descStorage.get(name);
    }

    /**
     * Wraps Ignite model so that model accepts and returns serialized objects (byte arrays).
     *
     * @param mdl Ignite model.
     * @return Ignite model that accepts and returns serialized objects (byte arrays).
     */
    private static <I extends Serializable, O extends Serializable> IgniteModel<byte[], byte[]> wrapIgniteModel(
        IgniteModel<I, O> mdl) {
        return input -> {
            I deserializedInput = Utils.deserialize(input);
            O output = mdl.predict(deserializedInput);

            return Utils.serialize(output);
        };
    }

    /**
     * Unwraps Ignite model so that model accepts and returns deserialized objects ({@link Vector} and {@link Double}).
     *
     * @param mdl Ignite model.
     * @param <I> Type of input.
     * @param <O> Type of output.
     * @return Ignite model that accepts and returns deserialized objects ({@link Vector} and {@link Double}).
     */
    private static <I extends Serializable, O extends Serializable> Model<I, O> unwrapIgniteSyncModel(
        Model<byte[], byte[]> mdl) {
        return new Model<I, O>() {
            /** {@inheritDoc} */
            @Override public O predict(I input) {
                byte[] serializedInput = Utils.serialize(input);
                byte[] serializedOutput = mdl.predict(serializedInput);

                return (O)Utils.deserialize(serializedOutput);
            }

            /** {@inheritDoc} */
            @Override public void close() {
                mdl.close();
            }
        };
    }

    /**
     * Unwraps Ignite model so that model accepts and returns deserialized objects ({@link Vector} and {@link Double}).
     *
     * @param mdl Ignite model.
     * @return Ignite model that accepts and returns deserialized objects ({@link Vector} and {@link Double}).
     */
    private static Model<Vector, Future<Double>> unwrapIgniteAsyncModel(Model<byte[], Future<byte[]>> mdl) {
        return new Model<Vector, Future<Double>>() {
            /** {@inheritDoc} */
            @Override public Future<Double> predict(Vector input) {
                byte[] serializedInput = Utils.serialize(input);
                Future<byte[]> serializedOutput = mdl.predict(serializedInput);

                return new FutureDeserializationWrapper<>(serializedOutput);
            }

            /** {@inheritDoc} */
            @Override public void close() {
                mdl.close();
            }
        };
    }

    /**
     * Future deserialization wrapper that accepts future that returns serialized object and turns it into future that
     * returns deserialized object.
     *
     * @param <T> Type of return value.
     */
    private static class FutureDeserializationWrapper<T> implements Future<T> {
        /** Delegate. */
        private final Future<byte[]> delegate;

        /**
         * Constructs a new instance of future deserialization wrapper.
         *
         * @param delegate Delegate.
         */
        public FutureDeserializationWrapper(Future<byte[]> delegate) {
            this.delegate = delegate;
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
            return (T)Utils.deserialize(delegate.get());
        }

        /** {@inheritDoc} */
        @Override public T get(long timeout, @NotNull TimeUnit unit) throws InterruptedException, ExecutionException,
            TimeoutException {
            return (T)Utils.deserialize(delegate.get(timeout, unit));
        }
    }
}
