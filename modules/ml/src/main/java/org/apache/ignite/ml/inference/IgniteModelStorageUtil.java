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

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
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
     * @param mdl Model to be saved.
     * @param name Model name to be used.
     */
    public static void saveModel(IgniteModel<Vector, Double> mdl, String name) {
        IgniteModel<byte[], byte[]> mdlWrapper = wrapIgniteModel(mdl);
        byte[] serializedMdl = Utils.serialize(mdlWrapper);
        UUID mdlId = UUID.randomUUID();

        saveModelStorage(serializedMdl, mdlId);
        saveModelDescriptorStorage(name, mdlId);
    }

    /**
     * Retrieves Ignite model by name using {@link SingleModelBuilder}.
     *
     * @param name Model name.
     * @return Synchronous model built using {@link SingleModelBuilder}.
     */
    public static Model<Vector, Double> getModel(String name) {
        return getSyncModel(name, new SingleModelBuilder());
    }

    /**
     * Retrieves Ignite model by name using synchronous model builder.
     *
     * @param name Model name.
     * @param mdlBldr Synchronous model builder.
     * @return Synchronous model built using specified model builder.
     */
    public static Model<Vector, Double> getSyncModel(String name, SyncModelBuilder mdlBldr) {
        ModelDescriptor desc = Objects.requireNonNull(getModelDescriptor(name), "Model not found [name=" + name + "]");

        Model<byte[], byte[]> infMdl = mdlBldr.build(desc.getReader(), desc.getParser());

        return unwrapIgniteSyncModel(infMdl);
    }

    /**
     * Retrieves Ignite model by name using asynchronous model builder.
     *
     * @param name Model name.
     * @param mdlBldr Asynchronous model builder.
     * @return Asynchronous model built using specified model builder.
     */
    public static Model<Vector, Future<Double>> getAsyncModel(String name, AsyncModelBuilder mdlBldr) {
        ModelDescriptor desc = Objects.requireNonNull(getModelDescriptor(name), "Model not found [name=" + name + "]");

        Model<byte[], Future<byte[]>> infMdl = mdlBldr.build(desc.getReader(), desc.getParser());

        return unwrapIgniteAsyncModel(infMdl);
    }

    /**
     * Saves specified serialized model into storage as a file.
     *
     * @param serializedMdl Serialized model represented as a byte array.
     * @param mdlId Model identifier.
     */
    private static void saveModelStorage(byte[] serializedMdl, UUID mdlId) {
        Ignite ignite = Ignition.ignite();

        ModelStorage storage = new ModelStorageFactory().getModelStorage(ignite);
        storage.mkdirs(IGNITE_MDL_FOLDER);
        storage.putFile(IGNITE_MDL_FOLDER + "/" + mdlId, serializedMdl);
    }

    /**
     * Saves model descriptor into descriptor storage.
     *
     * @param name Model name.
     * @param mdlId Model identifier used to find model in model storage (only with {@link ModelStorageModelReader}).
     */
    private static void saveModelDescriptorStorage(String name, UUID mdlId) {
        Ignite ignite = Ignition.ignite();

        ModelDescriptorStorage descStorage = new ModelDescriptorStorageFactory().getModelDescriptorStorage(ignite);
        descStorage.put(name, new ModelDescriptor(
            name,
            null,
            new ModelSignature(null, null, null),
            new ModelStorageModelReader(IGNITE_MDL_FOLDER + "/" + mdlId),
            new IgniteModelParser<>()
        ));
    }

    /**
     * Retirieves model descriptor.
     *
     * @param name Model name.
     * @return Model descriptor.
     */
    private static ModelDescriptor getModelDescriptor(String name) {
        Ignite ignite = Ignition.ignite();

        ModelDescriptorStorage descStorage = new ModelDescriptorStorageFactory().getModelDescriptorStorage(ignite);

        return descStorage.get(name);
    }

    /**
     * Wraps Ignite model so that model accepts and returns serialized objects (byte arrays).
     *
     * @param mdl Ignite model.
     * @return Ignite model that accepts and returns serialized objects (byte arrays).
     */
    private static IgniteModel<byte[], byte[]> wrapIgniteModel(IgniteModel<Vector, Double> mdl) {
        return input -> {
            Vector deserializedInput = Utils.deserialize(input);
            Double output = mdl.predict(deserializedInput);

            return Utils.serialize(output);
        };
    }

    /**
     * Unwraps Ignite model so that model accepts and returns deserialized objects ({@link Vector} and {@link Double}).
     *
     * @param mdl Ignite model.
     * @return Ignite model that accepts and returns deserialized objects ({@link Vector} and {@link Double}).
     */
    private static Model<Vector, Double> unwrapIgniteSyncModel(Model<byte[], byte[]> mdl) {
        return new Model<Vector, Double>() {
            /** {@inheritDoc} */
            @Override public Double predict(Vector input) {
                byte[] serializedInput = Utils.serialize(input);
                byte[] serializedOutput = mdl.predict(serializedInput);

                return Utils.deserialize(serializedOutput);
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
