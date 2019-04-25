/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.inference.parser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.ml.inference.util.DirectorySerializer;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;

/**
 * Implementation of TensorFlow model parser that accepts serialized directory with "SavedModel" as an input. The
 * directory is assumed to be serialized by {@link DirectorySerializer}.
 *
 * @param <I> Type of model input.
 * @param <O> Type of model output.
 */
public class TensorFlowSavedModelModelParser<I, O> extends TensorFlowBaseModelParser<I, O> {
    /** */
    private static final long serialVersionUID = 5638083440240281879L;

    /** Prefix to be used to create temporary directory for TensorFlow model files. */
    private static final String TMP_DIR_PREFIX = "tensorflow_saved_model_";

    /** Model tags. */
    private final String[] tags;

    /**
     * Constructs a new instance of TensorFlow model parser.
     *
     * @param tags Model tags.
     */
    public TensorFlowSavedModelModelParser(String... tags) {
        this.tags = tags;
    }

    /** {@inheritDoc} */
    @Override public Session parseModel(byte[] mdl) {
        Path dir = null;
        try {
            dir = Files.createTempDirectory(TMP_DIR_PREFIX);
            DirectorySerializer.deserialize(dir.toAbsolutePath(), mdl);
            SavedModelBundle bundle = SavedModelBundle.load(dir.toString(), tags);
            return bundle.session();
        }
        catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (dir != null)
                DirectorySerializer.deleteDirectory(dir);
        }
    }
}
