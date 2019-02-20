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

package org.apache.ignite.ml.mleap;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import ml.combust.mleap.core.types.ScalarType;
import ml.combust.mleap.core.types.StructField;
import ml.combust.mleap.core.types.StructType;
import ml.combust.mleap.runtime.MleapContext;
import ml.combust.mleap.runtime.frame.Transformer;
import ml.combust.mleap.runtime.javadsl.BundleBuilder;
import ml.combust.mleap.runtime.javadsl.ContextBuilder;
import ml.combust.mleap.runtime.transformer.PipelineModel;
import org.apache.ignite.ml.inference.parser.ModelParser;
import org.apache.ignite.ml.math.primitives.vector.NamedVector;
import scala.collection.JavaConverters;

/**
 * MLeap model parser.
 */
public class MLeapModelParser implements ModelParser<NamedVector, Double, MLeapModel> {
    /** */
    private static final long serialVersionUID = -370352744966205715L;

    /** Temporary file prefix. */
    private static final String TMP_FILE_PREFIX = "mleap_model";

    /** Temporary file postfix. */
    private static final String TMP_FILE_POSTFIX = ".zip";

    /** {@inheritDoc} */
    @Override public MLeapModel parse(byte[] mdl) {
        MleapContext mleapCtx = new ContextBuilder().createMleapContext();
        BundleBuilder bundleBuilder = new BundleBuilder();

        File file = null;
        try {
            file = File.createTempFile(TMP_FILE_PREFIX, TMP_FILE_POSTFIX);
            try (FileOutputStream fos = new FileOutputStream(file)) {
                fos.write(mdl);
                fos.flush();
            }

            Transformer transformer = bundleBuilder.load(file, mleapCtx).root();
            PipelineModel pipelineMdl = (PipelineModel)transformer.model();

            List<String> inputSchema = checkAndGetInputSchema(pipelineMdl);
            String outputSchema = checkAndGetOutputSchema(pipelineMdl);

            return new MLeapModel(transformer, inputSchema, outputSchema);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        finally {
            if (file != null)
                file.delete();
        }
    }

    /**
     * Util method that checks that input schema contains only one double type.
     *
     * @param mdl Pipeline model.
     * @return Name of output field.
     */
    private String checkAndGetOutputSchema(PipelineModel mdl) {
        Transformer lastTransformer = mdl.transformers().last();
        StructType outputSchema = lastTransformer.outputSchema();

        List<StructField> output = new ArrayList<>(JavaConverters.seqAsJavaListConverter(outputSchema.fields()).asJava());

        if (output.size() != 1)
            throw new IllegalArgumentException("Parser supports only scalar outputs");

        return output.get(0).name();
    }

    /**
     * Util method that checks that output schema contains only double types and returns list of field names.
     *
     * @param mdl Pipeline model.
     * @return List of field names.
     */
    private List<String> checkAndGetInputSchema(PipelineModel mdl) {
        Transformer firstTransformer = mdl.transformers().head();
        StructType inputSchema = firstTransformer.inputSchema();

        List<StructField> input = new ArrayList<>(JavaConverters.seqAsJavaListConverter(inputSchema.fields()).asJava());

        List<String> schema = new ArrayList<>();

        for (StructField field : input) {
            String fieldName = field.name();

            schema.add(field.name());
            if (!ScalarType.Double().base().equals(field.dataType().base()))
                throw new IllegalArgumentException("Parser supports only double types [name=" +
                    fieldName + ",type=" + field.dataType() + "]");
        }

        return schema;
    }
}
