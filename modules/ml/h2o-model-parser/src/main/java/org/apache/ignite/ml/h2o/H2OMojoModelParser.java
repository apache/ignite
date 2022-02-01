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

package org.apache.ignite.ml.h2o;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import hex.genmodel.CategoricalEncoding;
import hex.genmodel.MojoModel;
import hex.genmodel.MojoReaderBackend;
import hex.genmodel.MojoReaderBackendFactory;
import hex.genmodel.easy.EasyPredictModelWrapper;
import org.apache.ignite.ml.inference.parser.ModelParser;
import org.apache.ignite.ml.math.primitives.vector.NamedVector;

/**
 * H2O MOJO model parser.
 */
public class H2OMojoModelParser implements ModelParser<NamedVector, Double, H2OMojoModel> {
    /** */
    private static final long serialVersionUID = -170352744966205716L;

    /** {@inheritDoc} */
    @Override public H2OMojoModel parse(byte[] mojoBytes) {
        try (InputStream mojoInputStream = new ByteArrayInputStream(mojoBytes)) {
            MojoReaderBackend readerBackend = MojoReaderBackendFactory.createReaderBackend(mojoInputStream,
                    MojoReaderBackendFactory.CachingStrategy.MEMORY);

            MojoModel mojoMdl = MojoModel.load(readerBackend);

            validateMojoModel(mojoMdl);
            // we expect categorical values to be already encoded
            EasyPredictModelWrapper.Config cfg = new EasyPredictModelWrapper.Config()
                    .setUseExternalEncoding(true)
                    .setConvertInvalidNumbersToNa(true)
                    .setConvertUnknownCategoricalLevelsToNa(true)
                    .setModel(mojoMdl);
            EasyPredictModelWrapper easyPredict = new EasyPredictModelWrapper(cfg);
            return new H2OMojoModel(easyPredict);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse MOJO", e);
        }
    }

    /**
     * Validates the mojo model.
     * @param mojoMdl Mojo model.
     */
    private void validateMojoModel(MojoModel mojoMdl) {
        switch (mojoMdl.getModelCategory()) {
            case Binomial:
            case Multinomial:
            case Ordinal:
            case Regression:
            case Clustering:
                break; // ok - supported
            default:
                throw new UnsupportedOperationException("Model Category " + mojoMdl.getModelCategory() + " is not supported yet.");
        }
        if (mojoMdl.getCategoricalEncoding() == CategoricalEncoding.OneHotExplicit)
            return;

        for (int i = 0; i < mojoMdl.nfeatures(); i++) {
            if (mojoMdl.getDomainValues(i) != null) {
                String colName = mojoMdl.getNames()[i];
                throw new UnsupportedOperationException("Unsupported MOJO model: only models using trained using " +
                        "OneHotExplicit categorical encoding and models without categorical features are currently supported. " +
                        "Column `" + colName + "` is categorical.");
            }
        }
    }
}
