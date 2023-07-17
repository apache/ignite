package de.bwaldvogel.mongo.backend.ignite.util;


import ai.djl.Application;
import ai.djl.MalformedModelException;
import ai.djl.ModelException;
import ai.djl.inference.Predictor;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDArrays;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.NoBatchifyTranslator;
import ai.djl.translate.TranslateException;
import ai.djl.translate.TranslatorContext;
import ai.djl.util.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An example of inference using an universal sentence encoder model from TensorFlow Hub.
 *
 * <p>Refer to <a href="https://tfhub.dev/google/universal-sentence-encoder/4">Universal Sentence
 * Encoder</a> on TensorFlow Hub for more information.
 */
public final class UniversalSentenceEncoder {
    private static final Logger logger = LoggerFactory.getLogger(UniversalSentenceEncoder.class);

    private UniversalSentenceEncoder() {}

    public static void main(String[] args) throws IOException, ModelException, TranslateException {
        List<String> inputs = new ArrayList<>();
        inputs.add("The quick brown fox jumps over the lazy dog.");
        inputs.add("I am a sentence for which I would like to get its embedding");

        float[][] embeddings = UniversalSentenceEncoder.predict(inputs);
        for (int i = 0; i < inputs.size(); i++) {
            logger.info("Embedding for: " + inputs.get(i) + "\n" + Arrays.toString(embeddings[i]));
        }
    }

    public static float[][] predict(List<String> inputs)
            throws MalformedModelException, ModelNotFoundException, IOException,
                    TranslateException {
        String modelUrl =
                "https://storage.googleapis.com/tfhub-modules/google/universal-sentence-encoder/4.tar.gz";

        Criteria<String[], float[][]> criteria =
                Criteria.builder()
                        .optApplication(Application.NLP.TEXT_EMBEDDING)
                        .setTypes(String[].class, float[][].class)
                        .optModelUrls(modelUrl)
                        .optTranslator(new MyTranslator())
                        .optEngine("TensorFlow")
                        .optProgress(new ProgressBar())
                        .build();
        try (ZooModel<String[], float[][]> model = criteria.loadModel();
                Predictor<String[], float[][]> predictor = model.newPredictor()) {
            return predictor.predict(inputs.toArray(Utils.EMPTY_ARRAY));
        }
    }

    private static final class MyTranslator implements NoBatchifyTranslator<String[], float[][]> {

        MyTranslator() {}

        @Override
        public NDList processInput(TranslatorContext ctx, String[] inputs) {
            // manually stack for faster batch inference
            NDManager manager = ctx.getNDManager();
            NDList inputsList =
                    new NDList(
                            Arrays.stream(inputs)
                                    .map(manager::create)
                                    .collect(Collectors.toList()));
            return new NDList(NDArrays.stack(inputsList));
        }

        @Override
        public float[][] processOutput(TranslatorContext ctx, NDList list) {
            NDList result = new NDList();
            long numOutputs = list.singletonOrThrow().getShape().get(0);
            for (int i = 0; i < numOutputs; i++) {
                result.add(list.singletonOrThrow().get(i));
            }
            return result.stream().map(NDArray::toFloatArray).toArray(float[][]::new);
        }
    }
}
