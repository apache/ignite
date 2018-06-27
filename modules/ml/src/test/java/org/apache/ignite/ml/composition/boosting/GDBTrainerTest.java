package org.apache.ignite.ml.composition.boosting;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.Model;
import org.junit.Test;

public class GDBTrainerTest {
    @Test
    public void testFitRegression() {
        int size = 100;
        double[] xs = new double[size];
        double[] ys = new double[size];
        double from = -5.0;
        double to = 5.0;
        double step = Math.abs(from - to) / size;

        Map<Integer, double[]> learningSample = new HashMap<>();
        for (int i = 0; i < size; i++) {
            xs[i] = from + step * i;
            ys[i] = Math.pow(xs[i], 2);
            learningSample.put(i, new double[] {xs[i], ys[i]});
        }

        int maxTrees = 2000;
        System.out.print("[");
        for (int i = 0; i < maxTrees + 1; i += 10) {
            System.out.print(String.format("%.2f", (double)i));
            if (i != maxTrees)
                System.out.print(",");
        }
        System.out.println("]");
        System.out.print("[");
        for (int i = 0; i < maxTrees + 1; i += 10) {
            GDBTrainer trainer = new GDBTrainer(new MSELossFunction(), step, i);
            Model<double[], Double> model = trainer.fit(
                learningSample, 1,
                (k, v) -> new double[] {v[0]},
                (k, v) -> v[1]
            );

            double mse = 0.0;
            for (int j = 0; j < size; j++) {
                double x = xs[j];
                double y = ys[j];
                double p = model.apply(new double[] {x});
                mse += Math.pow(y - p, 2);
            }
            mse /= size;
            System.out.print(String.format("%.2f", mse));
            if (i != maxTrees)
                System.out.print(",");
        }
        System.out.println("]");
    }

    private double sigma(double x) {
        return 1.0 / (1.0 + Math.exp(-x));
    }

    @Test
    public void testFitClassifier() {
        int sampleSize = 100;
        double[] xs = new double[sampleSize];
        double[] ys = new double[sampleSize];

        for (int i = 0; i < sampleSize; i++) {
            xs[i] = i;
            ys[i] = ((int)(xs[i] / 10.0) % 2) == 0 ? 0.0 : 1.0;
        }

        Map<Integer, double[]> learningSample = new HashMap<>();
        for (int i = 0; i < sampleSize; i++)
            learningSample.put(i, new double[] { xs[i], ys[i] });

        int maxTrees = 2000;
        System.out.print("[");
        for (int i = 0; i < maxTrees + 1; i += 10) {
            System.out.print(String.format("%.2f", (double)i));
            if (i != maxTrees)
                System.out.print(",");
        }
        System.out.println("]");
        System.out.print("[");
        for (int i = 0; i < maxTrees + 1; i += 10) {
            GDBTrainer trainer = new GDBTrainer(new LogLossLossFunction(), step, i);
            Model<double[], Double> model = trainer.fit(
                learningSample, 1,
                (k, v) -> new double[] {v[0]},
                (k, v) -> v[1]
            );

            double errorRate = 0.0;
            for (int j = 0; j < sampleSize; j++) {
                double x = xs[j] + 0.5;
                double y = ys[j];
                Double modelAnswer = model.apply(new double[] {x});
                double p = sigma(modelAnswer) < 0.5 ? 0. : 1.;
                errorRate += ((y != p) ? 1.0 : 0.0);
            }
            errorRate /= sampleSize;
            System.out.print(String.format("%.2f", errorRate));
            if (i != maxTrees)
                System.out.print(",");
        }
        System.out.println("]");
    }

    private Double answer(Double x) {
        return Math.sin(x) > 0. ? 1.0 : 0.0;
    }

    @Test
    public void testFit2() {
        int sampleSize = 1000;
        GDBTrainer trainer = new GDBTrainer(new LogLossLossFunction(), step, 100);

        Map<Integer, double[]> data = new HashMap<>();

        for (int i = 0; i < sampleSize; i++) {
            double x = ((double)i) / 10.0;
            data.put(i, new double[] {x, answer(x)});
        }

        Model<double[], Double> model = trainer.fit(data,
            1,
            (k, v) -> Arrays.copyOf(v, v.length - 1),
            (k, v) -> v[v.length - 1]
        );

        double[] xs = new double[] {0., 0.25 * Math.PI, 0.5 * Math.PI, 0.75 * Math.PI, Math.PI, 1.25 * Math.PI, 1.5 * Math.PI};
        for (Double x : xs) {
            double y = answer(x);
            double pred = model.apply(new double[] {x});
            System.out.println(String.format("x = %f, y = %f, prediction = %f", x, y, pred));
        }
    }
}
