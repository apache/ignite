package org.apache.ignite.ml.selection.scoring.metric.classification;

import java.io.Serializable;
import org.apache.ignite.ml.selection.scoring.evaluator.aggregator.BinaryClassificationPointwiseMetricStatsAggregator;
import org.apache.ignite.ml.selection.scoring.metric.MetricName;

/**
 * F-measure metric class.
 */
public class FMeasure<L extends Serializable> extends BinaryClassificationMetric<L> {
    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = 9156779288165194348L;

    /**
     * Square of beta parameter for F-score.
     */
    private final double betaSquare;

    /**
     * Precision.
     */
    private final Precision<L> precision = new Precision<>();

    /**
     * Recall.
     */
    private final Recall<L> recall = new Recall<>();

    /**
     * Fscore.
     */
    private Double fscore = Double.NaN;

    /**
     * Creates an instance of FScore class.
     *
     * @param beta Beta (see https://en.wikipedia.org/wiki/F1_score ).
     */
    public FMeasure(double beta) {
        betaSquare = Math.pow(beta, 2);
    }

    /**
     * Creates an instance of FScore class.
     */
    public FMeasure() {
        betaSquare = 1;
    }

    /**
     * Creates an instance of FScore class.
     *
     * @param truthLabel Truth label.
     * @param falseLabel False label.
     * @param betaSquare Squared beta parameter.
     */
    public FMeasure(L truthLabel, L falseLabel, double betaSquare) {
        super(truthLabel, falseLabel);
        this.betaSquare = betaSquare;
    }

    /**
     * Creates an instance of FScore class.
     *
     * @param truthLabel Truth label.
     * @param falseLabel False label.
     */
    public FMeasure(L truthLabel, L falseLabel) {
        super(truthLabel, falseLabel);
        this.betaSquare = 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override public FMeasure<L> initBy(BinaryClassificationPointwiseMetricStatsAggregator<L> aggr) {
        precision.initBy(aggr);
        recall.initBy(aggr);

        double nom = (1 + betaSquare) * precision.value() * recall.value();
        double denom = (betaSquare * precision.value() + recall.value());
        fscore = nom / denom;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override public double value() {
        return fscore;
    }

    /**
     * {@inheritDoc}
     */
    @Override public MetricName name() {
        return MetricName.F_MEASURE;
    }
}