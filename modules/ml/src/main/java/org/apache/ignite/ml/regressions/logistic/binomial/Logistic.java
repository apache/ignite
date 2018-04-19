package org.apache.ignite.ml.regressions.logistic.binomial;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * Performs simple logistic regression.
 * User: tpeng
 * Date: 6/22/12
 * Time: 11:01 PM
 * 
 * @author tpeng
 * @author Matthieu Labas
 */
public class Logistic {

	/** the learning rate */
    private double rate;

	/** the weight to learn */
	private double[] weights;

	/** the number of iterations */
	private int ITERATIONS = 300;

	public Logistic(int n) {
		this.rate = 0.01;
		weights = new double[n];
	}

	private static double sigmoid(double z) {
		return 1.0 / (1.0 + Math.exp(-z));
	}

	public void train(List<Instance> instances) {
		for (int n=0; n<ITERATIONS; n++) {
			double lik = 0.0;
			for (int i=0; i<instances.size(); i++) {
				int[] x = instances.get(i).x;
				double predicted = classify(x);
				int label = instances.get(i).label;
				for (int j=0; j<weights.length; j++) {
					weights[j] = weights[j] + rate * (label - predicted) * x[j];
				}
				// not necessary for learning
				lik += label * Math.log(classify(x)) + (1-label) * Math.log(1- classify(x));
			}
			System.out.println("iteration: " + n + " " + Arrays.toString(weights) + " mle: " + lik);
		}
	}

	private double classify(int[] x) {
		double logit = .0;
		for (int i=0; i<weights.length;i++)  {
			logit += weights[i] * x[i];
		}
		return sigmoid(logit);
	}

	public static class Instance {
		public int label;
		public int[] x;

		public Instance(int label, int[] x) {
			this.label = label;
			this.x = x;
		}
	}



	public static void main(String... args) throws FileNotFoundException {
		List<Instance> instances = new ArrayList<>();
		instances.add(new Instance(1, new int[]{2,1,2,1,0,0}));
		instances.add(new Instance(1, new int[]{2,1,1,1,1,0}));
		instances.add(new Instance(1, new int[]{1,2,1,1,1,0}));
		instances.add(new Instance(0, new int[]{0,1,0,0,1,0}));
		instances.add(new Instance(0, new int[]{1,0,0,0,1,0}));
		instances.add(new Instance(0, new int[]{0,0,0,0,0,0}));

		Logistic logistic = new Logistic(5);
		logistic.train(instances);
		int[] x = {2, 1, 1, 2, 1};
		System.out.println("prob(1|x) = " + logistic.classify(x));

		int[] x2 = {0, 0, 1, 0, 0};
		System.out.println("prob(1|x2) = " + logistic.classify(x2));

	}

}