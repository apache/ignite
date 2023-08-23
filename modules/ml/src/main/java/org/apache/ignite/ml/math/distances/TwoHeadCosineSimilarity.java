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
package org.apache.ignite.ml.math.distances;


import org.apache.ignite.ml.math.exceptions.math.CardinalityException;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.util.MatrixUtil;


/**
 * Calculates the {@code A * B } (DotProduct similarity) distance between two points.
 */
public class TwoHeadCosineSimilarity implements DistanceMeasure {
    /** {@inheritDoc} */
    @Override public double compute(Vector a, Vector b) throws CardinalityException {
    	Vector aLeft = a.copyOfRange(0, a.size()/2);
    	Vector aRight = a.copyOfRange(a.size()/2,a.size());
    	
    	Vector bLeft = b.copyOfRange(0, b.size()/2);
    	Vector bRight = b.copyOfRange(b.size()/2,b.size());
    	
        double left = aLeft.dot(bLeft) / (aLeft.kNorm(2d) * bLeft.kNorm(2d));
        double right = aRight.dot(bRight) / (aRight.kNorm(2d) * bRight.kNorm(2d));
        
        if(left<0 && right<0) {
        	double d = 1.0 + left * right;
            return d;
        }
        
        double d = 1.0 - left * right;
        return d;
    }
    
    @Override public boolean isSimilarity() {
		return true;
	}
}
