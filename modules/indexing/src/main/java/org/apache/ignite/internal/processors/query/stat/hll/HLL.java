/*
 * Copyright 2013 Aggregate Knowledge, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.stat.hll;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.ignite.internal.processors.query.stat.hll.serialization.HLLMetadata;
import org.apache.ignite.internal.processors.query.stat.hll.serialization.IHLLMetadata;
import org.apache.ignite.internal.processors.query.stat.hll.serialization.ISchemaVersion;
import org.apache.ignite.internal.processors.query.stat.hll.serialization.IWordDeserializer;
import org.apache.ignite.internal.processors.query.stat.hll.serialization.IWordSerializer;
import org.apache.ignite.internal.processors.query.stat.hll.serialization.SerializationUtil;
import org.apache.ignite.internal.processors.query.stat.hll.util.BitUtil;
import org.apache.ignite.internal.processors.query.stat.hll.util.BitVector;
import org.apache.ignite.internal.processors.query.stat.hll.util.HLLUtil;
import org.apache.ignite.internal.processors.query.stat.hll.util.LongIterator;
import org.apache.ignite.internal.processors.query.stat.hll.util.NumberUtil;

/**
 * A probabilistic set of hashed <code>long</code> elements. Useful for computing
 * the approximate cardinality of a stream of data in very small storage.<p/>
 *
 * A modified version of the <a href="http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf">
 * 'HyperLogLog' data structure and algorithm</a> is used, which combines both
 * probabilistic and non-probabilistic techniques to improve the accuracy and
 * storage requirements of the original algorithm.<p/>
 *
 * More specifically, initializing and storing a new {@link HLL} will
 * allocate a sentinel value symbolizing the empty set ({@link HLLType#EMPTY}).
 * After adding the first few values, a sorted list of unique integers is
 * stored in a {@link HLLType#EXPLICIT} hash set. When configured, accuracy can
 * be sacrificed for memory footprint: the values in the sorted list are
 * "promoted" to a "{@link HLLType#SPARSE}" map-based HyperLogLog structure.
 * Finally, when enough registers are set, the map-based HLL will be converted
 * to a bit-packed "{@link HLLType#FULL}" HyperLogLog structure.<p/>
 *
 * This data structure is interoperable with the implementations found at:
 * <ul>
 *   <li><a href="https://github.com/aggregateknowledge/postgresql-hll">postgresql-hll</a>, and</li>
 *   <li><a href="https://github.com/aggregateknowledge/js-hll">js-hll</a></li>
 * </ul>
 * when <a href="https://github.com/aggregateknowledge/postgresql-hll/blob/master/STORAGE.markdown">properly serialized</a>.
 *
 * @author timon
 */
public class HLL implements Cloneable {
    /** Minimum value for the log-base-2 of the number of registers in the HLL. */
    public static final int MINIMUM_LOG2M_PARAM = 4;

    /** Maximum value for the log-base-2 of the number of registers in the HLL. */
    public static final int MAXIMUM_LOG2M_PARAM = 30;

    /** Minimum value for the register width of the HLL. */
    public static final int MINIMUM_REGWIDTH_PARAM = 1;

    /** Maximum values for the register width of the HLL. */
    public static final int MAXIMUM_REGWIDTH_PARAM = 8;

    // minimum and maximum values for the 'expthresh' parameter of the
    // constructor that is meant to match the PostgreSQL implementation's
    // constructor and parameter names
    /** */
    public static final int MINIMUM_EXPTHRESH_PARAM = -1;

    /** */
    public static final int MAXIMUM_EXPTHRESH_PARAM = 18;

    /** */
    public static final int MAXIMUM_EXPLICIT_THRESHOLD = (1 << (MAXIMUM_EXPTHRESH_PARAM - 1)/*per storage spec*/);

    // ************************************************************************
    // Storage
    // storage used when #type is EXPLICIT, null otherwise
    private HashSet<Long> explicitStorage;

    // storage used when #type is SPARSE, null otherwise
    private HashMap<Integer, Byte> sparseProbabilisticStorage;

    // storage used when #type is FULL, null otherwise
    private BitVector probabilisticStorage;

    // current type of this HLL instance, if this changes then so should the
    // storage used (see above)
    private HLLType type;

    // ------------------------------------------------------------------------
    // Characteristic parameters
    // NOTE:  These members are named to match the PostgreSQL implementation's
    //        parameters.
    // log2(the number of probabilistic HLL registers)
    private final int log2m;

    // the size (width) each register in bits
    private final int regwidth;

    // ------------------------------------------------------------------------
    // Computed constants
    // ........................................................................
    // EXPLICIT-specific constants
    // flag indicating if the EXPLICIT representation should NOT be used
    private final boolean explicitOff;

    // flag indicating that the promotion threshold from EXPLICIT should be
    // computed automatically
    // NOTE:  this only has meaning when 'explicitOff' is false
    private final boolean explicitAuto;

    // threshold (in element count) at which a EXPLICIT HLL is converted to a
    // SPARSE or FULL HLL, always greater than or equal to zero and always a
    // power of two OR simply zero
    // NOTE:  this only has meaning when 'explicitOff' is false
    private final int explicitThreshold;

    // ........................................................................
    /** SPARSE-specific constants the computed width of the short words */
    private final int shortWordLength;

    /** flag indicating if the SPARSE representation should not be used. */
    private final boolean sparseOff;

    /** threshold (in register count) at which a SPARSE HLL is converted to a FULL HLL, always greater than zero. */
    private final int sparseThreshold;

    // ........................................................................
    /** Probabilistic algorithm constants the number of registers, will always be a power of 2. */
    private final int m;

    /** a mask of the log2m bits set to one and the rest to zero */
    private final int mBitsMask;

    /** a mask as wide as a register (see #fromBytes()) */
    private final int valueMask;

    /** mask used to ensure that p(w) does not overflow register (see #Constructor() and #addRaw()) */
    private final long pwMaxMask;

    /** alpha * m^2 (the constant in the "'raw' HyperLogLog estimator") */
    private final double alphaMSquared;

    /** the cutoff value of the estimator for using the "small" range cardinality correction formula. */
    private final double smallEstimatorCutoff;

    /** the cutoff value of the estimator for using the "large" range cardinality correction formula. */
    private final double largeEstimatorCutoff;

    // ========================================================================
    /**
     * NOTE: Arguments here are named and structured identically to those in the
     *       PostgreSQL implementation, which can be found
     *       <a href="https://github.com/aggregateknowledge/postgresql-hll/blob/master/
     *       README.markdown#explanation-of-parameters-and-tuning">here</a>.
     *
     * @param log2m log-base-2 of the number of registers used in the HyperLogLog
     *        algorithm. Must be at least 4 and at most 30.
     * @param regwidth number of bits used per register in the HyperLogLog
     *        algorithm. Must be at least 1 and at most 8.
     * @param expthresh tunes when the {@link HLLType#EXPLICIT} to
     *        {@link HLLType#SPARSE} promotion occurs,
     *        based on the set's cardinality. Must be at least -1 and at most 18.
     *        <table>
     *        <thead><tr><th><code>expthresh</code> value</th><th>Meaning</th></tr></thead>
     *        <tbody>
     *        <tr>
     *            <td>-1</td>
     *            <td>Promote at whatever cutoff makes sense for optimal memory usage. ('auto' mode)</td>
     *        </tr>
     *        <tr>
     *            <td>0</td>
     *            <td>Skip <code>EXPLICIT</code> representation in hierarchy.</td>
     *        </tr>
     *        <tr>
     *            <td>1-18</td>
     *            <td>Promote at 2<sup>expthresh - 1</sup> cardinality</td>
     *        </tr>
     *        </tbody>
     *        </table>
     * @param sparseon Flag indicating if the {@link HLLType#SPARSE}
     *        representation should be used.
     * @param type the type in the promotion hierarchy which this instance should
     *        start at. This cannot be <code>null</code>.
     */
    public HLL(final int log2m, final int regwidth, final int expthresh, final boolean sparseon, final HLLType type) {
        this.log2m = log2m;

        if ((log2m < MINIMUM_LOG2M_PARAM) || (log2m > MAXIMUM_LOG2M_PARAM))
            throw new IllegalArgumentException("'log2m' must be at least " + MINIMUM_LOG2M_PARAM + " and at most " +
                MAXIMUM_LOG2M_PARAM + " (was: " + log2m + ")");

        this.regwidth = regwidth;

        if ((regwidth < MINIMUM_REGWIDTH_PARAM) || (regwidth > MAXIMUM_REGWIDTH_PARAM))
            throw new IllegalArgumentException("'regwidth' must be at least " + MINIMUM_REGWIDTH_PARAM +
                " and at most " + MAXIMUM_REGWIDTH_PARAM + " (was: " + regwidth + ")");

        this.m = (1 << log2m);
        this.mBitsMask = m - 1;
        this.valueMask = (1 << regwidth) - 1;
        this.pwMaxMask = HLLUtil.pwMaxMask(regwidth);
        this.alphaMSquared = HLLUtil.alphaMSquared(m);
        this.smallEstimatorCutoff = HLLUtil.smallEstimatorCutoff(m);
        this.largeEstimatorCutoff = HLLUtil.largeEstimatorCutoff(log2m, regwidth);

        if (expthresh == -1) {
            this.explicitAuto = true;
            this.explicitOff = false;

            // NOTE:  This math matches the size calculation in the PostgreSQL impl.
            final long fullRepresentationSize = (this.regwidth * (long)this.m + 7/*round up to next whole byte*/)
                / Byte.SIZE;
            final int numLongs = (int)(fullRepresentationSize / 8/*integer division to round down*/);

            if (numLongs > MAXIMUM_EXPLICIT_THRESHOLD)
                this.explicitThreshold = MAXIMUM_EXPLICIT_THRESHOLD;
            else
                this.explicitThreshold = numLongs;

        }
        else if (expthresh == 0) {
            this.explicitAuto = false;
            this.explicitOff = true;
            this.explicitThreshold = 0;
        }
        else if ((expthresh > 0) && (expthresh <= MAXIMUM_EXPTHRESH_PARAM)) {
            this.explicitAuto = false;
            this.explicitOff = false;
            this.explicitThreshold = (1 << (expthresh - 1));
        }
        else
            throw new IllegalArgumentException("'expthresh' must be at least " + MINIMUM_EXPTHRESH_PARAM +
                " and at most " + MAXIMUM_EXPTHRESH_PARAM + " (was: " + expthresh + ")");

        this.shortWordLength = (regwidth + log2m);
        this.sparseOff = !sparseon;
        if (this.sparseOff)
            this.sparseThreshold = 0;
        else {
            // TODO improve this cutoff to include the cost overhead of Java
            //      members/objects
            final int largestPow2LessThanCutoff = (int)NumberUtil.log2((this.m * this.regwidth) / this.shortWordLength);
            this.sparseThreshold = (1 << largestPow2LessThanCutoff);
        }

        initializeStorage(type);
    }

    /**
     *  Construct an empty HLL with the given {@code log2m} and {@code regwidth}.<p/>
     *
     *  This is equivalent to calling <code>HLL(log2m, regwidth, -1, true, HLLType.EMPTY)</code>.
     *
     * @param log2m log-base-2 of the number of registers used in the HyperLogLog
     *        algorithm. Must be at least 4 and at most 30.
     * @param regwidth number of bits used per register in the HyperLogLog
     *        algorithm. Must be at least 1 and at most 8.
     *
     * @see #HLL(int, int, int, boolean, HLLType)
     */
    public HLL(final int log2m, final int regwidth) {
        this(log2m, regwidth, -1, true, HLLType.EMPTY);
    }

    // -------------------------------------------------------------------------
    /**
     * Convenience constructor for testing. Assumes that both {@link HLLType#EXPLICIT}
     * and {@link HLLType#SPARSE} representations should be enabled.
     *
     * @param log2m log-base-2 of the number of registers used in the HyperLogLog
     *        algorithm. Must be at least 4 and at most 30.
     * @param regwidth number of bits used per register in the HyperLogLog
     *        algorithm. Must be at least 1 and at most 8.
     * @param explicitThreshold cardinality threshold at which the {@link HLLType#EXPLICIT}
     *        representation should be promoted to {@link HLLType#SPARSE}.
     *        This must be greater than zero and less than or equal to {@value #MAXIMUM_EXPLICIT_THRESHOLD}.
     * @param sparseThreshold register count threshold at which the {@link HLLType#SPARSE}
     *        representation should be promoted to {@link HLLType#FULL}.
     *        This must be greater than zero.
     * @param type the type in the promotion hierarchy which this instance should
     *        start at. This cannot be <code>null</code>.
     */
    /*package, for testing*/ HLL(
        final int log2m,
        final int regwidth,
        final int explicitThreshold,
        final int sparseThreshold,
        final HLLType type
    ) {
        this.log2m = log2m;

        if ((log2m < MINIMUM_LOG2M_PARAM) || (log2m > MAXIMUM_LOG2M_PARAM))
            throw new IllegalArgumentException("'log2m' must be at least " + MINIMUM_LOG2M_PARAM +
                " and at most " + MAXIMUM_LOG2M_PARAM + " (was: " + log2m + ")");

        this.regwidth = regwidth;

        if ((regwidth < MINIMUM_REGWIDTH_PARAM) || (regwidth > MAXIMUM_REGWIDTH_PARAM))
            throw new IllegalArgumentException("'regwidth' must be at least " + MINIMUM_REGWIDTH_PARAM +
                " and at most " + MAXIMUM_REGWIDTH_PARAM + " (was: " + regwidth + ")");

        this.m = (1 << log2m);
        this.mBitsMask = m - 1;
        this.valueMask = (1 << regwidth) - 1;
        this.pwMaxMask = HLLUtil.pwMaxMask(regwidth);
        this.alphaMSquared = HLLUtil.alphaMSquared(m);
        this.smallEstimatorCutoff = HLLUtil.smallEstimatorCutoff(m);
        this.largeEstimatorCutoff = HLLUtil.largeEstimatorCutoff(log2m, regwidth);

        this.explicitAuto = false;
        this.explicitOff = false;
        this.explicitThreshold = explicitThreshold;

        if ((explicitThreshold < 1) || (explicitThreshold > MAXIMUM_EXPLICIT_THRESHOLD))
            throw new IllegalArgumentException("'explicitThreshold' must be at least 1 and at most " +
                MAXIMUM_EXPLICIT_THRESHOLD + " (was: " + explicitThreshold + ")");

        this.shortWordLength = (regwidth + log2m);
        this.sparseOff = false;
        this.sparseThreshold = sparseThreshold;

        initializeStorage(type);
    }

    /**
     * @return the type in the promotion hierarchy of this instance. This will
     *         never be <code>null</code>.
     */
    public HLLType getType() { return type; }

    // ========================================================================
    // Add
    /**
     * Adds <code>rawValue</code> directly to the HLL.
     *
     * @param  rawValue the value to be added. It is very important that this
     *         value <em>already be hashed</em> with a strong (but not
     *         necessarily cryptographic) hash function. For instance, the
     *         Murmur3 implementation in
     *         <a href="http://guava-libraries.googlecode.com/git/guava/src/com/google/common/hash/Murmur3_128HashFunction.java">
     *         Google's Guava</a> library is an excellent hash function for this
     *         purpose and, for seeds greater than zero, matches the output
     *         of the hash provided in the PostgreSQL implementation.
     */
    public void addRaw(final long rawValue) {
        switch (type) {
            case EMPTY: {
                // NOTE:  EMPTY type is always promoted on #addRaw()
                if (explicitThreshold > 0) {
                    initializeStorage(HLLType.EXPLICIT);
                    explicitStorage.add(rawValue);
                }
                else if (!sparseOff) {
                    initializeStorage(HLLType.SPARSE);
                    addRawSparseProbabilistic(rawValue);
                }
                else {
                    initializeStorage(HLLType.FULL);
                    addRawProbabilistic(rawValue);
                }

                return;
            }
            case EXPLICIT: {
                explicitStorage.add(rawValue);

                // promotion, if necessary
                if (explicitStorage.size() > explicitThreshold) {
                    if (!sparseOff) {
                        initializeStorage(HLLType.SPARSE);

                        for (final long value : explicitStorage)
                            addRawSparseProbabilistic(value);
                    }
                    else {
                        initializeStorage(HLLType.FULL);

                        for (final long value : explicitStorage)
                            addRawProbabilistic(value);
                    }
                    explicitStorage = null;
                }

                return;
            }
            case SPARSE: {
                addRawSparseProbabilistic(rawValue);

                // promotion, if necessary
                if (sparseProbabilisticStorage.size() > sparseThreshold) {
                    initializeStorage(HLLType.FULL);

                    for (final int registerIndex : sparseProbabilisticStorage.keySet()) {
                        final byte registerVal = sparseProbabilisticStorage.getOrDefault(registerIndex, (byte)0);
                        probabilisticStorage.setMaxRegister(registerIndex, registerVal);
                    }
                    sparseProbabilisticStorage = null;
                }

                return;
            }
            case FULL:
                addRawProbabilistic(rawValue);
                return;
            default:
                throw new RuntimeException("Unsupported HLL type " + type);
        }
    }

    // ------------------------------------------------------------------------
    // #addRaw(..) helpers
    /**
     * Adds the raw value to the {@link #sparseProbabilisticStorage}.
     * {@link #type} must be {@link HLLType#SPARSE}.
     *
     * @param rawValue the raw value to add to the sparse storage.
     */
    private void addRawSparseProbabilistic(final long rawValue) {
        // p(w): position of the least significant set bit (one-indexed)
        // By contract: p(w) <= 2^(registerValueInBits) - 1 (the max register value)
        //
        // By construction of pwMaxMask (see #Constructor()),
        //      lsb(pwMaxMask) = 2^(registerValueInBits) - 2,
        // thus lsb(any_long | pwMaxMask) <= 2^(registerValueInBits) - 2,
        // thus 1 + lsb(any_long | pwMaxMask) <= 2^(registerValueInBits) -1.
        final long substreamValue = (rawValue >>> log2m);
        final byte p_w;

        if (substreamValue == 0L) {
            // The paper does not cover p(0x0), so the special value 0 is used.
            // 0 is the original initialization value of the registers, so by
            // doing this the multiset simply ignores it. This is acceptable
            // because the probability is 1/(2^(2^registerSizeInBits)).
            p_w = 0;
        }
        else
            p_w = (byte)(1 + BitUtil.leastSignificantBit(substreamValue | pwMaxMask));

        // Short-circuit if the register is being set to zero, since algorithmically
        // this corresponds to an "unset" register, and "unset" registers aren't
        // stored to save memory. (The very reason this sparse implementation
        // exists.) If a register is set to zero it will break the #algorithmCardinality
        // code.
        if (p_w == 0)
            return;

        // NOTE:  no +1 as in paper since 0-based indexing
        final int j = (int)(rawValue & mBitsMask);

        final byte curVal = sparseProbabilisticStorage.getOrDefault(j, (byte)0);

        if (p_w > curVal)
            sparseProbabilisticStorage.put(j, p_w);

    }

    /**
     * Adds the raw value to the {@link #probabilisticStorage}.
     * {@link #type} must be {@link HLLType#FULL}.
     *
     * @param rawVal the raw value to add to the full probabilistic storage.
     */
    private void addRawProbabilistic(final long rawVal) {
        // p(w): position of the least significant set bit (one-indexed)
        // By contract: p(w) <= 2^(registerValueInBits) - 1 (the max register value)
        //
        // By construction of pwMaxMask (see #Constructor()),
        //      lsb(pwMaxMask) = 2^(registerValueInBits) - 2,
        // thus lsb(any_long | pwMaxMask) <= 2^(registerValueInBits) - 2,
        // thus 1 + lsb(any_long | pwMaxMask) <= 2^(registerValueInBits) -1.
        final long substreamVal = (rawVal >>> log2m);
        final byte p_w;

        if (substreamVal == 0L) {
            // The paper does not cover p(0x0), so the special value 0 is used.
            // 0 is the original initialization value of the registers, so by
            // doing this the multiset simply ignores it. This is acceptable
            // because the probability is 1/(2^(2^registerSizeInBits)).
            p_w = 0;
        }
        else
            p_w = (byte)(1 + BitUtil.leastSignificantBit(substreamVal | pwMaxMask));

        // Short-circuit if the register is being set to zero, since algorithmically
        // this corresponds to an "unset" register, and "unset" registers aren't
        // stored to save memory. (The very reason this sparse implementation
        // exists.) If a register is set to zero it will break the #algorithmCardinality
        // code.
        if (p_w == 0)
            return;

        // NOTE:  no +1 as in paper since 0-based indexing
        final int j = (int)(rawVal & mBitsMask);

        probabilisticStorage.setMaxRegister(j, p_w);
    }

    // ------------------------------------------------------------------------
    // Storage helper
    /**
     * Initializes storage for the specified {@link HLLType} and changes the
     * instance's {@link #type}.
     *
     * @param type the {@link HLLType} to initialize storage for. This cannot be
     *        <code>null</code> and must be an instantiable type. (For instance,
     *        it cannot be {@link HLLType#UNDEFINED}.)
     */
    private void initializeStorage(final HLLType type) {
        this.type = type;

        switch (type) {
            case EMPTY:
                // nothing to be done
                break;
            case EXPLICIT:
                this.explicitStorage = new HashSet<>();

                break;
            case SPARSE:
                this.sparseProbabilisticStorage = new HashMap<>();

                break;
            case FULL:
                this.probabilisticStorage = new BitVector(regwidth, m);

                break;
            default:
                throw new RuntimeException("Unsupported HLL type " + type);
        }
    }

    // ========================================================================
    // Cardinality
    /**
     * Computes the cardinality of the HLL.
     *
     * @return the cardinality of HLL. This will never be negative.
     */
    public long cardinality() {
        switch (type) {
            case EMPTY:
                return 0/*by definition*/;
            case EXPLICIT:
                return explicitStorage.size();
            case SPARSE:
                return (long)Math.ceil(sparseProbabilisticAlgorithmCardinality());
            case FULL:
                return (long)Math.ceil(fullProbabilisticAlgorithmCardinality());
            default:
                throw new RuntimeException("Unsupported HLL type " + type);
        }
    }

    // ------------------------------------------------------------------------
    // Cardinality helpers
    /**
     * Computes the exact cardinality value returned by the HLL algorithm when
     * represented as a {@link HLLType#SPARSE} HLL. Kept
     * separate from {@link #cardinality()} for testing purposes. {@link #type}
     * must be {@link HLLType#SPARSE}.
     *
     * @return the exact, unrounded cardinality given by the HLL algorithm
     */
    /*package, for testing*/ double sparseProbabilisticAlgorithmCardinality() {
        final int m = this.m/*for performance*/;

        // compute the "indicator function" -- sum(2^(-M[j])) where M[j] is the
        // 'j'th register value
        double sum = 0;
        int numberOfZeroes = 0/*"V" in the paper*/;

        for (int j = 0; j < m; j++) {
            final long register = sparseProbabilisticStorage.getOrDefault(j, (byte)0);

            sum += 1.0 / (1L << register);

            if (register == 0L) numberOfZeroes++;
        }

        // apply the estimate and correction to the indicator function
        final double estimator = alphaMSquared / sum;

        if ((numberOfZeroes != 0) && (estimator < smallEstimatorCutoff))
            return HLLUtil.smallEstimator(m, numberOfZeroes);
        else if (estimator <= largeEstimatorCutoff)
            return estimator;
        else
            return HLLUtil.largeEstimator(log2m, regwidth, estimator);
    }

    /**
     * Computes the exact cardinality value returned by the HLL algorithm when
     * represented as a {@link HLLType#FULL} HLL. Kept
     * separate from {@link #cardinality()} for testing purposes. {@link #type}
     * must be {@link HLLType#FULL}.
     *
     * @return the exact, unrounded cardinality given by the HLL algorithm
     */
    /*package, for testing*/ double fullProbabilisticAlgorithmCardinality() {
        final int m = this.m/*for performance*/;

        // compute the "indicator function" -- sum(2^(-M[j])) where M[j] is the
        // 'j'th register value
        double sum = 0;
        int numberOfZeroes = 0/*"V" in the paper*/;
        final LongIterator iterator = probabilisticStorage.registerIterator();

        while (iterator.hasNext()) {
            final long register = iterator.next();

            sum += 1.0 / (1L << register);
            if (register == 0L) numberOfZeroes++;
        }

        // apply the estimate and correction to the indicator function
        final double estimator = alphaMSquared / sum;

        if ((numberOfZeroes != 0) && (estimator < smallEstimatorCutoff))
            return HLLUtil.smallEstimator(m, numberOfZeroes);
        else if (estimator <= largeEstimatorCutoff)
            return estimator;
        else
            return HLLUtil.largeEstimator(log2m, regwidth, estimator);
    }

    // ========================================================================
    // Clear
    /**
     * Clears the HLL. The HLL will have cardinality zero and will act as if no
     * elements have been added.<p/>
     *
     * NOTE: Unlike {@link #addRaw(long)}, <code>clear</code> does NOT handle
     * transitions between {@link HLLType}s - a probabilistic type will remain
     * probabilistic after being cleared.
     */
    public void clear() {
        switch (type) {
            case EMPTY:
                return /*do nothing*/;
            case EXPLICIT:
                explicitStorage.clear();
                return;
            case SPARSE:
                sparseProbabilisticStorage.clear();
                return;
            case FULL:
                probabilisticStorage.fill(0);
                return;
            default:
                throw new RuntimeException("Unsupported HLL type " + type);
        }
    }

    // ========================================================================
    // Union
    /**
     * Computes the union of HLLs and stores the result in this instance.
     *
     * @param other the other {@link HLL} instance to union into this one. This
     *        cannot be <code>null</code>.
     */
    public void union(final HLL other) {
        // TODO: verify HLLs are compatible
        final HLLType otherType = other.getType();

        if (type.equals(otherType)) {
            homogeneousUnion(other);

            return;
        }
        else {
            heterogenousUnion(other);

            return;
        }
    }

    // ------------------------------------------------------------------------
    // Union helpers
    /**
     * Computes the union of two HLLs, of different types, and stores the
     * result in this instance.
     *
     * @param other the other {@link HLL} instance to union into this one. This
     *        cannot be <code>null</code>.
     */
    /*package, for testing*/ void heterogenousUnion(final HLL other) {
        /*
         * The logic here is divided into two sections: unions with an EMPTY
         * HLL, and unions between EXPLICIT/SPARSE/FULL
         * HLL.
         *
         * Between those two sections, all possible heterogeneous unions are
         * covered. Should another type be added to HLLType whose unions
         * are not easily reduced (say, as EMPTY's are below) this may be more
         * easily implemented as Strategies. However, that is unnecessary as it
         * stands.
         */

        // ....................................................................
        // Union with an EMPTY
        if (HLLType.EMPTY.equals(type)) {
            // NOTE:  The union of empty with non-empty HLL is just a
            //        clone of the non-empty.

            switch (other.getType()) {
                case EXPLICIT: {
                    // src:  EXPLICIT
                    // dest: EMPTY

                    if (other.explicitStorage.size() <= explicitThreshold) {
                        type = HLLType.EXPLICIT;
                        explicitStorage = (HashSet<Long>) other.explicitStorage.clone();
                    }
                    else {
                        if (!sparseOff)
                            initializeStorage(HLLType.SPARSE);
                        else
                            initializeStorage(HLLType.FULL);

                        for (final long value : other.explicitStorage)
                            addRaw(value);
                    }

                    return;
                }
                case SPARSE: {
                    // src:  SPARSE
                    // dest: EMPTY

                    if (!sparseOff) {
                        type = HLLType.SPARSE;
                        sparseProbabilisticStorage = (HashMap<Integer, Byte>)other.sparseProbabilisticStorage.clone();
                    }
                    else {
                        initializeStorage(HLLType.FULL);

                        for (final int registerIndex : other.sparseProbabilisticStorage.keySet()) {
                            final byte registerValue = other.sparseProbabilisticStorage.get(registerIndex);
                            probabilisticStorage.setMaxRegister(registerIndex, registerValue);
                        }
                    }
                    return;
                }
                default/*case FULL*/: {
                    // src:  FULL
                    // dest: EMPTY

                    type = HLLType.FULL;
                    probabilisticStorage = other.probabilisticStorage.clone();

                    return;
                }
            }
        }
        else if (HLLType.EMPTY.equals(other.getType())) {
            // source is empty, so just return destination since it is unchanged
            return;
        } /* else -- both of the sets are not empty */

        // ....................................................................
        // NOTE: Since EMPTY is handled above, the HLLs are non-EMPTY below
        switch (type) {
            case EXPLICIT: {
                // src:  FULL/SPARSE
                // dest: EXPLICIT
                // "Storing into destination" cannot be done (since destination
                // is by definition of smaller capacity than source), so a clone
                // of source is made and values from destination are inserted
                // into that.

                // Determine source and destination storage.
                // NOTE:  destination storage may change through promotion if
                //        source is SPARSE.
                if (HLLType.SPARSE.equals(other.getType())) {
                    if (!sparseOff) {
                        type = HLLType.SPARSE;
                        sparseProbabilisticStorage = (HashMap<Integer, Byte>)other.sparseProbabilisticStorage.clone();
                    }
                    else {
                        initializeStorage(HLLType.FULL);

                        for (final int registerIndex : other.sparseProbabilisticStorage.keySet()) {
                            final byte registerValue = other.sparseProbabilisticStorage.get(registerIndex);
                            probabilisticStorage.setMaxRegister(registerIndex, registerValue);
                        }
                    }
                }
                else /*source is HLLType.FULL*/ {
                    type = HLLType.FULL;
                    probabilisticStorage = other.probabilisticStorage.clone();
                }

                for (final long value : explicitStorage)
                    addRaw(value);

                explicitStorage = null;
                return;
            }
            case SPARSE: {
                if (HLLType.EXPLICIT.equals(other.getType())) {
                    // src:  EXPLICIT
                    // dest: SPARSE
                    // Add the raw values from the source to the destination.

                    for (final long value : other.explicitStorage)
                        addRaw(value);

                    // NOTE:  addRaw will handle promotion cleanup
                }
                else /*source is HLLType.FULL*/ {
                    // src:  FULL
                    // dest: SPARSE
                    // "Storing into destination" cannot be done (since destination
                    // is by definition of smaller capacity than source), so a
                    // clone of source is made and registers from the destination
                    // are merged into the clone.

                    type = HLLType.FULL;
                    probabilisticStorage = other.probabilisticStorage.clone();

                    for (final int registerIndex : sparseProbabilisticStorage.keySet()) {
                        final byte registerValue = sparseProbabilisticStorage.get(registerIndex);
                        probabilisticStorage.setMaxRegister(registerIndex, registerValue);
                    }

                    sparseProbabilisticStorage = null;
                }

                return;
            }
            default/*destination is HLLType.FULL*/: {
                if (HLLType.EXPLICIT.equals(other.getType())) {
                    // src:  EXPLICIT
                    // dest: FULL
                    // Add the raw values from the source to the destination.
                    // Promotion is not possible, so don't bother checking.

                    for (final long value : other.explicitStorage)
                        addRaw(value);

                }
                else /*source is HLLType.SPARSE*/ {
                    // src:  SPARSE
                    // dest: FULL
                    // Merge the registers from the source into the destination.
                    // Promotion is not possible, so don't bother checking.

                    for (final int registerIndex : other.sparseProbabilisticStorage.keySet()) {
                        final byte registerValue = other.sparseProbabilisticStorage.get(registerIndex);
                        probabilisticStorage.setMaxRegister(registerIndex, registerValue);
                    }
                }

            }
        }
    }

    /**
     * Computes the union of two HLLs of the same type, and stores the
     * result in this instance.
     *
     * @param other the other {@link HLL} instance to union into this one. This
     *        cannot be <code>null</code>.
     */
    private void homogeneousUnion(final HLL other) {
        switch (type) {
            case EMPTY:
                // union of empty and empty is empty
                return;
            case EXPLICIT:
                for (final long value : other.explicitStorage)
                    addRaw(value);

                // NOTE:  #addRaw() will handle promotion, if necessary
                return;
            case SPARSE:
                for (final int registerIndex : other.sparseProbabilisticStorage.keySet()) {
                    final byte registerValue = other.sparseProbabilisticStorage.get(registerIndex);
                    final byte currentRegisterValue = sparseProbabilisticStorage.getOrDefault(registerIndex, (byte)0);

                    if (registerValue > currentRegisterValue)
                        sparseProbabilisticStorage.put(registerIndex, registerValue);
                }

                // promotion, if necessary
                if (sparseProbabilisticStorage.size() > sparseThreshold) {
                    initializeStorage(HLLType.FULL);

                    for (final int registerIndex : sparseProbabilisticStorage.keySet()) {
                        final byte registerValue = sparseProbabilisticStorage.get(registerIndex);
                        probabilisticStorage.setMaxRegister(registerIndex, registerValue);
                    }

                    sparseProbabilisticStorage = null;
                }

                return;
            case FULL:
                for (int i = 0; i < m; i++) {
                    final long registerValue = other.probabilisticStorage.getRegister(i);
                    probabilisticStorage.setMaxRegister(i, registerValue);
                }

                return;
            default:
                throw new RuntimeException("Unsupported HLL type " + type);
        }
    }

    // ========================================================================
    // Serialization
    /**
     * Serializes the HLL to an array of bytes in correspondence with the format
     * of the default schema version, {@link SerializationUtil#DEFAULT_SCHEMA_VERSION}.
     *
     * @return the array of bytes representing the HLL. This will never be
     *         <code>null</code> or empty.
     */
    public byte[] toBytes() {
        return toBytes(SerializationUtil.DEFAULT_SCHEMA_VERSION);
    }

    /**
     * Serializes the HLL to an array of bytes in correspondence with the format
     * of the specified schema version.
     *
     * @param  schemaVersion the schema version dictating the serialization format
     * @return the array of bytes representing the HLL. This will never be
     *         <code>null</code> or empty.
     */
    public byte[] toBytes(final ISchemaVersion schemaVersion) {
        final byte[] bytes;
        switch (type) {
            case EMPTY:
                bytes = new byte[schemaVersion.paddingBytes(type)];

                break;
            case EXPLICIT: {
                final IWordSerializer serializer = schemaVersion.getSerializer(type, Long.SIZE, explicitStorage.size());

                final long[] values = explicitStorage.stream().mapToLong(Long::longValue).toArray();
                Arrays.sort(values);

                for (final long value : values)
                    serializer.writeWord(value);

                bytes = serializer.getBytes();

                break;
            }
            case SPARSE: {
                final IWordSerializer serializer =
                    schemaVersion.getSerializer(type, shortWordLength, sparseProbabilisticStorage.size());

                final int[] indices = sparseProbabilisticStorage.keySet().stream().mapToInt(Integer::intValue).toArray();
                Arrays.sort(indices);

                for (final int registerIndex : indices) {
                    final long registerValue = sparseProbabilisticStorage.get(registerIndex);
                    // pack index and value into "short word"
                    final long shortWord = ((registerIndex << regwidth) | registerValue);
                    serializer.writeWord(shortWord);
                }

                bytes = serializer.getBytes();

                break;
            }
            case FULL: {
                final IWordSerializer serializer = schemaVersion.getSerializer(type, regwidth, m);
                probabilisticStorage.getRegisterContents(serializer);

                bytes = serializer.getBytes();

                break;
            }
            default:
                throw new RuntimeException("Unsupported HLL type " + type);
        }

        final IHLLMetadata metadata = new HLLMetadata(schemaVersion.schemaVersionNumber(),
            type,
            log2m,
            regwidth,
            (int)NumberUtil.log2(explicitThreshold),
            explicitOff,
            explicitAuto,
            !sparseOff);
        schemaVersion.writeMetadata(bytes, metadata);

        return bytes;
    }

    /**
     * Deserializes the HLL (in {@link #toBytes(ISchemaVersion)} format) serialized
     * into <code>bytes</code>.<p/>
     *
     * @param  bytes the serialized bytes of new HLL
     * @return the deserialized HLL. This will never be <code>null</code>.
     *
     * @see #toBytes(ISchemaVersion)
     */
    public static HLL fromBytes(final byte[] bytes) {
        final ISchemaVersion schemaVersion = SerializationUtil.getSchemaVersion(bytes);
        final IHLLMetadata metadata = schemaVersion.readMetadata(bytes);

        final HLLType type = metadata.HLLType();
        final int regwidth = metadata.registerWidth();
        final int log2m = metadata.registerCountLog2();
        final boolean sparseon = metadata.sparseEnabled();

        final int expthresh;

        if (metadata.explicitAuto())
            expthresh = -1;
        else if (metadata.explicitOff())
            expthresh = 0;
        else {
            // NOTE: take into account that the postgres-compatible constructor
            //       subtracts one before taking a power of two.
            expthresh = metadata.log2ExplicitCutoff() + 1;
        }

        final HLL hll = new HLL(log2m, regwidth, expthresh, sparseon, type);

        // Short-circuit on empty, which needs no other deserialization.
        if (HLLType.EMPTY.equals(type))
            return hll;

        final int wordLength;

        switch (type) {
            case EXPLICIT:
                wordLength = Long.SIZE;

                break;
            case SPARSE:
                wordLength = hll.shortWordLength;

                break;
            case FULL:
                wordLength = hll.regwidth;

                break;
            default:
                throw new RuntimeException("Unsupported HLL type " + type);
        }

        final IWordDeserializer deserializer = schemaVersion.getDeserializer(type, wordLength, bytes);

        switch (type) {
            case EXPLICIT:
                // NOTE:  This should not exceed expthresh and this will always
                //        be exactly the number of words that were encoded,
                //        because the word length is at least a byte wide.
                // SEE:   IWordDeserializer#totalWordCount()
                for (int i = 0; i < deserializer.totalWordCount(); i++)
                    hll.explicitStorage.add(deserializer.readWord());

                break;
            case SPARSE:
                // NOTE:  If the shortWordLength were smaller than 8 bits
                //        (1 byte) there would be a possibility (because of
                //        padding arithmetic) of having one or more extra
                //        registers read. However, this is not relevant as the
                //        extra registers will be all zeroes, which are ignored
                //        in the sparse representation.
                for (int i = 0; i < deserializer.totalWordCount(); i++) {
                    final long shortWord = deserializer.readWord();
                    final byte registerValue = (byte)(shortWord & hll.valueMask);

                    // Only set non-zero registers.
                    if (registerValue != 0)
                        hll.sparseProbabilisticStorage.put((int)(shortWord >>> hll.regwidth), registerValue);
                }

                break;
            case FULL:
                // NOTE:  Iteration is done using m (register count) and NOT
                //        deserializer#totalWordCount() because regwidth may be
                //        less than 8 and as such the padding on the 'last' byte
                //        may be larger than regwidth, causing an extra register
                //        to be read.
                // SEE: IWordDeserializer#totalWordCount()

                for (long i = 0; i < hll.m; i++)
                    hll.probabilisticStorage.setRegister(i, deserializer.readWord());

                break;
            default:
                throw new RuntimeException("Unsupported HLL type " + type);
        }

        return hll;
    }

    /**
     * Create a deep copy of this HLL.
     *
     * @see java.lang.Object#clone()
     */
    @Override public HLL clone() throws CloneNotSupportedException {
        // NOTE: Since the package-only constructor assumes both explicit and
        //       sparse are enabled, the easiest thing to do here is to re-derive
        //       the expthresh parameter and create a new HLL with the public
        //       constructor.
        final int copyExpthresh;

        if (explicitAuto)
            copyExpthresh = -1;
        else if (explicitOff)
            copyExpthresh = 0;
        else {
            // explicitThreshold is defined as:
            //
            //      this.explicitThreshold = (1 << (expthresh - 1));
            //
            // Since explicitThreshold is a power of two and only has a single
            // bit set, finding the LSB is the same as finding the inverse
            copyExpthresh = BitUtil.leastSignificantBit(explicitThreshold) + 1;
        }

        final HLL copy = new HLL(log2m, regwidth, copyExpthresh, !sparseOff/*sparseOn*/, type);

        switch (type) {
            case EMPTY:
                // nothing to be done
                break;
            case EXPLICIT:
                copy.explicitStorage = (HashSet<Long>)this.explicitStorage.clone();

                break;
            case SPARSE:
                copy.sparseProbabilisticStorage = (HashMap<Integer, Byte>)this.sparseProbabilisticStorage.clone();

                break;
            case FULL:
                copy.probabilisticStorage = this.probabilisticStorage.clone();

                break;
            default:
                throw new RuntimeException("Unsupported HLL type " + type);
        }
        return copy;
    }
}
