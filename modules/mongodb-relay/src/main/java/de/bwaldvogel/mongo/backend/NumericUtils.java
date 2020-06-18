package de.bwaldvogel.mongo.backend;

import java.math.BigDecimal;

import de.bwaldvogel.mongo.bson.Decimal128;

public class NumericUtils {

    @FunctionalInterface
    public interface BigDecimalCalculation {
        BigDecimal apply(BigDecimal a, BigDecimal b);
    }

    @FunctionalInterface
    public interface DoubleCalculation {
        double apply(double a, double b);
    }

    @FunctionalInterface
    public interface LongCalculation {
        long apply(long a, long b);
    }

    private static Number calculate(Number a, Number b,
                                    LongCalculation longCalculation,
                                    DoubleCalculation doubleCalculation,
                                    BigDecimalCalculation bigDecimalCalculation) {
        if (a instanceof Decimal128 || b instanceof Decimal128) {
            BigDecimal result = bigDecimalCalculation.apply(toBigDecimal(a), toBigDecimal(b));
            return new Decimal128(result);
        } else if (a instanceof Double || b instanceof Double) {
            return Double.valueOf(doubleCalculation.apply(a.doubleValue(), b.doubleValue()));
        } else if (a instanceof Float || b instanceof Float) {
            double result = doubleCalculation.apply(a.doubleValue(), b.doubleValue());
            return Float.valueOf((float) result);
        } else if (a instanceof Long || b instanceof Long) {
            return Long.valueOf(longCalculation.apply(a.longValue(), b.longValue()));
        } else if (a instanceof Integer || b instanceof Integer) {
            long result = longCalculation.apply(a.longValue(), b.longValue());
            int intResult = (int) result;
            if (intResult == result) {
                return Integer.valueOf(intResult);
            } else {
                return Long.valueOf(result);
            }
        } else if (a instanceof Short || b instanceof Short) {
            long result = longCalculation.apply(a.longValue(), b.longValue());
            short shortResult = (short) result;
            if (shortResult == result) {
                return Short.valueOf(shortResult);
            } else {
                return Long.valueOf(result);
            }
        } else {
            throw new UnsupportedOperationException("cannot calculate on " + a + " and " + b);
        }
    }

    private static BigDecimal toBigDecimal(Number value) {
        return Decimal128.fromNumber(value).toBigDecimal();
    }

    public static Number addNumbers(Number one, Number other) {
        return calculate(one, other, Long::sum, Double::sum, BigDecimal::add);
    }

    public static Number subtractNumbers(Number one, Number other) {
        return calculate(one, other, (a, b) -> a - b, (a, b) -> a - b, BigDecimal::subtract);
    }

    public static Number multiplyNumbers(Number one, Number other) {
        return calculate(one, other, (a, b) -> a * b, (a, b) -> a * b, BigDecimal::multiply);
    }

}
