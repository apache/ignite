// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.hpc.closure;

import org.gridgain.grid.util.typedef.internal.*;

import java.math.*;
import java.util.*;

/**
 * This class for internal use in examples.
 *
 * @author @java.author
 * @version @java.version
 */
class GridNumberUtils {
    /** Random numbers generator. */
    static final Random rand = new Random();

    /**
     * Calculates greatest common divisor (GCD) of two numbers.
     *
     * @param a First number.
     * @param b Second number.
     * @return Greatest common divisor.
     */
    static int getGCD(int a, int b) {
        int x;
        int y;

        if (a > b) {
            x = a;
            y = b;
        }
        else {
            x = b;
            y = a;
        }

        int z = x % y;

        return (z == 0) ? y : getGCD(z, y);
    }

    /**
     * Calculates lowest common multiple (LCM) of two numbers.
     *
     * @param a First number.
     * @param b Second number.
     * @return Lowest common multiple.
     */
    static int getLCM(int a, int b) {
        int gcd = getGCD(a, b);

        return a * b / gcd;
    }

    /**
     * Generate new non zero random number.
     *
     * @param bound Bound of the random number.
     * @return Non zero random number.
     */
    static int getRand(int bound) {
        int num = rand.nextInt(bound);

        return (num != 0) ? num : getRand(bound);
    }

    /**
     * Calculates factorial.
     *
     * @param num Number for factorial.
     * @return Factorial.
     * @throws IllegalArgumentException if {@code num} is less than 0.
     */
    static BigInteger factorial(int num) {
        A.ensure(num >= 0, "factorial is not defined for negative argument");

        BigInteger fact = new BigInteger("1");

        for (int i = 2; i <= num; i++)
            fact = fact.multiply(new BigInteger(String.valueOf(i)));

        return fact;
    }

    /**
     * Defines whether number is prime.
     *
     * @param num Number.
     * @return {@code true} if number is prime, {@code false} if not.
     */
    static boolean isPrime(int num) {
        boolean prime = false;

        if (num > 0) {
            prime = true;

            for (int i = 2; i * i <= num; i++) {
                if (num % i == 0) {
                    prime = false;

                    break;
                }
            }
        }

        return prime;
    }
}
