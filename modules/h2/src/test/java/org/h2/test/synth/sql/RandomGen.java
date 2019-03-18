/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.sql;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Random;

/**
 * A random data generator class.
 */
public class RandomGen {
    private final Random random = new Random();

    /**
     * Create a new random instance with a fixed seed value.
     */
    public RandomGen() {
        random.setSeed(12);
    }

    /**
     * Get the next integer that is smaller than max.
     *
     * @param max the upper limit (exclusive)
     * @return the random value
     */
    public int getInt(int max) {
        return max == 0 ? 0 : random.nextInt(max);
    }

    /**
     * Get the next gaussian value.
     *
     * @return the value
     */
    public double nextGaussian() {
        return random.nextGaussian();
    }

    /**
     * Get the next random value that is at most max but probably much lower.
     *
     * @param max the maximum value
     * @return the value
     */
    public int getLog(int max) {
        if (max == 0) {
            return 0;
        }
        while (true) {
            int d = Math.abs((int) (random.nextGaussian() / 2. * max));
            if (d < max) {
                return d;
            }
        }
    }

    /**
     * Get a number of random bytes.
     *
     * @param data the target buffer
     */
    public void getBytes(byte[] data) {
        random.nextBytes(data);
    }

    /**
     * Get a boolean that is true with the given probability in percent.
     *
     * @param percent the probability
     * @return the boolean value
     */
    public boolean getBoolean(int percent) {
        return random.nextInt(100) <= percent;
    }

    /**
     * Get a random string with the given length.
     *
     * @param len the length
     * @return the string
     */
    public String randomString(int len) {
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < len; i++) {
            String from = (i % 2 == 0) ? "bdfghklmnpqrst" : "aeiou";
            buff.append(from.charAt(getInt(from.length())));
        }
        return buff.toString();
    }

    /**
     * Get a random integer. In 10% of the cases, Integer.MAX_VALUE is returned,
     * in 10% of the cases Integer.MIN_VALUE. Also in the 10% of the cases, a
     * random value between those extremes is returned. In 20% of the cases,
     * this method returns 0. In 10% of the cases, a gaussian value in the range
     * -200 and +1800 is returned. In all other cases, a gaussian value in the
     * range -5 and +20 is returned.
     *
     * @return the random value
     */
    public int getRandomInt() {
        switch (random.nextInt(10)) {
        case 0:
            return Integer.MAX_VALUE;
        case 1:
            return Integer.MIN_VALUE;
        case 2:
            return random.nextInt();
        case 3:
        case 4:
            return 0;
        case 5:
            return (int) (random.nextGaussian() * 2000) - 200;
        default:
            return (int) (random.nextGaussian() * 20) - 5;
        }
    }

    /**
     * Get a random long. The algorithm is similar to a random int.
     *
     * @return the random value
     */
    public long getRandomLong() {
        switch (random.nextInt(10)) {
        case 0:
            return Long.MAX_VALUE;
        case 1:
            return Long.MIN_VALUE;
        case 2:
            return random.nextLong();
        case 3:
        case 4:
            return 0;
        case 5:
            return (int) (random.nextGaussian() * 20000) - 2000;
        default:
            return (int) (random.nextGaussian() * 200) - 50;
        }
    }

    /**
     * Get a random double. The algorithm is similar to a random int.
     *
     * @return the random value
     */
    public double getRandomDouble() {
        switch (random.nextInt(10)) {
        case 0:
            return Double.MIN_VALUE;
        case 1:
            return Double.MAX_VALUE;
        case 2:
            return Float.MIN_VALUE;
        case 3:
            return Float.MAX_VALUE;
        case 4:
            return random.nextDouble();
        case 5:
        case 6:
            return 0;
        case 7:
            return random.nextGaussian() * 20000. - 2000.;
        default:
            return random.nextGaussian() * 200. - 50.;
        }
    }

    /**
     * Get a random boolean.
     *
     * @return the random value
     */
    public boolean nextBoolean() {
        return random.nextBoolean();
    }

    /**
     * Get a random integer array. In 10% of the cases, null is returned.
     *
     * @return the array
     */
    public int[] getIntArray() {
        switch (random.nextInt(10)) {
        case 0:
            return null;
        default:
            int len = getInt(100);
            int[] list = new int[len];
            for (int i = 0; i < len; i++) {
                list[i] = getRandomInt();
            }
            return list;
        }
    }

    /**
     * Get a random byte array. In 10% of the cases, null is returned.
     *
     * @return the array
     */
    public byte[] getByteArray() {
        switch (random.nextInt(10)) {
        case 0:
            return null;
        default:
            int len = getInt(100);
            byte[] list = new byte[len];
            random.nextBytes(list);
            return list;
        }
    }

    /**
     * Get a random time value. In 10% of the cases, null is returned.
     *
     * @return the value
     */
    public Time randomTime() {
        if (random.nextInt(10) == 0) {
            return null;
        }
        StringBuilder buff = new StringBuilder();
        buff.append(getInt(24));
        buff.append(':');
        buff.append(getInt(24));
        buff.append(':');
        buff.append(getInt(24));
        return Time.valueOf(buff.toString());

    }

    /**
     * Get a random timestamp value. In 10% of the cases, null is returned.
     *
     * @return the value
     */
    public Timestamp randomTimestamp() {
        if (random.nextInt(10) == 0) {
            return null;
        }
        StringBuilder buff = new StringBuilder();
        buff.append(getInt(10) + 2000);
        buff.append('-');
        int month = getInt(12) + 1;
        if (month < 10) {
            buff.append('0');
        }
        buff.append(month);
        buff.append('-');
        int day = getInt(28) + 1;
        if (day < 10) {
            buff.append('0');
        }
        buff.append(day);
        buff.append(' ');
        int hour = getInt(24);
        if (hour < 10) {
            buff.append('0');
        }
        buff.append(hour);
        buff.append(':');
        int minute = getInt(60);
        if (minute < 10) {
            buff.append('0');
        }
        buff.append(minute);
        buff.append(':');
        int second = getInt(60);
        if (second < 10) {
            buff.append('0');
        }
        buff.append(second);
        // TODO test timestamp nanos
        return Timestamp.valueOf(buff.toString());
    }

    /**
     * Get a random date value. In 10% of the cases, null is returned.
     *
     * @return the value
     */
    public Date randomDate() {
        if (random.nextInt(10) == 0) {
            return null;
        }
        StringBuilder buff = new StringBuilder();
        buff.append(getInt(10) + 2000);
        buff.append('-');
        int month = getInt(12) + 1;
        if (month < 10) {
            buff.append('0');
        }
        buff.append(month);
        buff.append('-');
        int day = getInt(28) + 1;
        if (day < 10) {
            buff.append('0');
        }
        buff.append(day);
        return Date.valueOf(buff.toString());
    }

    /**
     * Randomly modify a SQL statement.
     *
     * @param sql the original SQL statement
     * @return the modified statement
     */
    public String modify(String sql) {
        int len = getLog(10);
        for (int i = 0; i < len; i++) {
            int pos = getInt(sql.length());
            if (getBoolean(50)) {
                String badChars = "abcABCDEF\u00ef\u00f6\u00fcC1230=<>+\"\\*%&/()=?$_-.:,;{}[]";
                // auml, ouml, uuml
                char bad = badChars.charAt(getInt(badChars.length()));
                sql = sql.substring(0, pos) + bad + sql.substring(pos);
            } else {
                if (pos >= sql.length()) {
                    sql = sql.substring(0, pos);
                } else {
                    sql = sql.substring(0, pos) + sql.substring(pos + 1);
                }
            }
        }
        return sql;
    }

    /**
     * Set the seed value.
     *
     * @param seed the new seed value
     */
    public void setSeed(int seed) {
        random.setSeed(seed);
    }

}
