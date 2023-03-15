/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkore.ignite.lucene.util;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class FBUtilities
{
    private static final Logger logger = LoggerFactory.getLogger(FBUtilities.class);

    private static final ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());

    public static final BigInteger TWO = new BigInteger("2");
    private static final String DEFAULT_TRIGGER_DIR = "triggers";

    private static final String OPERATING_SYSTEM = System.getProperty("os.name").toLowerCase();
    public static final boolean isWindows = OPERATING_SYSTEM.contains("windows");

    private static volatile InetAddress localInetAddress;
    private static volatile InetAddress broadcastInetAddress;
    private static volatile InetAddress broadcastRpcAddress;

    public static int getAvailableProcessors()
    {
        if (System.getProperty("cassandra.available_processors") != null)
            return Integer.parseInt(System.getProperty("cassandra.available_processors"));
        else
            return Runtime.getRuntime().availableProcessors();
    }

    private static final ThreadLocal<MessageDigest> localMD5Digest = new ThreadLocal<MessageDigest>()
    {
        @Override
        protected MessageDigest initialValue()
        {
            return newMessageDigest("MD5");
        }

        @Override
        public MessageDigest get()
        {
            MessageDigest digest = super.get();
            digest.reset();
            return digest;
        }
    };

    public static final int MAX_UNSIGNED_SHORT = 0xFFFF;

    public static MessageDigest threadLocalMD5Digest()
    {
        return localMD5Digest.get();
    }

    public static MessageDigest newMessageDigest(String algorithm)
    {
        try
        {
            return MessageDigest.getInstance(algorithm);
        }
        catch (NoSuchAlgorithmException nsae)
        {
            throw new RuntimeException("the requested digest algorithm (" + algorithm + ") is not available", nsae);
        }
    }


    public static Collection<InetAddress> getAllLocalAddresses()
    {
        Set<InetAddress> localAddresses = new HashSet<InetAddress>();
        try
        {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            if (nets != null)
            {
                while (nets.hasMoreElements())
                    localAddresses.addAll(Collections.list(nets.nextElement().getInetAddresses()));
            }
        }
        catch (SocketException e)
        {
            throw new AssertionError(e);
        }
        return localAddresses;
    }

    public static String getNetworkInterface(InetAddress localAddress)
    {
        try
        {
            for(NetworkInterface ifc : Collections.list(NetworkInterface.getNetworkInterfaces()))
            {
                if(ifc.isUp())
                {
                    for(InetAddress addr : Collections.list(ifc.getInetAddresses()))
                    {
                        if (addr.equals(localAddress))
                            return ifc.getDisplayName();
                    }
                }
            }
        }
        catch (SocketException e) {}
        return null;
    }

    /**
     * Given two bit arrays represented as BigIntegers, containing the given
     * number of significant bits, calculate a midpoint.
     *
     * @param left The left point.
     * @param right The right point.
     * @param sigbits The number of bits in the points that are significant.
     * @return A midpoint that will compare bitwise halfway between the params, and
     * a boolean representing whether a non-zero lsbit remainder was generated.
     */
    public static Pair<BigInteger,Boolean> midpoint(BigInteger left, BigInteger right, int sigbits)
    {
        BigInteger midpoint;
        boolean remainder;
        if (left.compareTo(right) < 0)
        {
            BigInteger sum = left.add(right);
            remainder = sum.testBit(0);
            midpoint = sum.shiftRight(1);
        }
        else
        {
            BigInteger max = TWO.pow(sigbits);
            // wrapping case
            BigInteger distance = max.add(right).subtract(left);
            remainder = distance.testBit(0);
            midpoint = distance.shiftRight(1).add(left).mod(max);
        }
        return Pair.create(midpoint, remainder);
    }

    public static int compareUnsigned(byte[] bytes1, byte[] bytes2, int offset1, int offset2, int len1, int len2)
    {
        return FastByteOperations.compareUnsigned(bytes1, offset1, len1, bytes2, offset2, len2);
    }

    public static int compareUnsigned(byte[] bytes1, byte[] bytes2)
    {
        return compareUnsigned(bytes1, bytes2, 0, 0, bytes1.length, bytes2.length);
    }

    /**
     * @return The bitwise XOR of the inputs. The output will be the same length as the
     * longer input, but if either input is null, the output will be null.
     */
    public static byte[] xor(byte[] left, byte[] right)
    {
        if (left == null || right == null)
            return null;
        if (left.length > right.length)
        {
            byte[] swap = left;
            left = right;
            right = swap;
        }

        // left.length is now <= right.length
        byte[] out = Arrays.copyOf(right, right.length);
        for (int i = 0; i < left.length; i++)
        {
            out[i] = (byte)((left[i] & 0xFF) ^ (right[i] & 0xFF));
        }
        return out;
    }

    public static byte[] hash(ByteBuffer... data)
    {
        MessageDigest messageDigest = localMD5Digest.get();
        for (ByteBuffer block : data)
        {
            if (block.hasArray())
                messageDigest.update(block.array(), block.arrayOffset() + block.position(), block.remaining());
            else
                messageDigest.update(block.duplicate());
        }

        return messageDigest.digest();
    }

    public static BigInteger hashToBigInteger(ByteBuffer data)
    {
        return new BigInteger(hash(data)).abs();
    }

 
    public static File cassandraTriggerDir()
    {
        File triggerDir = null;
        if (System.getProperty("cassandra.triggers_dir") != null)
        {
            triggerDir = new File(System.getProperty("cassandra.triggers_dir"));
        }
        else
        {
            URL confDir = FBUtilities.class.getClassLoader().getResource(DEFAULT_TRIGGER_DIR);
            if (confDir != null)
                triggerDir = new File(confDir.getFile());
        }
        if (triggerDir == null || !triggerDir.exists())
        {
            logger.warn("Trigger directory doesn't exist, please create it and try again.");
            return null;
        }
        return triggerDir;
    }

    public static String getReleaseVersionString()
    {
        try (InputStream in = FBUtilities.class.getClassLoader().getResourceAsStream("org/apache/cassandra/config/version.properties"))
        {
            if (in == null)
            {
                return System.getProperty("cassandra.releaseVersion", "Unknown");
            }
            Properties props = new Properties();
            props.load(in);
            return props.getProperty("CassandraVersion");
        }
        catch (Exception e)
        {
            //JVMStabilityInspector.inspectThrowable(e);
            logger.warn("Unable to load version.properties", e);
            return "debug version";
        }
    }

    public static long timestampMicros()
    {
        // we use microsecond resolution for compatibility with other client libraries, even though
        // we can't actually get microsecond precision.
        return System.currentTimeMillis() * 1000;
    }

    public static int nowInSeconds()
    {
        return (int) (System.currentTimeMillis() / 1000);
    }


    public static <T> T waitOnFuture(Future<T> future)
    {
        try
        {
            return future.get();
        }
        catch (ExecutionException ee)
        {
            throw new RuntimeException(ee);
        }
        catch (InterruptedException ie)
        {
            throw new AssertionError(ie);
        }
    }

  



    public static <T> NavigableSet<T> singleton(T column, Comparator<? super T> comparator)
    {
        NavigableSet<T> s = new TreeSet<T>(comparator);
        s.add(column);
        return s;
    }

    public static <T> NavigableSet<T> emptySortedSet(Comparator<? super T> comparator)
    {
        return new TreeSet<T>(comparator);
    }


    /**
     * Used to get access to protected/private field of the specified class
     * @param klass - name of the class
     * @param fieldName - name of the field
     * @return Field or null on error
     */
    public static Field getProtectedField(Class klass, String fieldName)
    {
        try
        {
            Field field = klass.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field;
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }


    public static Map<String, String> fromJsonMap(String json)
    {
        try
        {
            return jsonMapper.readValue(json, Map.class);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static List<String> fromJsonList(String json)
    {
        try
        {
            return jsonMapper.readValue(json, List.class);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static String json(Object object)
    {
        try
        {
            return jsonMapper.writeValueAsString(object);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static String prettyPrintMemory(long size)
    {
        return prettyPrintMemory(size, false);
    }

    public static String prettyPrintMemory(long size, boolean includeSpace)
    {
        if (size >= 1 << 30)
            return String.format("%.3f%sGiB", size / (double) (1 << 30), includeSpace ? " " : "");
        if (size >= 1 << 20)
            return String.format("%.3f%sMiB", size / (double) (1 << 20), includeSpace ? " " : "");
        return String.format("%.3f%sKiB", size / (double) (1 << 10), includeSpace ? " " : "");
    }

    public static String prettyPrintMemoryPerSecond(long rate)
    {
        if (rate >= 1 << 30)
            return String.format("%.3fGiB/s", rate / (double) (1 << 30));
        if (rate >= 1 << 20)
            return String.format("%.3fMiB/s", rate / (double) (1 << 20));
        return String.format("%.3fKiB/s", rate / (double) (1 << 10));
    }

    public static String prettyPrintMemoryPerSecond(long bytes, long timeInNano)
    {
        // We can't sanely calculate a rate over 0 nanoseconds
        if (timeInNano == 0)
            return "NaN  KiB/s";

        long rate = (long) (((double) bytes / timeInNano) * 1000 * 1000 * 1000);

        return prettyPrintMemoryPerSecond(rate);
    }

    /**
     * Starts and waits for the given @param pb to finish.
     * @throws java.io.IOException on non-zero exit code
     */
    public static void exec(ProcessBuilder pb) throws IOException
    {
        Process p = pb.start();
        try
        {
            int errCode = p.waitFor();
            if (errCode != 0)
            {
            	try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
                     BufferedReader err = new BufferedReader(new InputStreamReader(p.getErrorStream())))
                {
            		String lineSep = System.getProperty("line.separator");
	                StringBuilder sb = new StringBuilder();
	                String str;
	                while ((str = in.readLine()) != null)
	                    sb.append(str).append(lineSep);
	                while ((str = err.readLine()) != null)
	                    sb.append(str).append(lineSep);
	                throw new IOException("Exception while executing the command: "+ StringUtils.join(pb.command(), " ") +
	                                      ", command error Code: " + errCode +
	                                      ", command output: "+ sb.toString());
                }
            }
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    public static void updateChecksumInt(Checksum checksum, int v)
    {
        checksum.update((v >>> 24) & 0xFF);
        checksum.update((v >>> 16) & 0xFF);
        checksum.update((v >>> 8) & 0xFF);
        checksum.update((v >>> 0) & 0xFF);
    }

    /**
      * Updates checksum with the provided ByteBuffer at the given offset + length.
      * Resets position and limit back to their original values on return.
      * This method is *NOT* thread-safe.
      */
    public static void updateChecksum(CRC32 checksum, ByteBuffer buffer, int offset, int length)
    {
        int position = buffer.position();
        int limit = buffer.limit();

        buffer.position(offset).limit(offset + length);
        checksum.update(buffer);

        buffer.position(position).limit(limit);
    }

    /**
     * Updates checksum with the provided ByteBuffer.
     * Resets position back to its original values on return.
     * This method is *NOT* thread-safe.
     */
    public static void updateChecksum(CRC32 checksum, ByteBuffer buffer)
    {
        int position = buffer.position();
        checksum.update(buffer);
        buffer.position(position);
    }

    public static long abs(long index)
    {
        long negbit = index >> 63;
        return (index ^ negbit) - negbit;
    }




    public static long copy(InputStream from, OutputStream to, long limit) throws IOException
    {
        byte[] buffer = new byte[64]; // 64 byte buffer
        long copied = 0;
        int toCopy = buffer.length;
        while (true)
        {
            if (limit < buffer.length + copied)
                toCopy = (int) (limit - copied);
            int sofar = from.read(buffer, 0, toCopy);
            if (sofar == -1)
                break;
            to.write(buffer, 0, sofar);
            copied += sofar;
            if (limit == copied)
                break;
        }
        return copied;
    }


    public static void updateWithShort(MessageDigest digest, int val)
    {
        digest.update((byte) ((val >> 8) & 0xFF));
        digest.update((byte) (val & 0xFF));
    }

    public static void updateWithByte(MessageDigest digest, int val)
    {
        digest.update((byte) (val & 0xFF));
    }

    public static void updateWithInt(MessageDigest digest, int val)
    {
        digest.update((byte) ((val >>> 24) & 0xFF));
        digest.update((byte) ((val >>> 16) & 0xFF));
        digest.update((byte) ((val >>>  8) & 0xFF));
        digest.update((byte) ((val >>> 0) & 0xFF));
    }

    public static void updateWithLong(MessageDigest digest, long val)
    {
        digest.update((byte) ((val >>> 56) & 0xFF));
        digest.update((byte) ((val >>> 48) & 0xFF));
        digest.update((byte) ((val >>> 40) & 0xFF));
        digest.update((byte) ((val >>> 32) & 0xFF));
        digest.update((byte) ((val >>> 24) & 0xFF));
        digest.update((byte) ((val >>> 16) & 0xFF));
        digest.update((byte) ((val >>>  8) & 0xFF));
        digest.update((byte)  ((val >>> 0) & 0xFF));
    }

    public static void updateWithBoolean(MessageDigest digest, boolean val)
    {
        updateWithByte(digest, val ? 0 : 1);
    }

    public static void closeAll(List<? extends AutoCloseable> l) throws Exception
    {
        Exception toThrow = null;
        for (AutoCloseable c : l)
        {
            try
            {
                c.close();
            }
            catch (Exception e)
            {
                if (toThrow == null)
                    toThrow = e;
                else
                    toThrow.addSuppressed(e);
            }
        }
        if (toThrow != null)
            throw toThrow;
    }

    public static byte[] toWriteUTFBytes(String s)
    {
        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeUTF(s);
            dos.flush();
            return baos.toByteArray();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

	public static void sleepQuietly(long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static long align(long val, int boundary)
    {
        return (val + boundary) & ~(boundary - 1);
    }

    @VisibleForTesting
    protected static void reset()
    {
        localInetAddress = null;
        broadcastInetAddress = null;
        broadcastRpcAddress = null;
    }
}