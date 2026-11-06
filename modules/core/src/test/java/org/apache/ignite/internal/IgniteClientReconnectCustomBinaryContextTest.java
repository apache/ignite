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

package org.apache.ignite.internal;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryIoSession;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.util.ReflectionUtils;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.time.temporal.ChronoField.YEAR;

/**
 * Test binary configuration restored after client reconnect.
 */
public class IgniteClientReconnectCustomBinaryContextTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "Test";

    /** */
    private final AtomicBoolean block = new AtomicBoolean(false);

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setBinaryConfiguration(
            new BinaryConfiguration()
                .setTypeConfigurations(
                    Arrays.asList(
                        new BinaryTypeConfiguration(DateKey.class.getName())
                            .setIdMapper(new BinaryBasicIdMapper(true))
                            .setNameMapper(new BinaryBasicNameMapper(true)),
                        new BinaryTypeConfiguration("java.time.Ser")
                            .setSerializer(new LocalDateBinarySerializer())
                    )
                )
        );

        if (igniteInstanceName.equals("client")) {
            TcpDiscoverySpi discoverySpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();
            TcpDiscoveryIpFinder ipFinder = discoverySpi.getIpFinder();

            BlockingDiscoverySpi clientDiscoverySpi = new BlockingDiscoverySpi(block);
            clientDiscoverySpi.setIpFinder(ipFinder);

            cfg.setDiscoverySpi(clientDiscoverySpi);

            cfg.setFailureDetectionTimeout(30000);
            cfg.setClientFailureDetectionTimeout(30000);
        }

        return cfg;
    }

    /** */
    @Test
    public void testReconnect() throws Exception {
        IgniteEx srv1 = startGrid(0);
        IgniteEx client = startClientGrid("client");

        srv1.getOrCreateCache(getCacheConfiguration());

        DateKey storedKey = new DateKey(LocalDate.now());
        String val = "Hello";

        // Put value to cache and ensure it is visible from client and server.
        srv1.cache(CACHE_NAME).put(storedKey, val);
        assertEquals(val, srv1.cache(CACHE_NAME).get(storedKey));
        assertEquals(val, client.cache(CACHE_NAME).get(storedKey));

        // Wait for client disconnected.
        block.set(true);
        assertTrue(GridTestUtils.waitForCondition(() -> {
            try {
                client.cache(CACHE_NAME).get(storedKey);
            }
            catch (IgniteClientDisconnectedException ex) {
                return true;
            }
            return false;
        }, getTestTimeout()));

        assertTrue(client.context().clientDisconnected());
        block.set(false);

        // Wait for client to reconnect.
        try {
            client.cache(CACHE_NAME);
        }
        catch (IgniteClientDisconnectedException ex) {
            ex.reconnectFuture().get();
        }

        // Ensure value is still there.
        assertEquals(val, client.cache(CACHE_NAME).get(storedKey));

        client.close();

        Ignite client1 = startClientGrid("client");
        assertEquals(val, client1.cache(CACHE_NAME).get(storedKey));
    }

    /** */
    private static CacheConfiguration<DateKey, String> getCacheConfiguration() {
        CacheConfiguration<DateKey, String> ccfg = new CacheConfiguration<>();

        ccfg.setName(CACHE_NAME);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setTypes(DateKey.class, String.class);
        ccfg.setIndexedTypes(DateKey.class, String.class);
        ccfg.setCacheMode(CacheMode.PARTITIONED);

        return ccfg;
    }

    /** */
    public static class DateKey {
        /** */
        LocalDate date;

        /** */
        public DateKey(LocalDate date) {
            this.date = date;
        }
    }

    /** */
    public static class LocalDateBinarySerializer implements BinarySerializer {
        /** */
        private static final byte DURATION_TYPE = 1;

        /** */
        private static final byte LOCAL_DATE_TYPE = 3;

        /** */
        private static final byte LOCAL_DATE_TIME_TYPE = 5;

        /** */
        private final Field objectField;

        /** */
        private final Field typeField;

        /** */
        public LocalDateBinarySerializer() {
            try {
                Class<?> cls = Class.forName("java.time.Ser");
                objectField = ReflectionUtils.findField(cls, "object");
                objectField.setAccessible(true);

                typeField = ReflectionUtils.findField(cls, "type");
                typeField.setAccessible(true);
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        /** */
        @Override public void writeBinary(Object obj, BinaryWriter writer) throws BinaryObjectException {
            Byte type = (Byte)ReflectionUtils.getField(typeField, obj);
            writer.writeByte("type", type);
            if (type == DURATION_TYPE) {
                Duration duration = (Duration)ReflectionUtils.getField(objectField, obj);
                writer.writeString("duration", duration != null ? duration.toString() : null);
            }
            else {
                Temporal date = (Temporal)ReflectionUtils.getField(objectField, obj);
                writer.writeInt("year", date.get(YEAR));
                writer.writeInt("month", date.get(MONTH_OF_YEAR));
                writer.writeInt("day", date.get(DAY_OF_MONTH));
                if (type == LOCAL_DATE_TIME_TYPE) {
                    writer.writeInt("hour", date.get(HOUR_OF_DAY));
                    writer.writeInt("minute", date.get(MINUTE_OF_HOUR));
                    writer.writeInt("second", date.get(SECOND_OF_MINUTE));
                    writer.writeInt("nano", date.get(NANO_OF_SECOND));
                }
            }
        }

        /** */
        @Override public void readBinary(Object obj, BinaryReader reader) throws BinaryObjectException {
            byte type = reader.readByte("type");

            if (type == DURATION_TYPE) {
                String str = reader.readString("duration");
                Duration duration = str != null ? Duration.parse(str) : null;
                ReflectionUtils.setField(objectField, obj, duration);

            }
            else {
                int year = reader.readInt("year");
                int month = reader.readInt("month");
                int day = reader.readInt("day");

                LocalDate date = LocalDate.of(year, month, day);
                if (type == LOCAL_DATE_TYPE) {
                    ReflectionUtils.setField(objectField, obj, date);
                }
                else {
                    LocalTime time = LocalTime.of(
                        reader.readInt("hour"),
                        reader.readInt("minute"),
                        reader.readInt("second"),
                        reader.readInt("nano")
                    );
                    LocalDateTime dateTime = LocalDateTime.of(date, time);
                    ReflectionUtils.setField(objectField, obj, dateTime);
                }
            }
        }
    }

    /**
     * Discovery SPI that stops processing messages when {@code block} is set to {@code true}.
     */
    private static class BlockingDiscoverySpi extends TcpDiscoverySpi {
        /**  */
        private final AtomicBoolean block;

        /**  */
        public BlockingDiscoverySpi(AtomicBoolean block) {
            this.block = block;
        }

        /** */
        @Override protected void writeMessage(
            TcpDiscoveryIoSession ses,
            TcpDiscoveryAbstractMessage msg,
            long timeout
        ) throws IOException, IgniteCheckedException {
            if (!block.get())
                super.writeMessage(ses, msg, timeout);
        }
    }
}

