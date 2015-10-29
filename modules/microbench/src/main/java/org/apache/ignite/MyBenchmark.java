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

package org.apache.ignite;

import org.apache.ignite.internal.portable.GridPortableMarshaller;
import org.apache.ignite.internal.portable.PortableContext;
import org.apache.ignite.internal.portable.PortableMetaDataHandler;
import org.apache.ignite.internal.portable.PortableObjectImpl;
import org.apache.ignite.internal.portable.streams.PortableSimpleMemoryAllocator;
import org.apache.ignite.internal.util.IgniteUtils;

import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.portable.PortableException;
import org.apache.ignite.portable.PortableMarshalAware;
import org.apache.ignite.portable.PortableMetadata;
import org.apache.ignite.portable.PortableObject;
import org.apache.ignite.portable.PortableReader;
import org.apache.ignite.portable.PortableWriter;
import org.apache.ignite.util.MarshallerContextMicrobenchImpl;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 100000)
@Measurement(iterations = 100000)
@Fork(1)
public class MyBenchmark {

    private static PortableMarshaller marsh;

    private static OptimizedMarshaller optMarsh;

    private static byte[] marshAddrBytes;

    private static PortableObject marshPortable;

    @Setup
    public static void setup() throws Exception {
        PortableMetaDataHandler metaHnd = new PortableMetaDataHandler() {
            @Override public void addMeta(int typeId, PortableMetadata meta) { }
            @Override public PortableMetadata metadata(int typeId) {
                return null;
            }
        };

        marsh = new PortableMarshaller();
        PortableContext ctx = new PortableContext(metaHnd, null);
        marsh.setContext(new MarshallerContextMicrobenchImpl(null));
        IgniteUtils.invoke(PortableMarshaller.class, marsh, "setPortableContext", ctx);

        optMarsh = new OptimizedMarshaller();
        optMarsh.setContext(new MarshallerContextMicrobenchImpl(null));

        marshAddrBytes = marsh.marshal(new ManyFields());

        marshPortable = new PortableObjectImpl(U.<GridPortableMarshaller>field(marsh, "impl").context(),
            marshAddrBytes, 0);
    }

//    @Benchmark
//    public byte[] testAddressWrite() throws Exception {
//        return marsh.marshal(new Address());
//    }

    @Benchmark
    public Object testRead() throws Exception {
        return marsh.unmarshal(marshAddrBytes, null);
    }

//    @Benchmark
//    public Object testFieldRead() throws Exception {
//        return marshPortable.field("street");
//    }

    private static final Address addr = new Address();

    public static void main(String[] args) throws Exception {
//        setup();
//        while (true) {
//            marsh.unmarshal(marshAddrBytes, null);
////            String val = marshPortable.field("street");
////
////            System.out.println(val);
//        }

        Options opts = new OptionsBuilder().include(MyBenchmark.class.getSimpleName()).build();
        new Runner(opts).run();
    }

    enum Sex { MALE, FEMALE }

    static class Customer {
        public int customerId;
        public String name;
        public Date birthday;
        public Sex gender;
        public String emailAddress;
        //long[] longArray;
    }

    public static Customer newCustomer(int i) {
        Customer customer = new Customer();
        customer.customerId = i;
        customer.name = "Name" + i;
        customer.gender = Sex.FEMALE;
        customer.birthday = new Date(System.currentTimeMillis() -
            ThreadLocalRandom.current().nextInt(100 * 365 * 24 * 60 * 60 * 1000));
        customer.emailAddress = "email." + customer.name + "@gmail.com";
        //customer.longArray = new long[100];
        return customer;
    }

    static class ManyFields {
        public int field1 = 1;
        public int field2 = 2;
        public int field3 = 3;
        public int field4 = 4;
        public int field5 = 5;

        public int field6 = 6;
        public int field7 = 7;
        public int field8 = 8;
        public int field9 = 9;
        public int field10 = 10;

    }

    static class Address implements PortableMarshalAware, Externalizable {
        public int streetNum = 49;
        public int flatNum = 30;
        public String city = "y9ftLwibL9xXNUy";
        public String street = "TtzN26NtxVqueAc6nVUY";

        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(streetNum);
            out.writeInt(flatNum);
            out.writeObject(city);
            out.writeObject(street);
        }

        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            streetNum = in.readInt();
            flatNum = in.readInt();
            city = (String)in.readObject();
            street = (String)in.readObject();
        }

        /** {@inheritDoc} */
        @Override public void writePortable(PortableWriter writer) throws PortableException {
            writer.writeInt("streetNum", streetNum);
            writer.writeInt("flatNum", flatNum);
            writer.writeString("city", city);
            writer.writeString("street", street);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(PortableReader reader) throws PortableException {
            streetNum = reader.readInt("streetNum");
            flatNum = reader.readInt("flatNum");
            city = reader.readString("city");
            street = reader.readString("street");
        }
    }

}
