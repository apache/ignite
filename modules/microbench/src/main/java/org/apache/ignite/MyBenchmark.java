/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.ignite;

import org.apache.ignite.internal.portable.PortableContext;
import org.apache.ignite.internal.portable.PortableMetaDataHandler;
import org.apache.ignite.internal.portable.streams.PortableSimpleMemoryAllocator;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.portable.PortableException;
import org.apache.ignite.portable.PortableMarshalAware;
import org.apache.ignite.portable.PortableMetadata;
import org.apache.ignite.portable.PortableReader;
import org.apache.ignite.portable.PortableWriter;
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

    private static byte[] optMarshAddrBytes;

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
        marsh.setContext(new MarshallerContextTestImpl(null));
        IgniteUtils.invoke(PortableMarshaller.class, marsh, "setPortableContext", ctx);

        optMarsh = new OptimizedMarshaller();
        optMarsh.setContext(new MarshallerContextTestImpl(null));

        marshAddrBytes = marsh.marshal(new Address());
        optMarshAddrBytes = optMarsh.marshal(new Address());

        byte[] data = marsh.marshal(newCustomer(1));

        System.out.println(data.length);
    }

    @Benchmark
    public byte[] testAddressWrite() throws Exception {
        return marsh.marshal(new Address());
    }

//    @Benchmark
//    public Address testAddressRead() throws Exception {
//        return marsh.unmarshal(marshAddrBytes, null);
//    }

    private static final Address addr = new Address();

    public static void main(String[] args) throws Exception {
        setup();
        while (true)
            marsh.unmarshal(marshAddrBytes, null);

//        Options opts = new OptionsBuilder().include(MyBenchmark.class.getSimpleName()).build();
//        new Runner(opts).run();
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
