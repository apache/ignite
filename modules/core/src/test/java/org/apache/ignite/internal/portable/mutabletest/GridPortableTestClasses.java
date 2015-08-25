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

package org.apache.ignite.internal.portable.mutabletest;

import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.portable.*;

import com.google.common.base.*;

import java.io.*;
import java.util.*;

/**
 *
 */
@SuppressWarnings({"PublicInnerClass", "PublicField"})
public class GridPortableTestClasses {
    /**
     *
     */
    public static class TestObjectContainer {
        /** */
        public Object foo;

        /**
         *
         */
        public TestObjectContainer() {
            // No-op.
        }

        /**
         * @param foo Object.
         */
        public TestObjectContainer(Object foo) {
            this.foo = foo;
        }
    }

    /**
     *
     */
    public static class TestObjectOuter {
        /** */
        public TestObjectInner inner;

        /** */
        public String foo;

        /**
         *
         */
        public TestObjectOuter() {

        }

        /**
         * @param inner Inner object.
         */
        public TestObjectOuter(TestObjectInner inner) {
            this.inner = inner;
        }
    }

    /** */
    public static class TestObjectInner {
        /** */
        public Object foo;

        /** */
        public TestObjectOuter outer;
    }

    /** */
    public static class TestObjectArrayList {
        /** */
        public List<String> list = new ArrayList<>();
    }

    /**
     *
     */
    public static class TestObjectPlainPortable {
        /** */
        public PortableObject plainPortable;

        /**
         *
         */
        public TestObjectPlainPortable() {
            // No-op.
        }

        /**
         * @param plainPortable Object.
         */
        public TestObjectPlainPortable(PortableObject plainPortable) {
            this.plainPortable = plainPortable;
        }
    }

    /**
     *
     */
    public static class TestObjectAllTypes implements Serializable {
        /** */
        public Byte b_;

        /** */
        public Short s_;

        /** */
        public Integer i_;

        /** */
        public Long l_;

        /** */
        public Float f_;

        /** */
        public Double d_;

        /** */
        public Character c_;

        /** */
        public Boolean z_;

        /** */
        public byte b;

        /** */
        public short s;

        /** */
        public int i;

        /** */
        public long l;

        /** */
        public float f;

        /** */
        public double d;

        /** */
        public char c;

        /** */
        public boolean z;

        /** */
        public String str;

        /** */
        public UUID uuid;

        /** */
        public Date date;


        /** */
        public byte[] bArr;

        /** */
        public short[] sArr;

        /** */
        public int[] iArr;

        /** */
        public long[] lArr;

        /** */
        public float[] fArr;

        /** */
        public double[] dArr;

        /** */
        public char[] cArr;

        /** */
        public boolean[] zArr;

        /** */
        public String[] strArr;

        /** */
        public UUID[] uuidArr;

        /** */
        public TestObjectEnum anEnum;

        /** */
        public TestObjectEnum[] enumArr;

        /** */
        public Map.Entry entry;

        //public Date[] dateArr; // todo test date array.

        /**
         * @return Array.
         */
        private byte[] serialize() {
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();

            try {
                ObjectOutput out = new ObjectOutputStream(byteOut);

                out.writeObject(this);

                out.close();
            }
            catch (IOException e) {
                Throwables.propagate(e);
            }

            return byteOut.toByteArray();
        }

        /**
         *
         */
        public void setDefaultData() {
            b_ = 11;
            s_ = 22;
            i_ = 33;
            l_ = 44L;
            f_ = 55f;
            d_ = 66d;
            c_ = 'e';
            z_ = true;

            b = 1;
            s = 2;
            i = 3;
            l = 4;
            f = 5;
            d = 6;
            c = 7;
            z = true;

            str = "abc";
            uuid = new UUID(1, 1);
            date = new Date(1000000);

            bArr = new byte[]{1, 2, 3};
            sArr = new short[]{1, 2, 3};
            iArr = new int[]{1, 2, 3};
            lArr = new long[]{1, 2, 3};
            fArr = new float[]{1, 2, 3};
            dArr = new double[]{1, 2, 3};
            cArr = new char[]{1, 2, 3};
            zArr = new boolean[]{true, false};

            strArr = new String[]{"abc", "ab", "a"};
            uuidArr = new UUID[]{new UUID(1, 1), new UUID(2, 2)};

            anEnum = TestObjectEnum.A;

            enumArr = new TestObjectEnum[]{TestObjectEnum.B};

            entry = new GridMapEntry<>(1, "a");
        }
    }

    /**
     *
     */
    public enum TestObjectEnum {
        A, B, C
    }

    /**
     *
     */
    public static class Address {
        /** City. */
        public String city;

        /** Street. */
        public String street;

        /** Street number. */
        public int streetNumber;

        /** Flat number. */
        public int flatNumber;

        /**
         * Default constructor.
         */
        public Address() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param city City.
         * @param street Street.
         * @param streetNumber Street number.
         * @param flatNumber Flat number.
         */
        public Address(String city, String street, int streetNumber, int flatNumber) {
            this.city = city;
            this.street = street;
            this.streetNumber = streetNumber;
            this.flatNumber = flatNumber;
        }
    }

    /**
     *
     */
    public static class Company {
        /** ID. */
        public int id;

        /** Name. */
        public String name;

        /** Size. */
        public int size;

        /** Address. */
        public Address address;

        /** Occupation. */
        public String occupation;

        /**
         * Default constructor.
         */
        public Company() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param id ID.
         * @param name Name.
         * @param size Size.
         * @param address Address.
         * @param occupation Occupation.
         */
        public Company(int id, String name, int size, Address address, String occupation) {
            this.id = id;
            this.name = name;
            this.size = size;
            this.address = address;
            this.occupation = occupation;
        }
    }

    /**
     *
     */
    public static class AddressBook {
        /** */
        private Map<String, List<Company>> companyByStreet = new TreeMap<>();

        /**
         *
         * @param street Street.
         * @return Company.
         */
        public List<Company> findCompany(String street) {
            return companyByStreet.get(street);
        }

        /**
         *
         * @param company Company.
         */
        public void addCompany(Company company) {
            List<Company> list = companyByStreet.get(company.address.street);

            if (list == null) {
                list = new ArrayList<>();

                companyByStreet.put(company.address.street, list);
            }

            list.add(company);
        }

        /**
         *
         * @return map
         */
        public Map<String, List<Company>> getCompanyByStreet() {
            return companyByStreet;
        }

        /**
         *
         * @param companyByStreet map
         */
        public void setCompanyByStreet(Map<String, List<Company>> companyByStreet) {
            this.companyByStreet = companyByStreet;
        }
    }

}
