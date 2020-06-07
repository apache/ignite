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

package org.apache.ignite.internal.binary;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;

public class MarshallerTestObjects {

    static class VerySimpleObject {
        int foo = 1;
        int ba1 = 1;
        String baz1 = "abc";
        String baz2 = null;
        String baz3 = null;
        String baz4 = null;
        String baz5 = "abc";

        public VerySimpleObject() {
        }

        public VerySimpleObject(int foo, int ba1, String baz1, String baz2, String baz3, String baz4, String baz5) {
            this.foo = foo;
            this.ba1 = ba1;
            this.baz1 = baz1;
            this.baz2 = baz2;
            this.baz3 = baz3;
            this.baz4 = baz4;
            this.baz5 = baz5;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            VerySimpleObject that = (VerySimpleObject) o;
            return foo == that.foo &&
                    ba1 == that.ba1 &&
                    Objects.equals(baz1, that.baz1) &&
                    Objects.equals(baz2, that.baz2) &&
                    Objects.equals(baz3, that.baz3) &&
                    Objects.equals(baz4, that.baz4) &&
                    Objects.equals(baz5, that.baz5);
        }

        @Override
        public int hashCode() {
            return Objects.hash(foo, ba1, baz1, baz2, baz3, baz4, baz5);
        }

        @Override
        public String toString() {
            return "VerySimpleObject{" +
                    "foo=" + foo +
                    ", ba1=" + ba1 +
                    ", baz1='" + baz1 + '\'' +
                    ", baz2='" + baz2 + '\'' +
                    ", baz3='" + baz3 + '\'' +
                    ", baz4='" + baz4 + '\'' +
                    ", baz5='" + baz5 + '\'' +
                    '}';
        }
    }

    static class StartingWithNull {
        String baz2 = null;
        int foo = 1;
        int ba1 = 1;
        String baz1 = "abc";
        String baz3 = null;
        String baz4 = null;
        String baz5 = "abc";

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StartingWithNull that = (StartingWithNull) o;
            return foo == that.foo &&
                    ba1 == that.ba1 &&
                    Objects.equals(baz2, that.baz2) &&
                    Objects.equals(baz1, that.baz1) &&
                    Objects.equals(baz3, that.baz3) &&
                    Objects.equals(baz4, that.baz4) &&
                    Objects.equals(baz5, that.baz5);
        }

        @Override
        public int hashCode() {
            return Objects.hash(baz2, foo, ba1, baz1, baz3, baz4, baz5);
        }

        @Override
        public String toString() {
            return "StartingWithNull{" +
                    "baz2='" + baz2 + '\'' +
                    ", foo=" + foo +
                    ", ba1=" + ba1 +
                    ", baz1='" + baz1 + '\'' +
                    ", baz3='" + baz3 + '\'' +
                    ", baz4='" + baz4 + '\'' +
                    ", baz5='" + baz5 + '\'' +
                    '}';
        }
    }

    static class NestedComplexObject {
        int foo = 1;
        int ba1 = 1;
        String baz1 = "abc";
        String baz2 = null;
        String baz3 = null;
        VerySimpleObject simpleObject = new VerySimpleObject();
        String baz4 = null;
        String baz5 = "abc";

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NestedComplexObject that = (NestedComplexObject) o;
            return foo == that.foo &&
                    ba1 == that.ba1 &&
                    Objects.equals(baz1, that.baz1) &&
                    Objects.equals(baz2, that.baz2) &&
                    Objects.equals(baz3, that.baz3) &&
                    Objects.equals(simpleObject, that.simpleObject) &&
                    Objects.equals(baz4, that.baz4) &&
                    Objects.equals(baz5, that.baz5);
        }

        @Override
        public int hashCode() {
            return Objects.hash(foo, ba1, baz1, baz2, baz3, simpleObject, baz4, baz5);
        }

        @Override
        public String toString() {
            return "NestedComplexObject{" +
                    "foo=" + foo +
                    ", ba1=" + ba1 +
                    ", baz1='" + baz1 + '\'' +
                    ", baz2='" + baz2 + '\'' +
                    ", baz3='" + baz3 + '\'' +
                    ", simpleObject=" + simpleObject +
                    ", baz4='" + baz4 + '\'' +
                    ", baz5='" + baz5 + '\'' +
                    '}';
        }
    }

    static class ComplexObject {
        int foo = 1;
        int ba1 = 1;
        String baz1 = "abc";
        String baz2 = null;
        String baz3 = null;
        List<Integer> fooList = new ArrayList<Integer>(Arrays.asList(new Integer[]{1, null, 3}));
        String baz4 = null;
        String baz5 = "abc";

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ComplexObject that = (ComplexObject) o;
            return foo == that.foo &&
                    ba1 == that.ba1 &&
                    Objects.equals(baz1, that.baz1) &&
                    Objects.equals(baz2, that.baz2) &&
                    Objects.equals(baz3, that.baz3) &&
                    Objects.equals(fooList, that.fooList) &&
                    Objects.equals(baz4, that.baz4) &&
                    Objects.equals(baz5, that.baz5);
        }

        @Override
        public int hashCode() {
            return Objects.hash(foo, ba1, baz1, baz2, baz3, fooList, baz4, baz5);
        }

        @Override
        public String toString() {
            return "ComplexObject{" +
                    "foo=" + foo +
                    ", ba1=" + ba1 +
                    ", baz1='" + baz1 + '\'' +
                    ", baz2='" + baz2 + '\'' +
                    ", baz3='" + baz3 + '\'' +
                    ", fooList=" + fooList +
                    ", baz4='" + baz4 + '\'' +
                    ", baz5='" + baz5 + '\'' +
                    '}';
        }
    }


    static class LargeNestedComplexObject {
        int foo = 1;
        int ba1 = 1;
        String baz1 = "abc";
        String baz2 = null;
        String baz3 = null;
        VerySimpleObject simpleObject1 = new VerySimpleObject();
        List<Integer> fooList1 = new ArrayList<Integer>(Arrays.asList(new Integer[]{1, null, 3}));
        VerySimpleObject simpleObject2 = new VerySimpleObject();
        List<Integer> fooList2 = new ArrayList<Integer>(Arrays.asList(new Integer[]{1, null, 3}));
        String baz4 = null;
        String baz5 = "abc......................................................................................abc";

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LargeNestedComplexObject that = (LargeNestedComplexObject) o;
            return foo == that.foo &&
                    ba1 == that.ba1 &&
                    Objects.equals(baz1, that.baz1) &&
                    Objects.equals(baz2, that.baz2) &&
                    Objects.equals(baz3, that.baz3) &&
                    Objects.equals(simpleObject1, that.simpleObject1) &&
                    Objects.equals(fooList1, that.fooList1) &&
                    Objects.equals(simpleObject2, that.simpleObject2) &&
                    Objects.equals(fooList2, that.fooList2) &&
                    Objects.equals(baz4, that.baz4) &&
                    Objects.equals(baz5, that.baz5);
        }

        @Override
        public int hashCode() {
            return Objects.hash(foo, ba1, baz1, baz2, baz3, simpleObject1, fooList1, simpleObject2, fooList2, baz4, baz5);
        }

        @Override
        public String toString() {
            return "LargeNestedComplexObject{" +
                    "foo=" + foo +
                    ", ba1=" + ba1 +
                    ", baz1='" + baz1 + '\'' +
                    ", baz2='" + baz2 + '\'' +
                    ", baz3='" + baz3 + '\'' +
                    ", simpleObject1=" + simpleObject1 +
                    ", fooList1=" + fooList1 +
                    ", simpleObject2=" + simpleObject2 +
                    ", fooList2=" + fooList2 +
                    ", baz4='" + baz4 + '\'' +
                    ", baz5='" + baz5 + '\'' +
                    '}';
        }
    }

    static class EmptyObject {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o != null && getClass() == o.getClass())
                return true;
            else
                return false;
        }

        @Override
        public int hashCode() {
            return this.getClass().hashCode();
        }

        @Override
        public String toString() {
            return super.toString();
        }
    }

    static class ObjectwithLotsOfNull {
        int foo = 1;
        int ba1 = 1;
        String baz1 = "abc";
        String baz2 = null;
        String baz3 = null;
        VerySimpleObject simpleObject1 =  new VerySimpleObject(1,2,null, null,null,null,null);
        List<Integer> fooList1 = new ArrayList<Integer>(Arrays.asList(new Integer[]{1, null, 3}));
        VerySimpleObject simpleObject2 =  new VerySimpleObject(1,2,null, null,null,null,null);
        List<Integer> fooList2 = null;
        String baz4 = null;
        String baz5 = "abc";
        String baz6 = null;
        String baz7 = null;
        String baz8 = "........................";
        String baz9 = null;
        String baz10 = null;
        VerySimpleObject simpleObject3 = new VerySimpleObject(1,2,null, null,null,null,null);
        String baz14 = null;
        String ba15 = "abc";
        String baz16 = null;
        String baz17 = null;
        String baz18 = null;
        String baz19 = null;
        String baz110 = null;
        String baz111 = null;
        String baz112 = null;
        VerySimpleObject simpleObject13 = new VerySimpleObject(1,2,null, null,null,null,null);

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ObjectwithLotsOfNull)) return false;
            ObjectwithLotsOfNull that = (ObjectwithLotsOfNull) o;
            return foo == that.foo &&
                    ba1 == that.ba1 &&
                    Objects.equals(baz1, that.baz1) &&
                    Objects.equals(baz2, that.baz2) &&
                    Objects.equals(baz3, that.baz3) &&
                    Objects.equals(simpleObject1, that.simpleObject1) &&
                    Objects.equals(fooList1, that.fooList1) &&
                    Objects.equals(simpleObject2, that.simpleObject2) &&
                    Objects.equals(fooList2, that.fooList2) &&
                    Objects.equals(baz4, that.baz4) &&
                    Objects.equals(baz5, that.baz5) &&
                    Objects.equals(baz6, that.baz6) &&
                    Objects.equals(baz7, that.baz7) &&
                    Objects.equals(baz8, that.baz8) &&
                    Objects.equals(baz9, that.baz9) &&
                    Objects.equals(baz10, that.baz10) &&
                    Objects.equals(simpleObject3, that.simpleObject3);
        }

        @Override
        public int hashCode() {
            return Objects.hash(foo, ba1, baz1, baz2, baz3, simpleObject1, fooList1, simpleObject2, fooList2, baz4, baz5, baz6, baz7, baz8, baz9, baz10, simpleObject3);
        }

        @Override
        public String toString() {
            return "ObjectwithLotsOfNull{" +
                    "foo=" + foo +
                    ", ba1=" + ba1 +
                    ", baz1='" + baz1 + '\'' +
                    ", baz2='" + baz2 + '\'' +
                    ", baz3='" + baz3 + '\'' +
                    ", simpleObject1=" + simpleObject1 +
                    ", fooList1=" + fooList1 +
                    ", simpleObject2=" + simpleObject2 +
                    ", fooList2=" + fooList2 +
                    ", baz4='" + baz4 + '\'' +
                    ", baz5='" + baz5 + '\'' +
                    ", baz6='" + baz6 + '\'' +
                    ", baz7='" + baz7 + '\'' +
                    ", baz8='" + baz8 + '\'' +
                    ", baz9='" + baz9 + '\'' +
                    ", baz10='" + baz10 + '\'' +
                    ", simpleObject3=" + simpleObject3 +
                    '}';
        }
    }

    static class DateObject {
/*        Integer dateAsInteger = 20200201; */
LocalDate date = LocalDate.of(2020,1,1);
MyDate mydate = new MyDate(LocalDate.now());
        LocalDateTime localDateTime = LocalDateTime.of(2020,1,1,0,0,0,0);
   /*
        LocalDate localDate =  LocalDate.now();
        LocalTime time =  LocalTime.now();
*/
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof DateObject))
                return false;
            DateObject object = (DateObject)o;
            return
                /*Objects.equals(dateAsInteger, object.dateAsInteger) &&
                Objects.equals(date, object.date) &&*/
                Objects.equals(localDateTime, object.localDateTime)
                /*&&
                Objects.equals(localDate, object.localDate) &&
                Objects.equals(time, object.time)*/
                ;

        }


        @Override public int hashCode() {
            return Objects.hash(/*dateAsInteger, date, */localDateTime/*, localDate, time*/);
        }
    }

    static class MyDate implements Binarylizable {
        LocalDate date;

        MyDate(LocalDate date) {
            this.date=date;
        }
        LocalDate get() {
            return date;
        }

        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.rawWriter().writeShort( (short) date.getYear());
            writer.rawWriter().writeByte( (byte) date.getMonthValue());
            writer.rawWriter().writeShort( (byte) date.getDayOfMonth());
        }

        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            LocalDate.of(reader.rawReader().readShort(),
                reader.rawReader().readByte(),
                reader.rawReader().readByte());
        }
    }
}
