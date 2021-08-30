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

package org.apache.ignite.internal.table;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjects;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mappers;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 *
 */
@SuppressWarnings({
    "PMD.EmptyLineSeparatorCheck", "emptylineseparator",
    "unused", "UnusedAssignment", "InstanceVariableMayNotBeInitialized", "JoinDeclarationAndAssignmentJava"})
public class Example {
    /**
     * @return Table implementation.
     */
    private static List<Table> tableFactory() {
        return Collections.singletonList(new TableImpl(new DummyInternalTableImpl(), null, null, null));
    }

    /**
     * Use case 1: a simple one. The table has the structure
     * [
     * [id int, orgId int] // key
     * [name varchar, lastName varchar, decimal salary, int department] // value
     * ]
     * We show how to use the raw TableRow and a mapped class.
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("tableFactory")
    public void useCase1(Table t) {
        // Search row will allow nulls even in non-null columns.
        Tuple res = t.get(Tuple.create().set("id", 1).set("orgId", 1));

        String name = res.value("name");
        String lastName = res.value("latName");
        BigDecimal salary = res.value("salary");
        Integer department = res.value("department");

        // We may have primitive-returning methods if needed.
        int departmentPrimitive = res.intValue("department");

        // Note that schema itself already defined which fields are key field.
        class Employee {
            final int id;
            final int orgId;

            String name;
            String lastName;
            BigDecimal salary;
            int department;

            Employee(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }
        RecordView<Employee> employeeView = t.recordView(Employee.class);

        Employee e = employeeView.get(new Employee(1, 1));

        // As described in the IEP-54, we can have a truncated mapping.
        class TruncatedEmployee {
            final int id;
            final int orgId;

            String name;
            String lastName;

            TruncatedEmployee(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }

        RecordView<TruncatedEmployee> truncatedEmployeeView = t.recordView(TruncatedEmployee.class);

        // salary and department will not be sent over the network during this call.
        TruncatedEmployee te = truncatedEmployeeView.get(new TruncatedEmployee(1, 1));
    }

    /**
     * Use case 2: using simple KV mappings
     * The table has structure is
     * [
     * [id int, orgId int] // key
     * [name varchar, lastName varchar, decimal salary, int department] // value
     * ]
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("tableFactory")
    public void useCase2(Table t) {
        class EmployeeKey {
            final int id;
            final int orgId;

            EmployeeKey(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }

        class Employee {
            String name;
            String lastName;
            BigDecimal salary;
            int department;
        }

        KeyValueView<EmployeeKey, Employee> employeeKv = t.kvView(EmployeeKey.class, Employee.class);

        employeeKv.get(new EmployeeKey(1, 1));

        // As described in the IEP-54, we can have a truncated KV mapping.
        class TruncatedEmployee {
            String name;
            String lastName;
        }

        KeyValueView<EmployeeKey, TruncatedEmployee> truncatedEmployeeKv = t.kvView(EmployeeKey.class, TruncatedEmployee.class);

        TruncatedEmployee te = truncatedEmployeeKv.get(new EmployeeKey(1, 1));
    }

    /**
     * Use case 3: Single table strategy for inherited objects.
     * The table has structure is
     * [
     * [id long] // key
     * [owner varchar, cardNumber long, expYear int, expMonth int, accountNum long, bankName varchar] // value
     * ]
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("tableFactory")
    public void useCase3(Table t) {
        class BillingDetails {
            String owner;
        }

        class CreditCard extends BillingDetails {
            long cardNumber;
            int expYear;
            int expMonth;
        }

        class BankAccount extends BillingDetails {
            long account;
            String bankName;
        }

        KeyValueView<Long, CreditCard> credCardKvView = t.kvView(Long.class, CreditCard.class);
        CreditCard creditCard = credCardKvView.get(1L);

        KeyValueView<Long, BankAccount> backAccKvView = t.kvView(Long.class, BankAccount.class);
        BankAccount bankAccount = backAccKvView.get(2L);

        // Truncated view.
        KeyValueView<Long, BillingDetails> billingDetailsKVView = t.kvView(Long.class, BillingDetails.class);
        BillingDetails billingDetails = billingDetailsKVView.get(2L);

        // Without discriminator it is impossible to deserialize to correct type automatically.
        assert !(billingDetails instanceof CreditCard);
        assert !(billingDetails instanceof BankAccount);

        // Wide record.
        class BillingRecord {
            final long id;

            String owner;

            long cardNumber;
            int expYear;
            int expMonth;

            long account;
            String bankName;

            BillingRecord(long id) {
                this.id = id;
            }
        }

        final RecordView<BillingRecord> billingView = t.recordView(BillingRecord.class);

        final BillingRecord br = billingView.get(new BillingRecord(1));
    }

    /**
     * Use case 4: Conditional serialization.
     * The table has structure is
     * [
     * [id int, orgId int] // key
     * [owner varchar, type int, conditionalDetails byte[]] // value
     * ]
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("tableFactory")
    public void useCase4(Table t) {
        class OrderKey {
            final int id;
            final int orgId;

            OrderKey(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }

        class OrderValue {
            String owner;
            int type; // Discriminator value.
            /* BillingDetails */ Object billingDetails;
        }

        class CreditCard /* extends BillingDetails */ {
            long cardNumber;
            int expYear;
            int expMonth;
        }

        class BankAccount /* extends BillingDetails */ {
            long account;
            String bankName;
        }

        KeyValueView<OrderKey, OrderValue> orderKvView = t.kvView(Mappers.ofKeyClass(OrderKey.class),
            Mappers.ofValueClassBuilder(OrderValue.class)
                .map("billingDetails", (row) -> {
                    BinaryObject bObj = row.binaryObjectValue("conditionalDetails");
                    int type = row.intValue("type");

                    return type == 0 ?
                        BinaryObjects.deserialize(bObj, CreditCard.class) :
                        BinaryObjects.deserialize(bObj, BankAccount.class);
                }).build());

        OrderValue ov = orderKvView.get(new OrderKey(1, 1));

        // Same with direct Row access and BinaryObject wrapper.
        Tuple res = t.get(Tuple.create().set("id", 1).set("orgId", 1));

        byte[] objData = res.value("billingDetails");
        BinaryObject binObj = BinaryObjects.wrap(objData);
        // Work with the binary object as in Ignite 2.x

        // Additionally, we may have a shortcut similar to primitive methods.
        binObj = res.binaryObjectValue("billingDetails");

        // Same with RecordAPI.
        class OrderRecord {
            final int id;
            final int orgId;

            String owner;
            int type;
            BinaryObject billingDetails;

            OrderRecord(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }

        final RecordView<OrderRecord> orderRecView = t.recordView(OrderRecord.class);

        OrderRecord orderRecord = orderRecView.get(new OrderRecord(1, 1));
        binObj = orderRecord.billingDetails;

        // Manual deserialization is possible as well.
        Object billingDetails = orderRecord.type == 0 ?
            BinaryObjects.deserialize(binObj, CreditCard.class) :
            BinaryObjects.deserialize(binObj, BankAccount.class);
    }

    /**
     * Use case 5: using byte[] and binary objects in columns.
     * The table has structure
     * [
     * [id int, orgId int] // key
     * [originalObject byte[], upgradedObject byte[], int department] // value
     * ]
     * Where {@code originalObject} is some value that was originally put to the column,
     * {@code upgradedObject} is a version 2 of the object, and department is an extracted field.
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("tableFactory")
    public void useCase5(Table t) {
        Tuple res = t.get(Tuple.create().set("id", 1).set("orgId", 1));

        byte[] objData = res.value("originalObject");
        BinaryObject binObj = BinaryObjects.wrap(objData);
        // Work with the binary object as in Ignite 2.x

        // Additionally, we may have a shortcut similar to primitive methods.
        binObj = res.binaryObjectValue("upgradedObject");

        // Plain byte[] and BinaryObject fields in a class are straightforward.
        class Record {
            final int id;
            final int orgId;

            byte[] originalObject;
            Tuple upgradedObject;
            int department;

            Record(int id, int orgId) {
                this.id = id;
                this.orgId = orgId;
            }
        }

        RecordView<Record> recordView = t.recordView(Record.class);

        // Similarly work with the binary objects.
        Record rec = recordView.get(new Record(1, 1));

        // Now assume that we have some POJO classes to deserialize the binary objects.
        class JavaPerson {
            String name;
            String lastName;
        }

        class JavaPersonV2 extends JavaPerson {
            int department;
        }

        // We can have a compound record deserializing the whole tuple automatically.
        class JavaPersonRecord {
            JavaPerson originalObject;
            JavaPersonV2 upgradedObject;
            int department;
        }

        RecordView<JavaPersonRecord> personRecordView = t.recordView(JavaPersonRecord.class);

        // Or we can have an arbitrary record with custom class selection.
        class TruncatedRecord {
            JavaPerson upgradedObject;
            int department;
        }

        RecordView<TruncatedRecord> truncatedView = t.recordView(
            Mappers.ofRecordClassBuilder(TruncatedRecord.class)
                .map("upgradedObject", JavaPersonV2.class).build());

        // Or we can have a custom conditional type selection.
        RecordView<TruncatedRecord> truncatedView2 = t.recordView(
            Mappers.ofRecordClassBuilder(TruncatedRecord.class)
                .map("upgradedObject", (row) -> {
                    BinaryObject bObj = row.binaryObjectValue("upgradedObject");
                    int dept = row.intValue("department");

                    return dept == 0 ?
                        BinaryObjects.deserialize(bObj, JavaPerson.class) :
                        BinaryObjects.deserialize(bObj, JavaPersonV2.class);
                }).build());
    }

    /**
     * Use case 1: a simple one. The table has the structure
     * [
     * [id long] // key
     * [name varchar, lastName varchar, decimal salary, int department] // value
     * ]
     * We show how to use the raw TableRow and a mapped class.
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("tableFactory")
    public void useCase6(Table t) {
        // Search row will allow nulls even in non-null columns.
        Tuple res = t.get(Tuple.create().set("id", 1));

        String name = res.value("name");
        String lastName = res.value("latName");
        BigDecimal salary = res.value("salary");
        Integer department = res.value("department");

        // We may have primitive-returning methods if needed.
        int departmentPrimitive = res.intValue("department");

        // Note that schema itself already defined which fields are key field.
        class Employee {
            String name;
            String lastName;
            BigDecimal salary;
            int department;
        }

        class Key {
            long id;
        }

        KeyValueView<Long, Employee> employeeView = t.kvView(Long.class, Employee.class);

        Employee e = employeeView.get(1L);
    }

    /**
     * Use case 1: a simple one. The table has the structure
     * [
     * [byte[]] // key
     * [name varchar, lastName varchar, decimal salary, int department] // value
     * ]
     * We show how to use the raw TableRow and a mapped class.
     */
    @Disabled
    @ParameterizedTest
    @MethodSource("tableFactory")
    public void useCase7(Table t) {
        // Note that schema itself already defined which fields are key field.
        class Employee {
            String name;
            String lastName;
            BigDecimal salary;
            int department;
        }

        KeyValueView<Long, BinaryObject> employeeView = t.kvView(Long.class, BinaryObject.class);

        employeeView.put(1L, BinaryObjects.wrap(new byte[0] /* serialized Employee */));

        t.kvView(
            Mappers.identity(),
            Mappers.ofValueClassBuilder(BinaryObject.class).deserializeTo(Employee.class).build());
    }
}
