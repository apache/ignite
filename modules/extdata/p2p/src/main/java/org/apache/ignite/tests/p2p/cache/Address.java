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

package org.apache.ignite.tests.p2p.cache;

    import java.io.Externalizable;
    import java.io.IOException;
    import java.io.ObjectInput;
    import java.io.ObjectOutput;

/**
 */
public class Address implements Externalizable {
    private String street;

    private int house;

    public Address() {
    }

    public Address(String street, int house) {
        this.street = street;
        this.house = house;
    }

    public String getStreet() {
        return street;
    }

    public int getHouse() {
        return house;
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(street);
        out.writeInt(house);
    }

    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        street = (String)in.readObject();
        house = in.readInt();
    }
}
