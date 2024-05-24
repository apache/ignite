/*
 * Copyright 2020 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.vertx.spi.cluster.ignite.impl;

import io.vertx.core.spi.cluster.RegistrationInfo;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;

import java.util.Objects;

/**
 * @author Thomas Segismont
 * @author Lukas Prettenthaler
 */
public class IgniteRegistrationInfo implements Binarylizable, Comparable<IgniteRegistrationInfo> {
  private String address;
  private RegistrationInfo registrationInfo;

  public IgniteRegistrationInfo() {
  }

  public IgniteRegistrationInfo(String address, RegistrationInfo registrationInfo) {
    this.address = Objects.requireNonNull(address);
    this.registrationInfo = Objects.requireNonNull(registrationInfo);
  }

  public String address() {
    return address;
  }

  public RegistrationInfo registrationInfo() {
    return registrationInfo;
  }

  @Override
  public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
    writer.writeString("address", address);
    writer.writeString("nodeId", registrationInfo.nodeId());
    writer.writeLong("seq", registrationInfo.seq());
    writer.writeBoolean("isLocalOnly", registrationInfo.localOnly());
  }

  @Override
  public void readBinary(BinaryReader reader) throws BinaryObjectException {
    address = reader.readString("address");
    registrationInfo = new RegistrationInfo(reader.readString("nodeId"), reader.readLong("seq"), reader.readBoolean("isLocalOnly"));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    IgniteRegistrationInfo that = (IgniteRegistrationInfo) o;

    if (!address.equals(that.address)) return false;
    return registrationInfo.equals(that.registrationInfo);
  }

  @Override
  public int hashCode() {
    int result = address.hashCode();
    result = 31 * result + registrationInfo.hashCode();
    return result;
  }

  @Override
  public int compareTo(IgniteRegistrationInfo other) {
    return Integer.compare(hashCode(), other.hashCode());
  }
}
