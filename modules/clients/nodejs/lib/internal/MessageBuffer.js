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

'use strict';

const Long = require('long');
const BinaryUtils = require('./BinaryUtils');
const ObjectType = require('../ObjectType');
const Errors = require('../Errors');

const BUFFER_CAPACITY_DEFAULT = 256;
const BYTE_ZERO = 0;
const BYTE_ONE = 1;

class MessageBuffer {
    constructor(capacity = BUFFER_CAPACITY_DEFAULT) {
        this._buffer = Buffer.allocUnsafe(capacity);
        this._capacity = capacity;
        this._length = 0;
        this._position = 0;
    }

    static from(source) {
        const buf = new MessageBuffer();
        buf._buffer = Buffer.from(source);
        buf._length = buf._buffer.length;
        return buf;
    }

    set position(position) {
        this._position = position;
    }

    get length() {
        return this._length;
    }

    get data() {
        return this._buffer.slice(0, this.length);
    }

    writeByte(value) {
        this.writeNumber(value, ObjectType.TYPE_CODE.BYTE);
    }

    writeShort(value) {
        this.writeNumber(value, ObjectType.TYPE_CODE.SHORT);
    }

    writeInteger(value) {
        this.writeNumber(value, ObjectType.TYPE_CODE.INTEGER);
    }

    writeLong(value) {
        if (!Long.isLong(value)) {
            value = Long.fromValue(value);
        }
        const buffer = Buffer.from(value.toBytesLE());
        this._writeBuffer(buffer);
    }

    writeFloat(value) {
        this.writeNumber(value, ObjectType.TYPE_CODE.FLOAT);
    }

    writeDouble(value) {
        this.writeNumber(value, ObjectType.TYPE_CODE.DOUBLE);
    }

    writeNumber(value, type) {
        const size = BinaryUtils.getSize(type);
        this._ensureCapacity(size);
        switch (type) {
            case ObjectType.TYPE_CODE.BYTE:
                this._buffer.writeInt8(value, this._position);
                break;
            case ObjectType.TYPE_CODE.SHORT:
                this._buffer.writeInt16LE(value, this._position);
                break;
            case ObjectType.TYPE_CODE.INTEGER:
                this._buffer.writeInt32LE(value, this._position);
                break;
            case ObjectType.TYPE_CODE.FLOAT:
                this._buffer.writeFloatLE(value, this._position);
                break;
            case ObjectType.TYPE_CODE.DOUBLE:
                this._buffer.writeDoubleLE(value, this._position);
                break;
            default:
                throw new Errors.InternalError();
        }
        this._position += size;
    }

    writeBoolean(value) {
        this.writeByte(value ? BYTE_ONE : BYTE_ZERO);
    }

    writeChar(value) {
        this.writeShort(value.charCodeAt(0));
    }

    writeString(value) {
        const buffer = Buffer.from(value, BinaryUtils.ENCODING);
        const length = buffer.length;
        this.writeInteger(length);
        if (length > 0) {
            this._writeBuffer(buffer);
        }
    }

    writeDate(value) {
        this.writeLong(value.getTime());
    }

    readByte() {
        return this.readNumber(ObjectType.TYPE_CODE.BYTE);
    }

    readShort() {
        return this.readNumber(ObjectType.TYPE_CODE.SHORT);
    }

    readInteger() {
        return this.readNumber(ObjectType.TYPE_CODE.INTEGER);
    }

    readLong() {
        const size = BinaryUtils.getSize(ObjectType.TYPE_CODE.LONG)
        const value = Long.fromBytesLE([...this._buffer.slice(this._position, this._position + size)]);
        this._position += size;
        return value;
    }

    readFloat() {
        return this.readNumber(ObjectType.TYPE_CODE.FLOAT);
    }

    readDouble() {
        return this.readNumber(ObjectType.TYPE_CODE.DOUBLE);
    }

    readNumber(type) {
        let value;
        switch (type) {
            case ObjectType.TYPE_CODE.BYTE:
                value = this._buffer.readInt8(this._position);
                break;
            case ObjectType.TYPE_CODE.SHORT:
                value = this._buffer.readInt16LE(this._position);
                break;
            case ObjectType.TYPE_CODE.INTEGER:
                value = this._buffer.readInt32LE(this._position);
                break;
            case ObjectType.TYPE_CODE.FLOAT:
                value = this._buffer.readFloatLE(this._position);
                break;
            case ObjectType.TYPE_CODE.DOUBLE:
                value = this._buffer.readDoubleLE(this._position);
                break;
            default:
                throw new Errors.InternalError();
        }
        this._position += BinaryUtils.getSize(type);
        return value;
    }

    readBoolean(value) {
        return this.readByte() === BYTE_ONE;
    }

    readChar(value) {
        return String.fromCharCode(this.readShort());
    }

    readString() {
        const bytesCount = this.readInteger();
        const result = this._buffer.toString(BinaryUtils.ENCODING, this._position, this._position + bytesCount);
        this._position += bytesCount;
        return result;
    }

    readDate() {
        return new Date(this.readLong().toNumber());
    }

    _writeBuffer(buffer) {
        this._ensureCapacity(buffer.length);
        buffer.copy(this._buffer, this._position);
        this._position += buffer.length;
    }

    _ensureCapacity(valueSize) {
        if (valueSize <= 0) {
            throw new Errors.InternalError();
        }
        if (this._position + valueSize > this._capacity) {
            const newCapacity = this._capacity * 2;
            this._buffer = Buffer.concat([this._buffer, Buffer.allocUnsafe(this._capacity)], newCapacity);
            this._capacity = newCapacity;
        }
        if (this._position + valueSize > this._length) {
            this._length = this._position + valueSize;
        }
    }
}

module.exports = MessageBuffer;
