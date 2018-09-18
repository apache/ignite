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

    static from(source, position) {
        const buf = new MessageBuffer();
        buf._buffer = Buffer.from(source);
        buf._position = position;
        buf._length = buf._buffer.length;
        buf._capacity = buf._length;
        return buf;
    }

    concat(source) {
        this._buffer = Buffer.concat([this._buffer, source]);
        this._length = this._buffer.length;
        this._capacity = this._length;
    }

    get position() {
        return this._position;
    }

    set position(position) {
        this._position = position;
    }

    get length() {
        return this._length;
    }

    get data() {
        return this.getSlice(0, this.length);
    }

    get buffer() {
        return this._buffer;
    }

    getSlice(start, end) {
        return this._buffer.slice(start, end);
    }

    writeByte(value) {
        this.writeNumber(value, BinaryUtils.TYPE_CODE.BYTE);
    }

    writeShort(value) {
        this.writeNumber(value, BinaryUtils.TYPE_CODE.SHORT);
    }

    writeInteger(value) {
        this.writeNumber(value, BinaryUtils.TYPE_CODE.INTEGER);
    }

    writeLong(value) {
        try {
            if (!Long.isLong(value)) {
                value = Long.fromValue(value);
            }
        }
        catch (err) {
            throw Errors.IgniteClientError.valueCastError(value, BinaryUtils.TYPE_CODE.LONG);
        }
        const buffer = Buffer.from(value.toBytesLE());
        this.writeBuffer(buffer);
    }

    writeFloat(value) {
        this.writeNumber(value, BinaryUtils.TYPE_CODE.FLOAT);
    }

    writeDouble(value) {
        this.writeNumber(value, BinaryUtils.TYPE_CODE.DOUBLE);
    }

    writeNumber(value, type, signed = true) {
        const size = BinaryUtils.getSize(type);
        this._ensureCapacity(size);
        try {
            switch (type) {
                case BinaryUtils.TYPE_CODE.BYTE:
                    if (signed) {
                        this._buffer.writeInt8(value, this._position);
                    }
                    else {
                        this._buffer.writeUInt8(value, this._position);   
                    }
                    break;
                case BinaryUtils.TYPE_CODE.SHORT:
                    if (signed) {
                        this._buffer.writeInt16LE(value, this._position);
                    }
                    else {
                        this._buffer.writeUInt16LE(value, this._position);   
                    }
                    break;
                case BinaryUtils.TYPE_CODE.INTEGER:
                    if (signed) {
                        this._buffer.writeInt32LE(value, this._position);
                    }
                    else {
                        this._buffer.writeUInt32LE(value, this._position);   
                    }
                    break;
                case BinaryUtils.TYPE_CODE.FLOAT:
                    this._buffer.writeFloatLE(value, this._position);
                    break;
                case BinaryUtils.TYPE_CODE.DOUBLE:
                    this._buffer.writeDoubleLE(value, this._position);
                    break;
                default:
                    throw Errors.IgniteClientError.internalError();
            }
        }
        catch (err) {
            throw Errors.IgniteClientError.valueCastError(value, type);
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
            this.writeBuffer(buffer);
        }
    }

    writeDate(value) {
        this.writeLong(value.getTime());
    }

    readByte() {
        return this.readNumber(BinaryUtils.TYPE_CODE.BYTE);
    }

    readShort() {
        return this.readNumber(BinaryUtils.TYPE_CODE.SHORT);
    }

    readInteger() {
        return this.readNumber(BinaryUtils.TYPE_CODE.INTEGER);
    }

    readLong() {
        const size = BinaryUtils.getSize(BinaryUtils.TYPE_CODE.LONG)
        this._ensureSize(size);
        const value = Long.fromBytesLE([...this._buffer.slice(this._position, this._position + size)]);
        this._position += size;
        return value;
    }

    readFloat() {
        return this.readNumber(BinaryUtils.TYPE_CODE.FLOAT);
    }

    readDouble() {
        return this.readNumber(BinaryUtils.TYPE_CODE.DOUBLE);
    }

    readNumber(type, signed = true) {
        const size = BinaryUtils.getSize(type);
        this._ensureSize(size);
        let value;
        switch (type) {
            case BinaryUtils.TYPE_CODE.BYTE:
                value = signed ? this._buffer.readInt8(this._position) : this._buffer.readUInt8(this._position);
                break;
            case BinaryUtils.TYPE_CODE.SHORT:
                value = signed ? this._buffer.readInt16LE(this._position) : this._buffer.readUInt16LE(this._position);
                break;
            case BinaryUtils.TYPE_CODE.INTEGER:
                value = signed ? this._buffer.readInt32LE(this._position) : this._buffer.readUInt32LE(this._position);
                break;
            case BinaryUtils.TYPE_CODE.FLOAT:
                value = this._buffer.readFloatLE(this._position);
                break;
            case BinaryUtils.TYPE_CODE.DOUBLE:
                value = this._buffer.readDoubleLE(this._position);
                break;
            default:
                throw Errors.IgniteClientError.internalError();
        }
        this._position += size;
        return value;
    }

    readBoolean() {
        return this.readByte() === BYTE_ONE;
    }

    readChar() {
        return String.fromCharCode(this.readShort());
    }

    readString() {
        const bytesCount = this.readInteger();
        this._ensureSize(bytesCount);
        const result = this._buffer.toString(BinaryUtils.ENCODING, this._position, this._position + bytesCount);
        this._position += bytesCount;
        return result;
    }

    readBuffer(length) {
        this._ensureSize(length);
        const result = this._buffer.slice(this._position, this._position + length);
        this._position += length;
        return result;
    }

    readDate() {
        return new Date(this.readLong().toNumber());
    }

    writeBuffer(buffer, start = undefined, end = undefined) {
        if (start === undefined) {
            start = 0;
        }
        if (end === undefined) {
            end = buffer.length;
        }
        const size = end - start; 
        this._ensureCapacity(size);
        buffer.copy(this._buffer, this._position, start, end);
        this._position += size;
    }

    _ensureSize(size) {
        if (this._position + size > this._length) {
            throw Errors.IgniteClientError.internalError('Unexpected format of response');
        }
    }

    _ensureCapacity(valueSize) {
        if (valueSize <= 0) {
            throw Errors.IgniteClientError.internalError();
        }
        let newCapacity = this._capacity;
        while (this._position + valueSize > newCapacity) {
            newCapacity = newCapacity * 2;
        }
        if (this._capacity < newCapacity) {
            this._buffer = Buffer.concat([this._buffer, Buffer.allocUnsafe(newCapacity - this._capacity)], newCapacity);
            this._capacity = newCapacity;
        }
        if (this._position + valueSize > this._length) {
            this._length = this._position + valueSize;
        }
    }
}

module.exports = MessageBuffer;
