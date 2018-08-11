<?php
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

namespace Apache\Ignite\Impl\Binary;

use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Impl\Binary\BinaryUtils;

class MessageBuffer
{
    const BYTE_ZERO = 0;
    const BYTE_ONE = 1;
    const BUFFER_CAPACITY_DEFAULT = 256;
    
    private $buffer;
    private $position;
    private $length;

    private static $isLittleEndian;

    public static function init(): void
    {
        MessageBuffer::$isLittleEndian = pack('L', 1) === pack('V', 1);
    }
    
    public function __construct(int $capacity = MessageBuffer::BUFFER_CAPACITY_DEFAULT)
    {
        $this->buffer = '';
        $this->position = 0;
        $this->length = 0;
        $this->ensureCapacity($capacity);
    }
    
    public function getLength(): int
    {
        return $this->length;
    }
    
    public function getBuffer(): string
    {
        return $this->getSlice(0, $this->getLength());
    }
    
    public function getSlice(int $startPos, int $length): string
    {
        return substr($this->buffer, $startPos, $length);
    }
    
    public function getPosition(): int
    {
        return $this->position;
    }
    
    public function setPosition(int $position): void
    {
        $this->ensureCapacity($position);
        $this->position = $position;
    }
    
    public function append(string &$buffer): void
    {
        $this->buffer .= $buffer;
        $this->length += strlen($buffer);
    }
    
    public function writeByte(int $value, $signed = true): void
    {
        $this->writeNumber($value, ObjectType::BYTE, $signed);
    }
    
    public function writeShort(int $value): void
    {
        $this->writeNumber($value, ObjectType::SHORT);
    }

    public function writeInteger(int $value): void
    {
        $this->writeNumber($value, ObjectType::INTEGER);
    }

    public function writeLong(float $value): void
    {
        $this->writeNumber($value, ObjectType::LONG);
    }

    public function writeFloat(float $value): void
    {
        $this->writeNumber($value, ObjectType::FLOAT);
    }

    public function writeDouble(float $value): void
    {
        $this->writeNumber($value, ObjectType::DOUBLE);
    }

    public function writeNumber($value, int $type, bool $signed = true): void
    {
        $format = $this->getNumberFormat($type, $signed);
        $strValue = pack($format, $value);
        if (strlen($strValue) !== TypeInfo::getTypeInfo($type)->getSize()) {
            BinaryUtils::unsupportedType(BinaryUtils::getTypeName($type));
        }
        $this->convertEndianness($strValue, $type);
        $this->writeStr($strValue);
    }
    
    public function writeBoolean(bool $value): void
    {
        $this->writeByte($value ? MessageBuffer::BYTE_ONE : MessageBuffer::BYTE_ZERO);
    }
    
    public function writeChar(string $value): void
    {
        $this->writeShort(mb_ord($value));
    }

    public function writeString(string $value): void
    {
        $length = strlen($value);
        $this->writeInteger($length);
        if ($length > 0) {
            $this->writeStr($value);
        }
    }
    
    public function writeBuffer(MessageBuffer $buffer, int $startPos, int $length): void
    {
        $this->writeStr($buffer->buffer, $startPos, $length);
    }
    
    public function readByte(bool $signed = true): int
    {
        return $this->readNumber(ObjectType::BYTE, $signed);
    }

    public function readShort(): int
    {
        return $this->readNumber(ObjectType::SHORT);
    }

    public function readInteger(): int
    {
        return $this->readNumber(ObjectType::INTEGER);
    }

    public function readLong(): float
    {
        return $this->readNumber(ObjectType::LONG);
    }

    public function readFloat(): float
    {
        return $this->readNumber(ObjectType::FLOAT);
    }

    public function readDouble(): float
    {
        return $this->readNumber(ObjectType::DOUBLE);
    }
    
    public function readNumber(int $type, bool $signed = true)
    {
        $size = BinaryUtils::getSize($type);
        $this->ensureSize($size);
        $strValue = substr($this->buffer, $this->position, $size);
        $this->convertEndianness($strValue, $type);
        $value = unpack($this->getNumberFormat($type, $signed), $strValue);
        $this->position += $size;
        return $value[1];
    }

    public function readBoolean(): bool
    {
        return $this->readByte() === MessageBuffer::BYTE_ONE;
    }
    
    public function readChar(): string {
        return mb_chr($this->readShort());
    }

    public function readString(): string
    {
        $bytesCount = $this->readInteger();
        $this->ensureSize($bytesCount);
        $result = substr($this->buffer, $this->position, $bytesCount);
        $this->position += $bytesCount;
        return $result;
    }
    
    private function getNumberFormat(int $type, bool $signed): string
    {
        switch ($type) {
            case ObjectType::BYTE:
                return $signed ? 'c' : 'C';
            case ObjectType::SHORT:
                return $signed ? 's' : 'S';
            case ObjectType::INTEGER:
                return $signed ? 'l' : 'L';
            case ObjectType::LONG:
                return $signed ? 'q' : 'Q';
            case ObjectType::FLOAT:
                return 'g';
            case ObjectType::DOUBLE:
                return 'e';
            default:
                BinaryUtils::internalError();
        }
    }

    private function convertEndianness(string &$value, int $type): void
    {
        if (!MessageBuffer::$isLittleEndian &&
            ($type === ObjectType::SHORT ||
             $type === ObjectType::INTEGER ||
             $type === ObjectType::LONG)) {
            $value = strrev($value);
        }
    }
    
    private function writeStr(string &$buffer, int $startPos = 0, int $length = -1): void
    {
        if ($length < 0) {
            $length = strlen($buffer);
        }
        $this->ensureCapacity($length);
        for ($i = 0; $i < $length; $i++) {
            $this->buffer[$this->position + $i] = $buffer[$startPos + $i];
        }
        if ($this->position + $length > $this->length) {
            $this->length = $this->position + $length;
        }
        $this->position += $length;
    }
    
    private function ensureCapacity(int $size): void
    {
        if ($size <= 0) {
            return;
        }
        $capacity = strlen($this->buffer);
        $newCapacity = $capacity > 0 ? $capacity : $size;
        while ($this->position + $size > $newCapacity) {
            $newCapacity = $newCapacity * 2;
        }
        if ($capacity < $newCapacity) {
            $this->buffer .= str_repeat('0', $newCapacity - $capacity);
        }
    }

    private function ensureSize(int $size): void
    {
        if ($this->position + $size > $this->getLength()) {
            BinaryUtils::internalError('Unexpected format of response');
        }
    }
}

MessageBuffer::init();
