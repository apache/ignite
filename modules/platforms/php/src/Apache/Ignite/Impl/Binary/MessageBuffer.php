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

use Apache\Ignite\ObjectType\ObjectType;
use Apache\Ignite\Impl\Binary\BinaryUtils;
use Apache\Ignite\Exception\ClientException;

class MessageBuffer
{
    const BYTE_ZERO = 0;
    const BYTE_ONE = 1;
    const BUFFER_CAPACITY_DEFAULT = 256;
    const ENCODING_UTF_16 = 'UTF-16';

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
        return substr($this->buffer, 0, $this->getLength());
    }
    
    public function setPosition($position): void
    {
        $this->ensureCapacity($position);
        $this->position = $position;
    }
    
    public function append($buffer): void
    {
        $this->buffer .= $buffer;
        $this->length += strlen($buffer);
    }
    
    public function writeByte(int $value): void
    {
        $this->writeNumber($value, ObjectType::BYTE);
    }

    public function writeShort(int $value): void
    {
        $this->writeNumber($value, ObjectType::SHORT);
    }

    public function writeInteger(int $value): void
    {
        $this->writeNumber($value, ObjectType::INTEGER);
    }

    public function writeLong(int $value): void
    {
        $this->writeNumber($value, ObjectType::LONG);
    }

    public function writeFloat(float $value): void
    {
        // TODO: check size
        $this->writeNumber($value, ObjectType::FLOAT);
    }

    public function writeDouble(float $value): void
    {
        // TODO: check size
        $this->writeNumber($value, ObjectType::DOUBLE);
    }

    public function writeNumber($value, int $type): void
    {
        $strValue = null;
        switch ($type) {
            case ObjectType::BYTE:
                $strValue = pack('c', $value);
                break;
            case ObjectType::SHORT:
                $strValue = pack('s', $value);
                break;
            case ObjectType::INTEGER:
                $strValue = pack('l', $value);
                break;
            case ObjectType::LONG:
                $strValue = pack('q', $value);
                break;
            case ObjectType::FLOAT:
                $strValue = pack('g', $value);
                break;
            case ObjectType::DOUBLE:
                $strValue = pack('e', $value);
                break;
            default:
                throw ClientException::internalError();
        }
        // TODO: check pack errors
        $this->convertEndianness($strValue, $type);
        $this->writeBuffer($strValue);
    }

    public function writeBoolean(bool $value): void
    {
        $this->writeByte($value ? MessageBuffer::BYTE_ONE : MessageBuffer::BYTE_ZERO);
    }
    
    public function writeChar(string $value): void
    {
        $this->writeShort(mb_ord($value, MessageBuffer::ENCODING_UTF_16));
    }

    public function writeString(string $value): void
    {
        $length = strlen($value);
        $this->writeInteger($length);
        if ($length > 0) {
            $this->writeBuffer($value);
        }
    }
    
    public function writeDate(DateTime $value): void {
        $this->writeLong($value->getTimestamp());
    }
    
    public function readByte(): int
    {
        return $this->readNumber(ObjectType::BYTE);
    }

    public function readShort(): int
    {
        return $this->readNumber(ObjectType::SHORT);
    }

    public function readInteger(): int
    {
        return $this->readNumber(ObjectType::INTEGER);
    }

    public function readLong()
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
    
    public function readNumber(int $type)
    {
        $size = BinaryUtils::getSize($type);
        $this->ensureSize($size);
        $strValue = substr($this->buffer, $this->position, $size);
        $this->convertEndianness($strValue, $type);
        $value = 0;
        switch ($type) {
            case ObjectType::BYTE:
                $value = unpack('c', $strValue);
                break;
            case ObjectType::SHORT:
                $value = unpack('s', $strValue);
                break;
            case ObjectType::INTEGER:
                $value = unpack('l', $strValue);
                break;
            case ObjectType::LONG:
                $value = unpack('q', $strValue);
                break;
            case ObjectType::FLOAT:
                $value = unpack('g', $strValue);
                break;
            case ObjectType::DOUBLE:
                $value = unpack('e', $strValue);
                break;
            default:
                throw ClientException::internalError();
        }
        $this->position += $size;
        return $value[1];
    }

    public function readBoolean(): bool
    {
        return $this->readByte() === MessageBuffer::BYTE_ONE;
    }
    
    public function readChar(): string {
        return mb_chr($this->readShort(), MessageBuffer::ENCODING_UTF_16);
    }

    public function readString(): string
    {
        $bytesCount = $this->readInteger();
        $this->ensureSize($bytesCount);
        $result = substr($this->buffer, $this->position, $bytesCount);
        $this->position += $bytesCount;
        return $result;
    }
    
    public function readDate(): DateTime {
        $date = new DateTime();
        $date->setTimestamp($this->readLong());
        return $date;
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
    
    private function writeBuffer(string $buffer): void
    {
        $length = strlen($buffer);
        $this->ensureCapacity($length);
        for ($i = 0; $i < $length; $i++) {
            $this->buffer[$this->position + $i] = $buffer[$i];
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
            throw new ClientException('Unexpected format of response');
        }
    }
}

MessageBuffer::init();
