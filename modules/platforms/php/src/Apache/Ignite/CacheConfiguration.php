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

namespace Apache\Ignite;

use Apache\Ignite\Type\ObjectType;
use Apache\Ignite\Impl\Binary\CacheConfigurationInfo;
use Apache\Ignite\Impl\Binary\MessageBuffer;
use Apache\Ignite\Impl\Binary\BinaryUtils;
use Apache\Ignite\Impl\Binary\BinaryWriter;

/**
 * Class representing Ignite cache configuration on a server.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting. 
 */
class CacheConfiguration
{
    private $properties;
    
    public function __construct()
    {
        $this->properties = [];
    }
    
    /**
     *
     *
     * @param string $sqlSchema
     *
     * @return CacheConfiguration the same instance of the CacheConfiguration.
     */
    public function setSqlSchema(string $sqlSchema): CacheConfiguration
    {
        $this->properties[CacheConfigurationInfo::PROP_SQL_SCHEMA] = $sqlSchema;
        return $this;
    }

    /**
     *
     *
     * @return string
     */
    public function getSqlSchema(): ?string
    {
        return $this->getProperty(CacheConfigurationInfo::PROP_SQL_SCHEMA);
    }
    
    private function getProperty(int $prop)
    {
        if (array_key_exists($prop, $this->properties)) {
            return $this->properties[$prop];
        }
        return null;
    }

    public function write(MessageBuffer $buffer, string $name): void
    {
        $this->properties[CacheConfigurationInfo::PROP_NAME] = $name;

        $startPos = $buffer->getPosition();
        $buffer->setPosition($startPos +
            BinaryUtils::getSize(ObjectType::INTEGER) +
            BinaryUtils::getSize(ObjectType::SHORT));

        foreach ($this->properties as $propertyCode => $property) {
            $this->writeProperty($buffer, $propertyCode, $property);
        }

        $length = $buffer->getPosition() - $startPos;
        $buffer->setPosition($startPos);

        $buffer->writeInteger($length);
        $buffer->writeShort(count($this->properties));
    }

    private function writeProperty(MessageBuffer $buffer, int $propertyCode, $property): void
    {
        $buffer->writeShort($propertyCode);
        $propertyType = CacheConfigurationInfo::getType($propertyCode);
        switch (BinaryUtils::getTypeCode($propertyType)) {
            case ObjectType::INTEGER:
            case ObjectType::LONG:
            case ObjectType::BOOLEAN:
                BinaryWriter::writeObject($buffer, $property, $propertyType, false);
                return;
            case ObjectType::STRING:
                BinaryWriter::writeObject($buffer, $property, $propertyType);
                return;
            case ObjectType::OBJECT_ARRAY:
                $length = $property ? count($property) : 0;
                $buffer->writeInteger($length);
                foreach ($property as $prop) {
                    $prop->write($buffer);
                }
                return;
            default:
                BinaryUtils::internalError();
        }
    }
}
