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

namespace Apache\Ignite\Cache;

use Apache\Ignite\Internal\Binary\BinaryCommunicator;
use Apache\Ignite\Internal\Binary\MessageBuffer;

/**
 * Class representing Cache Key part of Ignite CacheConfiguration.
 *
 * All configuration settings are optional and have defaults which are defined on a server side.
 *
 * See Apache Ignite documentation for details of every configuration setting.
 */
class CacheKeyConfiguration
{
    private $typeName;
    private $affinityKeyFieldName;

    /**
     * CacheKeyConfiguration constructor.
     *
     * @param string|null $typeName
     * @param string|null $affinityKeyFieldName
     */
    public function __construct(string $typeName = null, string $affinityKeyFieldName = null)
    {
        $this->typeName = $typeName;
        $this->affinityKeyFieldName = $affinityKeyFieldName;
    }

    /**
     *
     *
     * @param string $typeName
     *
     * @return CacheKeyConfiguration the same instance of the CacheKeyConfiguration.
     */
    public function setTypeName(string $typeName): CacheKeyConfiguration
    {
        $this->typeName = $typeName;
        return $this;
    }

    /**
     *
     *
     * @return string|null
     */
    public function getTypeName(): ?string
    {
        return $this->typeName;
    }

    /**
     *
     *
     * @param string $affinityKeyFieldName
     *
     * @return CacheKeyConfiguration the same instance of the CacheKeyConfiguration.
     */
    public function setAffinityKeyFieldName(string $affinityKeyFieldName): CacheKeyConfiguration
    {
        $this->affinityKeyFieldName = $affinityKeyFieldName;
        return $this;
    }

    /**
     *
     *
     * @return string|null
     */
    public function getAffinityKeyFieldName(): ?string
    {
        return $this->affinityKeyFieldName;
    }

    // This is not the public API method, is not intended for usage by an application.
    public function write(BinaryCommunicator $communicator, MessageBuffer $buffer): void
    {
        BinaryCommunicator::writeString($buffer, $this->typeName);
        BinaryCommunicator::writeString($buffer, $this->affinityKeyFieldName);
    }

    // This is not the public API method, is not intended for usage by an application.
    public function read(BinaryCommunicator $communicator, MessageBuffer $buffer): void
    {
        $this->typeName = BinaryCommunicator::readString($buffer);
        $this->affinityKeyFieldName = BinaryCommunicator::readString($buffer);
    }
}
