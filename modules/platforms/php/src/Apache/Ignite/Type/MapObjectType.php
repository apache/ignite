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

namespace Apache\Ignite\Type;

use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Impl\Utils\ArgumentChecker;
use Apache\Ignite\Impl\Binary\BinaryUtils;

/** 
 * Class representing a map type of Ignite object.
 * 
 * It is described by ObjectType::MAP and one of @ref MapSubType.
 */
class MapObjectType extends ObjectType
{
    /** @name MapSubType
     *  @anchor MapSubType
     *  @{
     */

    /**
     * Basic hash map.
     */
    const HASH_MAP = 1;
    
    /**
     * Hash map, which maintains element order.
     */
    const LINKED_HASH_MAP = 2;
    
    /** @} */ // end of MapSubType
    
    private $subType;
    private $keyType;
    private $valueType;

    /**
     * Public constructor.
     *
     * Optionally specifies a kind of map and types of keys and values in the map.
     *
     * If a kind of map is not specified, MapObjectType::HASH_MAP is assumed.
     *
     * If key and/or value type is not specified then during operations the Ignite client
     * will try to make automatic mapping between PHP types and Ignite object types -
     * according to the mapping table defined in the description of the ObjectType class.
     * 
     * @param int $subType map subtype, one of @ref MapSubType constants.
     * @param int|ObjectType|null $keyType type of the keys in the map:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     * @param int|ObjectType|null $valueType type of the values in the map:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     * 
     * @throws ClientException if error.
     */
    public function __construct(int $subType = MapObjectType::HASH_MAP, $keyType = null, $valueType = null)
    {
        parent::__construct(ObjectType::MAP);
        ArgumentChecker::hasValueFrom(
            $subType, 'subType', false, [MapObjectType::HASH_MAP, MapObjectType::LINKED_HASH_MAP]);
        BinaryUtils::checkObjectType($keyType, 'keyType');
        BinaryUtils::checkObjectType($valueType, 'valueType');
        $this->subType = $subType;
        $this->keyType = $keyType;
        $this->valueType = $valueType;
    }

    /**
     * Returns map subtype, one of @ref MapSubType constants.
     * 
     * @return int map subtype, one of @ref MapSubType constants.
     */
    public function getSubType(): int
    {
        return $this->subType;
    }
    
    /**
     * Returns type of the keys in the map.
     * 
     * @return int|ObjectType|null type of the keys in the map:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     */
    public function getKeyType()
    {
        return $this->keyType;
    }

    /**
     * Returns type of the values in the map.
     * 
     * @return int|ObjectType|null type of the values in the map:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     */
    public function getValueType()
    {
        return $this->valueType;
    }
}
