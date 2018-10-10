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
use Apache\Ignite\Internal\Utils\ArgumentChecker;
use Apache\Ignite\Internal\Binary\BinaryUtils;

/** 
 * Class representing a collection type of Ignite object.
 * 
 * It is described by ObjectType::COLLECTION and one of @ref CollectionSubType.
 */
class CollectionObjectType extends ObjectType
{
    /** @name CollectionSubType
     *  @anchor CollectionSubType
     *  @{
     */
    
    /**
     * General set type, which can not be mapped to more specific set type.
     */
    const USER_SET = -1;
    
    /**
     * General collection type, which can not be mapped to any specific collection type.
     */
    const USER_COL = 0;
    
    /**
     * Resizeable array type.
     */
    const ARRAY_LIST = 1;
    
    /**
     * Linked list type.
     */
    const LINKED_LIST = 2;
    
    /**
     * Basic hash set type.
     */
    const HASH_SET = 3;
    
    /**
     * Hash set type, which maintains element order.
     */
    const LINKED_HASH_SET = 4;
    
    /**
     * This is a collection that only contains a single element, but behaves as a collection.
     */
    const SINGLETON_LIST = 5;

    /** @} */ // end of CollectionSubType

    private $subType;
    private $elementType;
    
    /**
     * Public constructor.
     *
     * Specifies the collection subtype and optionally specifies Ignite type of elements in the collection.
     *
     * If Ignite type of elements is not specified then during operations the Ignite client
     * tries to make automatic mapping between PHP types and Ignite object types -
     * according to the mapping table defined in the description of the ObjectType class.
     * 
     * @param int $subType collection subtype, one of @ref CollectionSubType constants.
     * @param int|ObjectType|null $elementType Ignite type of elements in the collection:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     * 
     * @throws ClientException if error.
     */
    public function __construct(int $subType, $elementType = null)
    {
        parent::__construct(ObjectType::COLLECTION);
        ArgumentChecker::hasValueFrom(
            $subType, 'subType', false, 
            [
                CollectionObjectType::USER_SET,
                CollectionObjectType::USER_COL,
                CollectionObjectType::ARRAY_LIST,
                CollectionObjectType::LINKED_LIST,
                CollectionObjectType::HASH_SET,
                CollectionObjectType::LINKED_HASH_SET,
                CollectionObjectType::SINGLETON_LIST
            ]);
        BinaryUtils::checkObjectType($elementType, 'elementType');
        $this->subType = $subType;
        $this->elementType = $elementType;
    }

    /**
     * Returns collection subtype, one of @ref CollectionSubType constants.
     * 
     * @return int collection subtype, one of @ref CollectionSubType constants.
     */
    public function getSubType(): int
    {
        return $this->subType;
    }
    
    /**
     * Returns Ignite type of elements in the collection.
     * 
     * @return int|ObjectType|null type of elements in the collection:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null that means the type is not specified
     */
    public function getElementType()
    {
        return $this->elementType;
    }
    
    public static function isSet($subType): bool
    {
        return $subType === CollectionObjectType::USER_SET ||
            $subType === CollectionObjectType::HASH_SET ||
            $subType === CollectionObjectType::LINKED_HASH_SET;
    }
}
