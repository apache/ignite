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

namespace Apache\Ignite\ObjectType;

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
     * General collection type, which can not be mapped to any more specific collection type.
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
     * Specifies a kind of collection and optionally specifies a type of elements in the collection.
     *
     * If the type of elements is not specified then during operations the Ignite client
     * will try to make automatic mapping between PHP types and Ignite object types -
     * according to the mapping table defined in the description of the ObjectType class.
     * 
     * @param int $subType collection subtype, one of the CollectionObjectType constants.
     * @param int|ObjectType|null $elementType type of elements in the collection:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     * 
     * @throws Exception::ClientException if error.
     */
    public function __construct(int $subType, $elementType = null)
    {
        // TODO: check args
        parent::__construct(ObjectType::COLLECTION);
        $this->subType = $subType;
        $this->elementType = $elementType;
    }
}
