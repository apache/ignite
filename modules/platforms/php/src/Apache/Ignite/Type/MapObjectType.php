<?php
/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache\Ignite\Type;

use Apache\Ignite\Exception\ClientException;
use Apache\Ignite\Internal\Utils\ArgumentChecker;
use Apache\Ignite\Internal\Binary\BinaryUtils;

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
     * Optionally specifies the map subtype and Ignite types of keys and values in the map.
     *
     * If the map subtype is not specified, MapObjectType::HASH_MAP is assumed.
     *
     * If Ignite type is not specified for the key and/or value then during operations the Ignite client
     * tries to make automatic mapping between PHP types and Ignite object types -
     * according to the mapping table defined in the description of the ObjectType class.
     * 
     * @param int $subType map subtype, one of @ref MapSubType constants.
     * @param int|ObjectType|null $keyType Ignite type of the keys in the map:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     * @param int|ObjectType|null $valueType Ignite type of the values in the map:
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
     * Returns the map subtype, one of @ref MapSubType constants.
     * 
     * @return int map subtype, one of @ref MapSubType constants.
     */
    public function getSubType(): int
    {
        return $this->subType;
    }
    
    /**
     * Returns Ignite type of the keys in the map.
     * 
     * @return int|ObjectType|null type of the keys in the map:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null that means the type is not specified
     */
    public function getKeyType()
    {
        return $this->keyType;
    }

    /**
     * Returns Ignite type of the values in the map.
     * 
     * @return int|ObjectType|null type of the values in the map:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null that means the type is not specified
     */
    public function getValueType()
    {
        return $this->valueType;
    }
}
