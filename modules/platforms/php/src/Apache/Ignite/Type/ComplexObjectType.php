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
use Apache\Ignite\Internal\Binary\BinaryUtils;

/**
 * Class representing a complex type of Ignite object.
 *
 * It corresponds to the ObjectType::COMPLEX_OBJECT Ignite type code.
 *
 * This class may be needed to help Ignite client to:
 *   - deserialize an Ignite complex object to a PHP object when reading data,
 *   - serialize a PHP object to an Ignite complex object when writing data.
 *
 * Note: only public properties of PHP objects can be serialized/deserialized.
 */
class ComplexObjectType extends ObjectType
{
    private $phpClassName;
    private $typeName;
    private $fieldTypes;
    
    /**
     * Public constructor.
     *
     * Creates a default representation of Ignite complex object type.
     * setPhpClassName(), setIgniteTypeName(), setFieldType() methods may be used
     * to change the default representation.
     */
    public function __construct()
    {
        parent::__construct(ObjectType::COMPLEX_OBJECT);
        $this->phpClassName = null;
        $this->typeName = null;
        $this->fieldTypes = [];
    }
    
    /**
     * Sets the name of the PHP class.
     *
     * Affects data reading operations only.
     *
     * The specified name will be used as PHP class name to instantiate a PHP object
     * the received Ignite complex object is deserialized to.
     *
     * By default (if the name is not specified), the Ignite complex type name of the received complex object
     * is used as PHP class name to instantiate a PHP object during deserialization.
     *
     * The PHP Class must have a constructor without parameters or with optional parameters only.
     *
     * @param string|null $phpClassName name of the PHP class or null (the name is not specified).
     * 
     * @return ComplexObjectType the same instance of the ComplexObjectType.
     */
    public function setPhpClassName(?string $phpClassName): ComplexObjectType
    {
        $this->phpClassName = $phpClassName;
        return $this;
    }
    
    /**
     * Gets the name of the PHP class.
     *
     * @return string|null name of the PHP class or null (the name is not specified).
     */
    public function getPhpClassName(): ?string
    {
        return $this->phpClassName;
    }
    
    /**
     * Sets the name of the Ignite complex type.
     *
     * Affects data writing operations only.
     *
     * The specified name will be used as the Ignite complex type name during the complex object's writing operations.
     *
     * By default (if the name is not specified), the Ignite complex type name of the serialized complex object
     * is taken from the name of the PHP class which instance is provided in a data writing operation.
     *
     * @param string|null $typeName name of the Ignite complex type or null (the name is not specified).
     * 
     * @return ComplexObjectType the same instance of the ComplexObjectType.
     */
    public function setIgniteTypeName(?string $typeName): ComplexObjectType
    {
        $this->typeName = $typeName;
        return $this;
    }

    /**
     * Gets the name of the Ignite complex type.
     * 
     * @return string|null name of the Ignite complex type or null (the name is not specified).
     */
    public function getIgniteTypeName(): ?string
    {
        return $this->typeName;
    }

    /**
     * Specifies Ignite type of the indicated field in the complex type.
     *
     * Affects data writing operations only.
     *
     * During data serialization Ignite client will assume that the indicated field
     * has the Ignite type specified by this method.
     *
     * By default (if the type of the field is not specified),
     * Ignite client tries to make automatic mapping between PHP types and Ignite object types -
     * according to the mapping table defined in the description of the ObjectType class.
     * 
     * @param string $fieldName name of the field.
     * @param int|ObjectType|null $fieldType Ignite type of the field:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     *
     * @return ComplexObjectType the same instance of the ComplexObjectType.
     *
     * @throws ClientException if error.
     */
    public function setFieldType(string $fieldName, $fieldType): ComplexObjectType
    {
        BinaryUtils::checkObjectType($fieldType, 'fieldType');
        $this->fieldTypes[$fieldName] = $fieldType;
        return $this;
    }

    /**
     * Gets Ignite type of the indicated field in the complex type.
     *
     * @param string $fieldName name of the field.
     *
     * @return int|ObjectType|null Ignite type of the field:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null that means the type is not specified
     */
    public function getFieldType(string $fieldName)
    {
        if (array_key_exists($fieldName, $this->fieldTypes)) {
            return $this->fieldTypes[$fieldName];
        }
        return null;
    }
}
