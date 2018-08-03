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

use Apache\Ignite\Impl\Utils\ArgumentChecker;
use Apache\Ignite\Impl\Binary\BinaryUtils;

/**
 * Class representing a complex type of Ignite object.
 *
 * It is described by ObjectType::COMPLEX_OBJECT and by a name of PHP Class which is mapped to/from
 * the Ignite complex type.
 */
class ComplexObjectType extends ObjectType
{
    private $phpClassName;
    private $typeName;
    private $fieldTypes;
    
    /**
     * Public constructor.
     *
     * Specifies a name of PHP Class which will be mapped to/from the complex type.
     *
     * If an object of the complex type is going to be received (deserialized),
     * the PHP Class must have a constructor without parameters or with optional parameters only.
     *
     * By default, the fields have no types specified. It means during operations the Ignite client
     * will try to make automatic mapping between PHP types and Ignite object types -
     * according to the mapping table defined in the description of the ObjectType class.
     *
     * A type of any field may be specified later by setFieldType() method.
     *
     * The name of the complex type is the name of the PHP Class.
     * 
     * @return ComplexObjectType new ComplexObjectType instance.
     */
    public function __construct()
    {
        parent::__construct(ObjectType::COMPLEX_OBJECT);
        $this->phpClassName = null;
        $this->typeName = null;
        $this->fieldTypes = [];
    }
    
    /**
     * Specifies a type of the field in the complex type.
     *
     * If the type is not specified then during operations the Ignite client
     * will try to make automatic mapping between PHP types and Ignite object types -
     * according to the mapping table defined in the description of the ObjectType class.
     * 
     * @param string $fieldName name of the field.
     * @param int|ObjectType|null $fieldType type of the field:
     *   - either a type code of primitive (simple) type (@ref PrimitiveTypeCodes)
     *   - or an instance of class representing non-primitive (composite) type
     *   - or null (or not specified) that means the type is not specified
     *
     * @return ComplexObjectType the same instance of the ComplexObjectType.
     *
     * @throws Exception::ClientException if error.
     */
    public function setFieldType(string $fieldName, $fieldType): ComplexObjectType
    {
        BinaryUtils::checkObjectType($fieldType, 'fieldType');
        $this->fieldTypes[$fieldName] = $fieldType;
        return $this;
    }

    /**
     * 
     * @param string $fieldName
     * @return int|ObjectType|null
     */
    public function getFieldType(string $fieldName)
    {
        if (array_key_exists($fieldName, $this->fieldTypes)) {
            return $this->fieldTypes[$fieldName];
        }
        return null;
    }
    
    /**
     * 
     * @param string $phpClassName
     */
    public function setPhpClassName(?string $phpClassName): void
    {
        $this->phpClassName = $phpClassName;
    }
    
    /**
     * 
     * @return string
     */
    public function getPhpClassName(): ?string
    {
        return $this->phpClassName;
    }
    
    /**
     * 
     * @param string $typeName
     */
    public function setTypeName(?string $typeName): void
    {
        $this->typeName = $typeName;
    }

    /**
     * 
     * @return string
     */
    public function getTypeName(): ?string
    {
        return $this->typeName;
    }
}
