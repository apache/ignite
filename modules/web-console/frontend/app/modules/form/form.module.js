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

import angular from 'angular';

// Field.
import placeholder from './field/bs-select-placeholder.directive';

// Validators.
import ipaddress from './validator/ipaddress.directive';
import javaKeywords from './validator/java-keywords.directive';
import javaPackageSpecified from './validator/java-package-specified.directive';
import javaBuiltInClass from './validator/java-built-in-class.directive';
import javaIdentifier from './validator/java-identifier.directive';
import javaPackageName from './validator/java-package-name.directive';
import propertyValueSpecified from './validator/property-value-specified.directive';
import propertyUnique from './validator/property-unique.directive';
import unique from './validator/unique.directive';
import uuid from './validator/uuid.directive';

// Helpers.
import igniteFormFieldInputAutofocus from './field/input/autofocus.directive';
import IgniteFormGUID from './services/FormGUID.service.js';

angular
.module('ignite-console.Form', [

])
// Field.
.directive('bsSelect', placeholder)
// Validators.
.directive('ipaddress', ipaddress)
.directive('javaKeywords', javaKeywords)
.directive('javaPackageSpecified', javaPackageSpecified)
.directive('javaBuiltInClass', javaBuiltInClass)
.directive('javaIdentifier', javaIdentifier)
.directive('javaPackageName', javaPackageName)
.directive('ignitePropertyValueSpecified', propertyValueSpecified)
.directive('ignitePropertyUnique', propertyUnique)
.directive('igniteUnique', unique)
.directive('uuid', uuid)
// Helpers.
.directive('igniteFormFieldInputAutofocus', igniteFormFieldInputAutofocus)

// Generator of globally unique identifier.
.service('IgniteFormGUID', IgniteFormGUID);
