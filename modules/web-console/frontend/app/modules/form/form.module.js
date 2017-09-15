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

// Fields styles.
import './field/field.scss';
import './field/feedback.scss';
import './field/input/text.scss';

// Panel.
import igniteFormPanel from './panel/panel.directive';
import igniteFormPanelField from './panel/field.directive';
import igniteFormPanelChevron from './panel/chevron.directive';
import igniteFormRevert from './panel/revert.directive';

// Field.
import igniteFormFieldLabel from './field/label.directive';
import igniteFormFieldTooltip from './field/tooltip.directive';
import placeholder from './field/bs-select-placeholder.directive';

// Group.
import igniteFormGroupAdd from './group/add.directive';
import igniteFormGroupTooltip from './group/tooltip.directive';

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
import igniteFormControlFeedback from './field/form-control-feedback.directive';
import igniteFormFieldUp from './field/up.directive';
import igniteFormFieldDown from './field/down.directive';

import IgniteFormGUID from './services/FormGUID.service.js';

angular
.module('ignite-console.Form', [

])
// Panel.
.directive(...igniteFormPanel)
.directive(...igniteFormPanelField)
.directive(...igniteFormPanelChevron)
.directive(...igniteFormRevert)
// Field.
.directive(...igniteFormFieldLabel)
.directive(...igniteFormFieldTooltip)
.directive(...placeholder)
// Group.
.directive(...igniteFormGroupAdd)
.directive(...igniteFormGroupTooltip)
// Validators.
.directive(...ipaddress)
.directive(...javaKeywords)
.directive(...javaPackageSpecified)
.directive(...javaBuiltInClass)
.directive(...javaIdentifier)
.directive(...javaPackageName)
.directive(...propertyValueSpecified)
.directive(...propertyUnique)
.directive(...unique)
.directive(...uuid)
// Helpers.
.directive(...igniteFormFieldInputAutofocus)
.directive(...igniteFormControlFeedback)
.directive(...igniteFormFieldUp)
.directive(...igniteFormFieldDown)

// Generator of globally unique identifier.
.service('IgniteFormGUID', IgniteFormGUID);
