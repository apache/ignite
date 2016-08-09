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

// Panel.
import igniteFormPanel from './panel/panel.directive';
import igniteFormPanelChevron from './panel/chevron.directive';
import igniteFormRevert from './panel/revert.directive';

// Field.
import igniteFormField from './field/field.directive';
import igniteFormFieldLabel from './field/label.directive';
import igniteFormFieldTooltip from './field/tooltip.directive';
import igniteFormFieldDropdown from './field/dropdown.directive';
import igniteFormFieldInputNumber from './field/input/number.directive';
import igniteFormFieldInputText from './field/input/text.directive';
import igniteFormFieldInputCheckbox from './field/input/checkbox.directive';
import igniteFormFieldInputDatalist from './field/input/datalist.directive';

import placeholder from './field/bs-select-placeholder.directive';

// Group.
import igniteFormGroup from './group/group.directive';
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

// Helpers.
import igniteFormFieldInputAutofocus from './field/input/autofocus.directive';
import igniteFormFieldUp from './field/up.directive';
import igniteFormFieldDown from './field/down.directive';
import igniteFormControlFeedback from './field/form-control-feedback.directive';

angular
.module('ignite-console.Form', [

])
// Panel.
.directive(...igniteFormPanel)
.directive(...igniteFormPanelChevron)
.directive(...igniteFormRevert)
// Field.
.directive(...igniteFormField)
.directive(...igniteFormFieldLabel)
.directive(...igniteFormFieldTooltip)
.directive(...igniteFormFieldDropdown)
.directive(...igniteFormFieldInputNumber)
.directive(...igniteFormFieldInputText)
.directive(...igniteFormFieldInputCheckbox)
.directive(...igniteFormFieldInputDatalist)
.directive(...placeholder)
// Group.
.directive(...igniteFormGroup)
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
// Helpers.
.directive(...igniteFormFieldInputAutofocus)
.directive(...igniteFormFieldUp)
.directive(...igniteFormFieldDown)
.directive(...igniteFormControlFeedback)
// Generator of globally unique identifier.
.factory('IgniteFormGUID', [() => {
    let guid = 0;

    return () => `form-field-${guid++}`;
}]);
