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

export default function pcbScaleNumber() {
    return {
        link(scope, el, attr, ngModel) {
            let factor;
            const ifVal = (fn) => (val) => val ? fn(val) : val;
            const wrap = (target) => (fn) => (value) => target(fn(value));
            const up = ifVal((v) => v / factor);
            const down = ifVal((v) => v * factor);

            ngModel.$formatters.unshift(up);
            ngModel.$parsers.push(down);
            ngModel.$validators.min = wrap(ngModel.$validators.min)(up);
            ngModel.$validators.max = wrap(ngModel.$validators.max)(up);

            scope.$watch(attr.pcbScaleNumber, (value, old) => {
                factor = Number(value);

                if (!ngModel.$viewValue)
                    return;

                ngModel.$setViewValue(ngModel.$viewValue * Number(old) / Number(value));

                ngModel.$render();
            });
        },
        require: 'ngModel'
    };
}
