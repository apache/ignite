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

// Service for clone objects.
export default ['IgniteClone', ['$rootScope', '$q', '$modal', ($root, $q, $modal) => {
    const scope = $root.$new();

    let _names = [];
    let deferred;
    let _validator;

    function _nextAvailableName(name) {
        let num = 1;
        let tmpName = name;

        while (_.includes(_names, tmpName)) {
            tmpName = name + '_' + num.toString();

            num++;
        }

        return tmpName;
    }

    const cloneModal = $modal({templateUrl: '/templates/clone.html', scope, placement: 'center', show: false});

    scope.ok = function(newName) {
        if (!_validator || _validator(newName)) {
            deferred.resolve(_nextAvailableName(newName));

            cloneModal.hide();
        }
    };

    cloneModal.confirm = function(oldName, names, validator) {
        _names = names;

        scope.newName = _nextAvailableName(oldName);

        _validator = validator;

        deferred = $q.defer();

        cloneModal.$promise.then(cloneModal.show);

        return deferred.promise;
    };

    return cloneModal;
}]];
