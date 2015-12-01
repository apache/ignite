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

export default ['$scope', ($scope) => {
    $scope.javaClassItems = [
        {label: 'snippet', value: 1},
        {label: 'factory class', value: 2}
    ];

    $scope.onLoad = (editor) => {
        editor.setReadOnly(true);
        editor.setOption('highlightActiveLine', false);
        editor.setAutoScrollEditorIntoView(true);
        editor.$blockScrolling = Infinity;

        var renderer = editor.renderer;

        renderer.setHighlightGutterLine(false);
        renderer.setShowPrintMargin(false);
        renderer.setOption('fontFamily', 'monospace');
        renderer.setOption('fontSize', '12px');
        renderer.setOption('minLines', '25');
        renderer.setOption('maxLines', '25');

        editor.setTheme('ace/theme/chrome');
	}

    $scope.javaClassServer = 1;

    $scope.$watch('javaClassServer', () => {
        $scope.javaServer = $generatorJava.cluster($scope.selectedItem,
            $scope.configServer.javaClassServer === 2 ? 'ServerConfigurationFactory' : false, null, false);
    })
}]