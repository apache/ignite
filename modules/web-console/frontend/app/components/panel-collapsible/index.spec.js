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

import 'mocha';
import {assert} from 'chai';
import angular from 'angular';
import {spy} from 'sinon';
import componentModule from './index';

suite('panel-collapsible', () => {
    const ICON_COLLAPSE = 'collapse';
    const ICON_EXPAND = 'expand';
    /** @type {ng.IScope} */
    let $scope;
    /** @type {ng.ICompileService} */
    let $compile;
    angular.module('test', [componentModule.name]);

    const isClosed = (el) => el[0].querySelector('.panel-collapsible__content').classList.contains('ng-hide');
    const click = (el) => el[0].querySelector('.panel-collapsible__status-icon').click();
    const getIcon = (el) => (
        el[0]
            .querySelector('.panel-collapsible__status-icon [ignite-icon]:not(.ng-hide)')
            .getAttribute('ignite-icon')
    );

    setup(() => {
        angular.module('test', [componentModule.name]);
        angular.mock.module('test');
        angular.mock.inject((_$rootScope_, _$compile_) => {
            $compile = _$compile_;
            $scope = _$rootScope_.$new();
        });
    });

    test('Required slot', () => {
        const el = angular.element(`<panel-collapsible></panel-collapsible>`);
        assert.throws(
            () => $compile(el)($scope),
            /Required transclusion slot `content` was not filled/,
            'Throws when panel-content slot was not filled'
        );
    });

    test('Open/close', () => {
        $scope.opened = false;
        const onOpen = $scope.onOpen = spy();
        const onClose = $scope.onClose = spy();
        const el = angular.element(`
            <panel-collapsible
                opened="opened"
                on-open="onOpen()"
                on-close="onClose()"
            >
                <panel-content>Content</panel-content>
            </panel-collapsible>
        `);

        $compile(el)($scope);
        $scope.$digest();
        assert.equal(getIcon(el), ICON_EXPAND, `Shows ${ICON_EXPAND} icon when closed`);
        assert.ok(isClosed(el), 'Hides content by default');
        click(el);
        $scope.$digest();
        assert.equal(getIcon(el), ICON_COLLAPSE, `Shows ${ICON_COLLAPSE} icon when opened`);
        assert.notOk(isClosed(el), 'Shows content when clicked');
        click(el);
        $scope.$digest();
        assert.equal(onOpen.callCount, 1, 'Evaluates onOpen expression');
        assert.equal(onClose.callCount, 1, 'Evaluates onClose expression');
        $scope.opened = true;
        $scope.$digest();
        assert.notOk(isClosed(el), 'Uses opened binding to control visibility');
    });

    test('Slot transclusion', () => {
        const el = angular.element(`
            <panel-collapsible opened='::true'>
                <panel-title>Title {{$panel.opened}}</panel-title>
                <panel-description>Description {{$panel.opened}}</panel-description>
                <panel-actions>
                    <button
                        class='my-button'
                        ng-click='$panel.close()'
                    >Button {{$panel.opened}}</button>
                </panel-actions>
                <panel-content>Content {{$panel.opened}}</panel-content>
            </panel-collapsible>
        `);
        $compile(el)($scope);
        $scope.$digest();
        assert.equal(
            el[0].querySelector('panel-title').textContent,
            'Title true',
            'Transcludes title slot and exposes $panel controller'
        );
        assert.equal(
            el[0].querySelector('panel-description').textContent,
            'Description true',
            'Transcludes Description slot and exposes $panel controller'
        );
        assert.equal(
            el[0].querySelector('panel-content').textContent,
            'Content true',
            'Transcludes content slot and exposes $panel controller'
        );
        el[0].querySelector('.my-button').click();
        $scope.$digest();
        assert.ok(isClosed(el), 'Can close by calling a method on exposed controller');
    });

    test('Disabled state', () => {
        const el = angular.element(`
            <panel-collapsible disabled='disabled'>
                <panel-content>Content</panel-content>
            </panel-collapsible>
        `);
        $compile(el)($scope);
        $scope.$digest();
        click(el);
        $scope.$digest();
        assert.ok(isClosed(el), `Can't be opened when disabled`);
        // TODO: test disabled styles
    });
});
