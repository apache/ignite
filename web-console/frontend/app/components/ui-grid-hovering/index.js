

import angular from 'angular';

import uiGridCell from './cell';
import uiGridHovering from './hovering';
import uiGridViewport from './viewport';

import './style.scss';

export default angular
    .module('ignite-console.ui-grid-hovering', [])
    .directive('uiGridCell', uiGridCell)
    .directive('uiGridHovering', uiGridHovering)
    .directive('uiGridViewport', uiGridViewport);
