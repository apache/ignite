

import angular from 'angular';
import _ from 'lodash';

/**
 * Special decorator that fix problem in AngularStrap selectAll / deselectAll methods.
 * If this problem will be fixed in AngularStrap we can remove this delegate.
 */
export default angular.module('mgcrea.ngStrap.select')
    .decorator('$select', ['$delegate', function($delegate) {
        function SelectFactoryDecorated(element, controller, config) {
            const delegate = $delegate(element, controller, config);

            // Common vars.
            const options = Object.assign({}, $delegate.defaults, config);

            const scope = delegate.$scope;

            const valueByIndex = (index) => {
                if (_.isUndefined(scope.$matches[index]))
                    return null;

                return scope.$matches[index].value;
            };

            const selectAll = (active) => {
                const selected = [];

                scope.$apply(() => {
                    for (let i = 0; i < scope.$matches.length; i++) {
                        if (scope.$isActive(i) === active) {
                            selected[i] = scope.$matches[i].value;

                            delegate.activate(i);

                            controller.$setViewValue(scope.$activeIndex.map(valueByIndex));
                        }
                    }
                });

                // Emit events.
                for (let i = 0; i < selected.length; i++) {
                    if (selected[i])
                        scope.$emit(options.prefixEvent + '.select', selected[i], i, delegate);
                }
            };

            scope.$selectAll = () => {
                scope.$$postDigest(selectAll.bind(this, false));
            };

            scope.$selectNone = () => {
                scope.$$postDigest(selectAll.bind(this, true));
            };

            return delegate;
        }

        SelectFactoryDecorated.defaults = $delegate.defaults;

        return SelectFactoryDecorated;
    }]);
