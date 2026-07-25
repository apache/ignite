

import angular from 'angular';
import _ from 'lodash';

/**
 * Decorator that fix problem in AngularStrap $tooltip.
 */
export default angular
    .module('mgcrea.ngStrap.tooltip')
    /**
     * Don't hide tooltip when mouse move from element to tooltip.
     */
    .decorator('$tooltip', ['$delegate', function($delegate) {
        function TooltipFactoryDecorated(element, config) {
            let tipElementEntered = false;

            config.onShow = ($tooltip) => {
                // Workaround for tooltip detection.
                if ($tooltip.$element && $tooltip.$options.trigger === 'click hover') {
                    $tooltip.$element.on('mouseenter', () => tipElementEntered = true);
                    $tooltip.$element.on('mouseleave', () => {
                        tipElementEntered = false;

                        $tooltip.leave();
                    });
                }
            };

            const $tooltip = $delegate(element, config);

            const scope = $tooltip.$scope;
            const options = $tooltip.$options;

            const _hide = $tooltip.hide;

            $tooltip.hide = (blur) => {
                if (!$tooltip.$isShown || tipElementEntered)
                    return;

                if ($tooltip.$element) {
                    $tooltip.$element.off('mouseenter');
                    $tooltip.$element.off('mouseleave');

                    return _hide(blur);
                }

                scope.$emit(options.prefixEvent + '.hide.before', $tooltip);

                if (!_.isUndefined(options.onBeforeHide) && _.isFunction(options.onBeforeHide))
                    options.onBeforeHide($tooltip);

                $tooltip.$isShown = scope.$isShown = false;
                scope.$$phase || (scope.$root && scope.$root.$$phase) || scope.$digest();
            };

            return $tooltip;
        }

        return TooltipFactoryDecorated;
    }])
    /**
     * Set width for dropdown as for element.
     */
    .decorator('$tooltip', ['$delegate', ($delegate) => {
        return function(el, config) {
            const $tooltip = $delegate(el, config);

            $tooltip.$referenceElement = el;
            $tooltip.destroy = _.flow($tooltip.destroy, () => $tooltip.$referenceElement = null);
            $tooltip.$applyPlacement = _.flow($tooltip.$applyPlacement, () => {
                if (!$tooltip.$element)
                    return;

                const refWidth = $tooltip.$referenceElement[0].getBoundingClientRect().width;
                const elWidth = $tooltip.$element[0].getBoundingClientRect().width;

                if (refWidth > elWidth) {
                    $tooltip.$element.css({
                        width: refWidth,
                        maxWidth: 'initial'
                    });
                }
            });

            return $tooltip;
        };
    }]);
