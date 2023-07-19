

export default function() {
    return {
        restrict: 'A',
        controller: class {
            static $inject = ['$scope', '$attrs', '$sce', '$element', '$window', 'IgniteIcon'];

            /**
             * @param {ng.IScope} $scope     
             * @param {ng.IAttributes} $attrs     
             * @param {ng.ISCEService} $sce       
             * @param {JQLite} $element   
             * @param {ng.IWindowService} $window    
             * @param {import('./service').default} IgniteIcon 
             */
            constructor($scope, $attrs, $sce, $element, $window, IgniteIcon) {
                this.$scope = $scope;
                this.$attrs = $attrs;
                this.$sce = $sce;
                this.$element = $element;
                this.$window = $window;
                this.IgniteIcon = IgniteIcon;
            }

            $onInit() {
                this.off = this.$scope.$on('$locationChangeSuccess', (e, url) => {
                    this.render(this.getFragmentURL(url));
                });

                this.wrapper = document.createElement('div');
            }

            $onDestroy() {
                this.$element = this.$window = this.wrapper = null;

                this.off();
            }

            $postLink() {
                /** @type {string} */
                this.name = this.$attrs.igniteIcon;
                this.$element.attr('viewBox', this.IgniteIcon.getIcon(this.name).viewBox);

                this.render(this.getFragmentURL());
            }

            getFragmentURL(url = this.$window.location.href) {
                // All browsers except for Chrome require absolute URL of a fragment.
                // Combine that with base tag and HTML5 navigation mode and you get this.
                return `${url.split('#')[0]}#${this.name}`;
            }

            /**
             * @param {string} url 
             */
            render(url) {
                // templateNamespace: 'svg' does not work in IE11
                this.wrapper.innerHTML = `<svg><use xlink:href="${url}" href="${url}" /></svg>`;

                Array.from(this.wrapper.childNodes[0].childNodes).forEach((n) => {
                    this.$element.empty().append(n);
                });
            }
        }
    };
}
