

export default function() {
    return {
        restrict: 'C',
        link: (scope, $element) => {
            $element.contents()
                .filter(function() {
                    return this.nodeType === 3;
                })
                .wrap('<span></span>');
        }
    };
}
