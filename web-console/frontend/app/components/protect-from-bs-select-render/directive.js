

export default function protectFromBsSelectRender() {
    return {
        link(scope, el, attr, ctrl) {
            const {$render} = ctrl;

            Object.defineProperty(ctrl, '$render', {
                set() {},
                get() {
                    return $render;
                }
            });
        },
        require: 'ngModel'
    };
}
