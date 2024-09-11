

export default function() {
    return {
        priority: 0,
        require: '^uiGrid',
        compile() {
            return {
                pre($scope, $element, attrs, uiGridCtrl) {
                    uiGridCtrl.grid.options.enableHovering = true;
                },
                post() { }
            };
        }
    };
}
