angular
.module('ignite-web-console.navbar', [

])
.provider('igniteNavbar', function() {

    var items = []

    this.push = function(data) {
        items.push(data);
    }

    this.$get = [function() {
        return items;
    }]
})
.directive('igniteNavbar', function(igniteNavbar) {

    function controller() {
        var ctrl = this;

        ctrl.items = igniteNavbar;
        console.log(ctrl.items)
    }

    return {
        restrict: 'A',
        controller: controller,
        controllerAs: 'navbar'
    }
})