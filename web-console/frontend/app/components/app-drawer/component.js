
import angular from 'angular';
import template from './template.pug';
import './style.scss';

export default {
    template,
    controller: class Ctrl {
        static $inject = ['$element', '$state', '$timeout'];

        position = 'right';
        width = '600px';
        isOpen = false;
        onClose = () => {};

        constructor($element, $state, $timeout) {
            this.$element = $element;
            this.$state = $state;
            this.$timeout = $timeout;
        }

        $onInit() {
           
        }

        $onChanges(changes) {
            if (changes && changes.isOpen) {
                this.$timeout(() => {
                    // 添加/移除no-scroll类到body
                    angular.element(document.body).toggleClass('no-scroll', this.isOpen);
                });
            }
        }
        
        // 关闭抽屉
        close() {
            this.onClose();
        };

        // 阻止事件冒泡
        stopPropagation(event) {
            event.stopPropagation();
        };   
        
    },
    bindings: {
        isOpen: '<',         // 控制抽屉开关状态
        onClose: '&',        // 关闭抽屉时的回调
        position: '@',       // 抽屉位置，默认为'right'
        width: '@',          // 抽屉宽度，默认为'300px'
        title: '<'           // 抽屉标题
    },
    transclude: true,      // 允许内容投影
};
