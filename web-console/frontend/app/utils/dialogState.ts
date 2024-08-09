

import {UIRouter, StateDeclaration, Transition} from '@uirouter/angularjs';

export function dialogState(component: string): Partial<StateDeclaration> {
    let dialog: mgcrea.ngStrap.modal.IModal | undefined;
    let hide: (() => void) | undefined;

    onEnter.$inject = ['$transition$'];

    function onEnter(transition: Transition) {
        const modal = transition.injector().get<mgcrea.ngStrap.modal.IModalService>('$modal');
        const router = transition.injector().get<UIRouter>('$uiRouter');

        dialog = modal({
            template: `
                <${component}
                    class='modal center modal--ignite theme--ignite'
                    tabindex='-1'
                    role='dialog'
                    on-hide=$hide()
                ></${component}>
            `,
            onHide(modal) {
                modal.destroy();
            }
        });

        hide = dialog.hide;

        dialog.hide = () => router.stateService.go('.^');
    }

    return {
        onEnter,
        onExit() {
            if (hide) hide();
            dialog = hide = void 0;
        }
    };
}
