

import template from './template.pug';
import './style.scss';

export class ModalImportModelsStepIndicator<T> {
    steps: Array<{value: T, label: string}>;

    currentStep: T;

    isVisited(index: number) {
        return index <= this.steps.findIndex((step) => step.value === this.currentStep);
    }
}

export const component = {
    template,
    controller: ModalImportModelsStepIndicator,
    bindings: {
        steps: '<',
        currentStep: '<'
    }
};
