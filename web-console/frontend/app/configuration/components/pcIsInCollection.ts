

class Controller<T> {
    ngModel: ng.INgModelController;
    items: T[];

    $onInit() {
        this.ngModel.$validators.isInCollection = (item) => {
            if (!item || !this.items)
                return true;

            return this.items.includes(item);
        };
    }

    $onChanges() {
        this.ngModel.$validate();
    }
}

export default function pcIsInCollection() {
    return {
        controller: Controller,
        require: {
            ngModel: 'ngModel'
        },
        bindToController: {
            items: '<pcIsInCollection'
        }
    };
}
