

// Directive to enable validation to match specified value.
export default function() {
    return {
        require: {
            ngModel: 'ngModel'
        },
        scope: false,
        bindToController: {
            igniteMatch: '<'
        },
        controller: class {
            /** @type {ng.INgModelController} */
            ngModel;
            /** @type {string} */
            igniteMatch;

            $postLink() {
                this.ngModel.$overrideModelOptions({allowInvalid: true});
                this.ngModel.$validators.mismatch = (value) => value === this.igniteMatch;
            }

            /**
             * @param {{igniteMatch: ng.IChangesObject<string>}} changes
             */
            $onChanges(changes) {
                if ('igniteMatch' in changes) this.ngModel.$validate();
            }
        }
    };
}
