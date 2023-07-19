

export class CancellationError extends Error {
    constructor(message = 'Cancelled by user') {
        super(message);

        // Workaround for Babel issue with extend: https://github.com/babel/babel/issues/3083#issuecomment-315569824
        this.constructor = CancellationError;

        // eslint-disable-next-line no-proto
        this.__proto__ = CancellationError.prototype;
    }
}
