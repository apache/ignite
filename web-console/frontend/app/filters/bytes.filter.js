

export default () => {
    /**
     * @param {number} bytes
     * @param {number} [precision]
     */
    const filter = (bytes, precision) => {
        if (bytes === 0)
            return '0 bytes';

        if (isNaN(bytes) || !isFinite(bytes))
            return '-';

        if (typeof precision === 'undefined')
            precision = 1;

        const units = ['bytes', 'kB', 'MB', 'GB', 'TB'];
        const number = Math.floor(Math.log(bytes) / Math.log(1024));

        return (bytes / Math.pow(1024, Math.floor(number))).toFixed(precision) + ' ' + units[number];
    };

    return filter;
};
