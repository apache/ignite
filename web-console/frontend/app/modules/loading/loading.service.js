

export default function() {
    const _overlays = {};

    /**
     * @param {string} key
     */
    const start = (key) => {
        setTimeout(() => {
            const loadingOverlay = _overlays[key];

            loadingOverlay && loadingOverlay.addClass('loading-active');
        });
    };

    /**
     * @param {string} key
     */
    const finish = (key) => {
        setTimeout(() => {
            const loadingOverlay = _overlays[key];

            loadingOverlay && loadingOverlay.removeClass('loading-active');
        });
    };

    const add = (key, element) => {
        _overlays[key] = element;

        return element;
    };

    return {
        add,
        start,
        finish
    };
}
