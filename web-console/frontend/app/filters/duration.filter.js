

export default () => {
    /**
     * @param {number} t Time in ms.
     * @param {string} dflt Default value.
     */
    const filter = (t, dflt = '0') => {
        if (t === 9223372036854775807)
            return 'Infinite';

        if (t <= 0)
            return dflt;

        const a = (i, suffix) => i && i !== '00' ? i + suffix + ' ' : '';

        const cd = 24 * 60 * 60 * 1000;
        const ch = 60 * 60 * 1000;
        const cm = 60 * 1000;
        const cs = 1000;

        const d = Math.floor(t / cd);
        const h = Math.floor((t - d * cd) / ch);
        const m = Math.floor((t - d * cd - h * ch) / cm);
        const s = Math.floor((t - d * cd - h * ch - m * cm) / cs);
        const ms = Math.round(t % 1000);

        return a(d, 'd') + a(h, 'h') + a(m, 'm') + a(s, 's') + (t < 1000 || (t < cm && ms !== 0) ? ms + 'ms' : '');
    };

    return filter;
};
