

export default function() {
    return function(val?: string) {
        return typeof val === 'string' ? val.replace(/(<\/?\w+>)/igm, '') : '';
    };
}
