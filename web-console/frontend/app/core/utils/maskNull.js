

// Filter that will check value and return `null` if needed.
export default function(val) {
    return _.isNil(val) ? 'null' : val;
}
