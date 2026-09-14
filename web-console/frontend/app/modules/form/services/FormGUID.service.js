

export default function() {
    let guid = 0;

    return () => `form-field-${guid++}`;
}
