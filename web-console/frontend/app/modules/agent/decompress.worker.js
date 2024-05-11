

/** This worker parse to JSON from string. */
// eslint-disable-next-line no-undef
onmessage = function(e) {
    const data = e.data;

    const res = JSON.parse(data.replace(/([:,\[])(-?[0-9]{15,}(?:\.\d+)?)/g, '$1"$2"'));

    postMessage(res);
};
