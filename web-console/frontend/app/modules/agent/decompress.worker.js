

/** This worker parse to JSON from string. */
// eslint-disable-next-line no-undef
onmessage = function(e) {
    const data = e.data;
    // const data2 = data.replace(/([:,\[])(-?[0-9]{15,}(?:\.\d+)?)/g, '$1"$2"');
    const res = JSON.parse(data);

    postMessage(res);
};
