const IgniteClient = require('apache-ignite-client');
const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;

async function connectClient() {
    const igniteClient = new IgniteClient(onStateChanged);
    try {
        const igniteClientConfiguration = new IgniteClientConfiguration(
            '127.0.0.1:10800', '127.0.0.1:10801', '127.0.0.1:10802');
        // Connect to Ignite node
        await igniteClient.connect(igniteClientConfiguration);
    }
    catch (err) {
        console.log(err.message);
    }
}

function onStateChanged(state, reason) {
    if (state === IgniteClient.STATE.CONNECTED) {
        console.log('Client is started');
    }
    else if (state === IgniteClient.STATE.CONNECTING) {
        console.log('Client is connecting');
    }
    else if (state === IgniteClient.STATE.DISCONNECTED) {
        console.log('Client is stopped');
        if (reason) {
            console.log(reason);
        }
    }
}

connectClient();
