//tag::conf2[]
const IgniteClient = require('apache-ignite-client');
const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;

const igniteClientConfiguration = new IgniteClientConfiguration('127.0.0.1:10800')
    .setUserName('ignite')
    .setPassword('ignite')
    .setConnectionOptions(false, {'timeout': 0});
//end::conf2[]

const igniteClient = new IgniteClient(function onStateChanged(state, reason) {
    if (state === IgniteClient.STATE.CONNECTED) {
        console.log('Client is started');
    } else if (state === IgniteClient.STATE.DISCONNECTED) {
        console.log('Client is stopped');
        if (reason) {
            console.log(reason);
        }
    }
});
igniteClient.connect(igniteClientConfiguration).then(() => igniteClient.disconnect());
