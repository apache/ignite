package io.stuar.test;

import org.eclipse.paho.client.mqttv3.*;
import org.junit.Test;

import java.nio.charset.StandardCharsets;


public class MqttClientTest {
    private String brokerUrl = "tcp://localhost:1883";

    @Test
    public void testBasic() throws MqttException {

        MqttClient client = new MqttClient(brokerUrl,MqttClient.generateClientId());

        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName("admin");
        options.setPassword("stuart".toCharArray());
        client.connect(options);

        MqttMessage msg = new MqttMessage();
        msg.setPayload("hello!".getBytes(StandardCharsets.UTF_8));
        client.publish("test",msg);

        try{
            client.disconnect();
            client.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }


    }
}
