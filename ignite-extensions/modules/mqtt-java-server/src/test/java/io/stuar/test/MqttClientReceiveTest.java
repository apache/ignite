package io.stuar.test;

import org.eclipse.paho.client.mqttv3.*;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


public class MqttClientReceiveTest {
    private String brokerUrl = "tcp://localhost:1883";

    @Test
    public void testBasicReceive() throws MqttException, IOException {

        MqttClient client = new MqttClient(brokerUrl,MqttClient.generateClientId());

        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName("admin");
        options.setPassword("stuart".toCharArray());
        client.connect(options);

        client.setCallback(new MqttCallback(){

            @Override
            public void connectionLost(Throwable throwable) {
                System.out.println(throwable);
            }

            @Override
            public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                System.out.println(s+":");
                System.out.println(new String(mqttMessage.getPayload(),StandardCharsets.UTF_8));
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
                System.out.println(iMqttDeliveryToken);
            }
        });

        client.subscribe("test");
        client.setTimeToWait(1000);

        System.in.read();
        try{
            client.disconnect();
            client.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }
}
