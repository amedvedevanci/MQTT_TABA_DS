/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.example.mqtt_taba_ds;

import java.util.UUID;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MqttSubscriberSample {

    public static void main(String[] args) {

        IMqttClient client ;
        //broker
        String broker = "tcp://localhost:1883";
        //randomly generated client UUID
        String clientId = UUID.randomUUID().toString();
        
        try{
           
           //create a new publisher instance, connecting to the MQTT broker with
           //the unique clientId generated above, and create a persistent session
           client = new MqttClient(broker,clientId,new MemoryPersistence());

           MqttConnectOptions connOpts = new MqttConnectOptions();
         
            //if cleanSession is true before connecting the client, 
            //then all pending publication deliveries for the client are removed 
            //when the client connects.
            connOpts.setCleanSession(false);
            
            client.setCallback(new SampleSubscriber());
            
            System.out.println("Connecting to broker: " + broker);
            client.connect(connOpts);
            System.out.println("Connected");
            
            //topics
            String topic1 = "news/ireland/weather/forecast";
            String topic2 = "news/ireland/#";
            String topic3 = "news/+/government/+";
            String topic4 = "news/#";
            
            //subscribe to topics, print confirmation. Can add QOS here
            client.subscribe(topic1,1);
            System.out.println("Subscribed to "+topic1);
            
            client.subscribe(topic2,2);
            System.out.println("Subscribed to "+topic2);
            
            client.subscribe(topic3);
            System.out.println("Subscribed to "+topic3);
            
            client.subscribe(topic4);
            System.out.println("Subscribed to "+topic4);
            
            //unsubscribe from topic
            client.unsubscribe(topic4);
               System.out.println("Unsubscribed from "+topic4);
               
            
        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("excep " + me);
            me.printStackTrace();
        }
    }
}
