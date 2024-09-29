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
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class MqttPublishSample {

	private static IMqttClient client ;


	public static void main(String[] args) {

                //broker
		String broker = "tcp://localhost:1883";
                //randomly generated client UUID
		String clientId = UUID.randomUUID().toString();

		try {

                        //create a new publisher instance, connecting to the MQTT broker with
                        //the unique clientId generated above, and create a persistent session
                        client = new MqttClient(broker,clientId,new MemoryPersistence());

                        MqttConnectOptions connOpts = new MqttConnectOptions();

			//if cleanSession is true before connecting the client, 
			//then all pending publication deliveries for the client are removed 
			//when the client connects.
			connOpts.setCleanSession(false);
                        
                        //set automatic reconnect in case of disconnection
                        connOpts.setAutomaticReconnect(true);
                        //set a connection timeout of 10 seconds
                        connOpts.setConnectionTimeout(60);

                        //retry interval for Keep-alive transmissions - how long to keep connection alive
                        //connection will send ping every 30 seconds for max amount of times defined in TCP parameter in the TCP adapter settings
			connOpts.setKeepAliveInterval(60);
			
			/*
			 * connect
			 */
			System.out.println("Connecting to broker: " + broker);
			client.connect(connOpts);
			System.out.println("Connected");


			/*
			 * sending messages
                        ```QOS: 0 at most once/fire and forget, 1 at least once, use if duplicates can be handled
                        ```2 exactly once, when duplicates cannot be handled and no loss is acceptable
			 */
                        
                        //if client is not connected, no sense to keep going
                        if(!client.isConnected()){
                            System.out.println("No connection");
                        }
                        //otherwise invoke publishmessage method
                        else{

                            publishMessage("news/ireland/weather/current", "rain");
                            
                            publishMessage("news/ireland/weather/forecast", "more rain");

                            publishMessage("news/netherlands/government", "who even knows anymore");          
                        }
			

			/*
			 * disconnect
			 */        		   
			client.disconnect();

			System.out.println("Disconnected");
			
			client.close();
	        
			System.exit(0);

		} catch (MqttException me) {
			System.out.println("reason " + me.getReasonCode());
			System.out.println("msg " + me.getMessage());
			System.out.println("loc-msg " + me.getLocalizedMessage());
			System.out.println("cause " + me.getCause());
			System.out.println("exception " + me);
			me.printStackTrace();
		}

	}

	private static void publishMessage(String topic, String payload) {
                
		System.out.println("Publishing message: " + payload + " on topic "+ topic );            

		MqttMessage message = new MqttMessage(payload.getBytes());
		//messages to be retained, so that clients will not have to wait for a new message to be published to get
                //the latest message, and also do not need to be connected at all times to receive the message
                message.setRetained(true);
		
                //setting QoS to 1, at least once, as duplicate messages being published should be ok in this instance
                message.setQos(1);     

		try {
                        //publish message
			client.publish(topic, message);

		} catch (MqttException me) {
			System.out.println("reason " + me.getReasonCode());
			System.out.println("msg " + me.getMessage());
			System.out.println("loc-msg " + me.getLocalizedMessage());
			System.out.println("cause " + me.getCause());
			System.out.println("exception " + me);
			me.printStackTrace();
		}

		System.out.println("Message published");


	}

}