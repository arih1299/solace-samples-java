/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.solace.samples;

import com.solacesystems.jcsmp.*;

public class TopicPublisher {

    public static void main(String... args) throws JCSMPException {

        // Check command line arguments
        if (args.length < 2 || args[1].split("@").length != 2) {
            System.out.println("Usage: TopicPublisher <host:port> <client-username@message-vpn> [client-password]");
            System.out.println();
            System.exit(-1);
        }
        if (args[1].split("@")[0].isEmpty()) {
            System.out.println("No client-username entered");
            System.out.println();
            System.exit(-1);
        }
        if (args[1].split("@")[1].isEmpty()) {
            System.out.println("No message-vpn entered");
            System.out.println();
            System.exit(-1);
        }

        System.out.println("TopicPublisher initializing...");

        // Create a JCSMP Session
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, args[0]);     // host:port
        properties.setProperty(JCSMPProperties.USERNAME, args[1].split("@")[0]); // client-username
        properties.setProperty(JCSMPProperties.VPN_NAME,  args[1].split("@")[1]); // message-vpn
        if (args.length > 2) { 
            properties.setProperty(JCSMPProperties.PASSWORD, args[2]); // client-password
        }
        final JCSMPSession session =  JCSMPFactory.onlyInstance().createSession(properties);

        session.connect();

        String destination = "tutorial/topic";
        if (args.length > 3 && !args[3].isEmpty()) {
            destination = args[3];
        }

        final Topic topic = JCSMPFactory.onlyInstance().createTopic(destination);

        /**
         * Anonymous inner-class for handling publishing events
         */
        XMLMessageProducer prod = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
            @Override
            public void responseReceived(String messageID) {
                System.out.println("Producer received response for msg: " + messageID);
            }
            @Override
            public void handleError(String messageID, JCSMPException e, long timestamp) {
                System.out.printf("Producer received error for msg: %s@%s - %s%n",
                        messageID,timestamp,e);
            }
        });
        // Publish-only session is now hooked up and running!

        BytesMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
        final String text = createDataSize(1);
        msg.setData(text.getBytes());
        System.out.printf("Connected. About to send message '%s' to topic '%s'...%n",text,topic.getName());

        while (true) {
            System.out.println("Sending message");
            prod.send(msg, topic);
            try {
                Thread.sleep(250);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                System.out.println("Exiting.");
                session.closeSession();
            }
        }

    }

    /**
     * Creates a message of size @msgSize in KB.
     */
    private static String createDataSize(int msgSizeKB) {
        int msgSizeB = msgSizeKB * 1024;
        StringBuilder sb = new StringBuilder(msgSizeKB);
        for (int i=0; i<msgSizeB; i++) {
            sb.append('a');
        }
        return sb.toString();
    }
}
