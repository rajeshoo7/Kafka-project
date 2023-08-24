package com.rajtechie.service;


import com.rajtechie.dto.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> template;

    public void sendMessageToTopic(String message) {
        CompletableFuture<SendResult<String, Object>> future = template.send ("rajtechie-topic", message);

        // future.get (); // block the sending the thread and get result about the sent the message this is synchronous // slows down the

        // to do this asynchronous call back implementation

        future.whenComplete ((result, ex) -> {
            if (ex == null) {
                System.out.println ("Sent message=[ " + message + "] with offset=[" + result.getRecordMetadata ().offset () + " ]");
            } else {
                System.out.println ("Unable to send message=[" + message + "] due to : " + ex.getMessage ());
            }

        });
    }
        public void sendEventsToTopic(Customer customer)
        {


            // future.get (); // block the sending the thread and get result about the sent the message this is synchronous // slows down the

            // to do this asynchronous call back implementation

            try {
                CompletableFuture<SendResult<String, Object>> future = template.send ("rajtechie-topic", customer);
                future.whenComplete ((result, ex) -> {
                    if(ex == null) {
                        System.out.println ("Sent message=[ " + customer.toString()+ "] with offset=[" + result.getRecordMetadata ().offset () + " ]");
                    } else
                    {
                        System.out.println ("Unable to send message=[" + customer.toString() + "] due to : " + ex.getMessage ());
                    }

                });
            }catch (Exception ex)
            {
                System.out.println ("ERROR:  "+ex.getMessage ());
            }


    }




}
