package kafka;

import org.apache.kafka.clients.consumer.*;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "node002:9092,node003:9092,node004:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                IntegerDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"group-1");

        Consumer<String,Integer> consumer = new KafkaConsumer<String, Integer>(properties);

        consumer.subscribe(Arrays.asList("topictest"));
        while (true){
            ConsumerRecords<String,Integer> records = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<String,Integer> record : records) {
                System.out.println("key:"+record.key()+"\tvalue:"+record.value()+
                        "\tparitition:"+record.partition()+"\toffset"+record.offset());
            }
        }


    }
}
