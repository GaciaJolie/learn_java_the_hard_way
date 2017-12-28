package kafka_client;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * MultiConsumerSinglePartition
 *
 * @author lyq
 * @create 2017-12-28 16:13
 */
public class MultiConsumerSinglePartition {
    static class MyConsumer implements Runnable{
        KafkaConsumer<String, String> consumer;
        int buffer;
        String name;

        MyConsumer(KafkaConsumer<String, String> consumer, String name){
            this.consumer = consumer;
            this.buffer = 0;
            this.name = name;
        }

        @Override
        public void run() {
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for(ConsumerRecord<String, String> record : records){
                    System.out.printf("name = %s, partition = %d, offset = %d, key = %s, value = %s%n", name,
                            record.partition(), record.offset(), record.key(), record.value());
                    buffer ++;
                }
                if(buffer >= 5){
                    consumer.commitSync();
                    buffer = 0;
                    System.out.println(name  + " commit");
                }
            }
        }
    }


    static class MyListener implements ConsumerRebalanceListener{



        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

        }
    }


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.20.0.139:9092");
        props.put("group.id", "mcsp");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("muti_part"));
        new Thread(new MyConsumer(consumer, "consumer2")).start();
    }
}
