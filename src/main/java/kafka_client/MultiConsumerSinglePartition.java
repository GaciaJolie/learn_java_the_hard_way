package kafka_client;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

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

        KafkaConsumer<String, String> consumer;
        String name;

        MyListener(KafkaConsumer<String, String> consumer, String name) {
            this.consumer = consumer;
            this.name = name;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
            for(TopicPartition partition : partitions){
                System.out.println("revoke " + name + " from partition " + partition.partition());
                System.out.println("commit partition " + partition.partition() + " offset " +consumer.position(partition));
                map.put(partition, new OffsetAndMetadata(consumer.position(partition)));
            }
            consumer.commitSync(map);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for(TopicPartition partition : partitions){
                System.out.println("assign partition " + partition.partition() + " to " + name);
            }
        }
    }


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "47.100.49.129:9092");
        props.put("group.id", "mcsp2");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        String name = "consumer2";
        consumer.subscribe(Arrays.asList("muti_part"), new MyListener(consumer, name));
        new Thread(new MyConsumer(consumer, name)).start();
    }
}
