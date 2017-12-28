package kafka_client;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * ResetOffsetDemo
 *
 * @author lyq
 * @create 2017-12-28 13:51
 */
public class ResetOffsetDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.20.0.139:9092");
        props.put("group.id", "test2");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("foo", "bar"));
        while (true){

            //从订阅的主题中获取所有自上次offset提交到最新的数据
            ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            //获取消息的分区信息
            for(TopicPartition partition : records.partitions()){
                //获取指定分区得到的消息
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                for(ConsumerRecord<String, String> record : partitionRecords){
                    //显示该分区得到记录的偏移量和值
                    System.out.println(record.offset() + ", " + record.value());
                }
                //获取该分区上对于该消费者组的最近一条消息偏移量
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                //提交最新的偏移量
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
            }
        }
    }
}
