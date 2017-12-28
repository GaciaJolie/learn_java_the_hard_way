### Kafka Comsumer Client API 说明

##### Maven依赖

```java
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
    <version>1.0.0</version>
</dependency>
```

##### 最简单的实例

```java
public class DemoConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.20.0.139:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("foo", "bar"));
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(0);
            for(ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}
```

程序解释

* 配置项
  * bootstrap.servers：kafka集群中的一个就行，只要能与客户端通信
  * group.id：消费者组，所有消费者都必要归属于某一个消费者组，每一个消费者组对相同的Topic拥有不同的Offset，即互不影响，同属于一个消费者组的消费者平摊消息，即不重复接受
  * enable.auto.commit：offset偏移量按照指定时间间隔自动提交，此时，只要从Topic中读取了消息而且被自动提交了offset，就认为该消息已经被消费成功，而不管客户端在处理具体消息时是否正常处理，因此此种策略下客户端需要自行设计消息处理失败后的处理策略以防止消息丢失
  * auto.commit.interval.ms：offset自动提交的时间间隔
  * key.deserializer：消息的key值解序列化类，此处表明消息key值将被解析为字符串
  * value.deserializer：消息value值解序列化类，此处表明消息value值将被解析为字符串
* 根据配置创建消费者
* 指定消费者监听的Topic，可多个
* 循环处理以下任务
  * 从指定监听的Topic中拉取由上次提交的offset（含）到最新的数据
  * 处理获取得到的消息

##### 扩展一：取消自动提交offset，改为当消息处理完成时手动提交

```java
public class ManualConsumerDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.20.0.139:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");---------------------------------//改动1
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("foo", "bar"));
        final int minBatchSize = 200;---------------------------------------------//改动2
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.key() + ", " + record.value());
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                consumer.commitSync();-------------------------------------------//改动3
                buffer.clear();
            }
        }
    }
}
```

程序解释

* 改动1：把自动提交offset的配置修改为非自动提交
* 改动2：这里为了说明多消息的处理需要一个过程，把消息加入到缓存中，待缓存达到指定大小后，一次性处理这些缓存数据，比如存入数据库
* 改动3：手动提交offset

##### 扩展二：任意变更offset值，而不是提交最新的

```java
public class ResetOffsetDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.20.0.139:9092");
        props.put("group.id", "test2");--------------------------------------------改动1
        props.put("auto.offset.reset", "earliest");--------------------------------改动2
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("foo", "bar"));
        while (true){--------------------------------------------------------------改动3
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
```

程序解释

* 改动1：为便于测试说明，修改消费者组，之前提到，消费者组是消费的单位，变更消费者组后相当于对原有Topic重新消费，但是第一次访问时默认没有offset，官方设置的默认值是latest，也就是说，当一个新的消费者组向一个Topic请求消息时，该组的offset会被默认设置为其他消费者组最新的offset，因此丢失了历史数据，为避免这个问题，重新设置该策略，即改动2
* 改动2：设置新消费者组对Topic首次请求的offset设置策略为最早，即从头开始
* 改动3：见程序注释，这里需要理解kafka的设计，kafka对于每个Topic都有1个或者多个partition，目的是为了提高并发量和防止单机器瓶颈，消息发送者把消息发送到指定Topic的时候可以选择默认策略，此时kafka会根据负载均衡策略自动把消息发到不同的partition，也可以由发送者根据一定的策略（比如对消息的某些字段求哈希）指定要发送的分区，每个分区互不干扰，每个分区对每个消费者组维护着一个offset，每个分区还有冗余因子，避免单点故障。提交的offset总是下一个要消费的消息位置

##### 扩展三：不指定Topic，而是指定partition进行消息订阅

```java
public class PartitionAssignDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.20.0.139:9092");
        props.put("group.id", "test3");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        String topic = "foo";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        //TopicPartition partition1 = new TopicPartition(topic, 1);
        consumer.assign(Arrays.asList(partition0));
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(2000);
            for(ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, partition = %d, value = %s%n", record.offset(), record.partition(), record.value());
            }
        }
    }
}
```

> 此处的Topic “foo”只有一个partition，id为0，因此如果分配不存在的partition给消费者时，在poll的时候就会造成阻塞

程序解释

* 根据topic名称和partition号创建TopicPartition对象，该分区必须真实存在
* 给指定消费者客户端指定要监听的partition
* 此种监听方式不会导致topic对监听者组的平均分配或者在分配

##### 扩展四：消费者只监听一个Topic的部分partition时会怎样

创建一个新的topic "muti_part"，指定分区数为2，往"bar"中发送几条消息，确保2个分区中都有消息

```shell
//创建Topic
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic muti_part
//发送消息
kafka-console-producer.sh --broker-list localhost:9092 --topic muti_part
>abc
>def
>ghi
>ijk
>jkl
>lmn
>123
>
```

读取消息

```java
public class PartitionAssignDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.20.0.139:9092");
        props.put("group.id", "test");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        String topic = "muti_part";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        //TopicPartition partition1 = new TopicPartition(topic, 1);
        consumer.assign(Arrays.asList(partition0));
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, partition = %d, value = %s%n", record.offset(), record.partition(), record.value());
            }
        }
    }
}
```

输出结果如下：

```java
15:46:17,064 [ main ] [ INFO ]:109 - Kafka version : 1.0.0
15:46:17,064 [ main ] [ INFO ]:110 - Kafka commitId : aaa7af6d4a11b29d
15:46:17,172 [ main ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=test] Discovered coordinator 10.20.0.139:9092 (id: 2147483647 rack: null)
offset = 0, partition = 0, value = def
offset = 1, partition = 0, value = ijk
offset = 2, partition = 0, value = lmn
```

结论：

> kafka均匀的把消息放到不同的partition中，该消费者只能获取指定分区的消息

##### 扩展五：多个消费者并行读取

情形一：Topic只有一个partition时，以topic "foo"为例

```java
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

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.20.0.139:9092");
        props.put("group.id", "mcsp");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("foo"));
        new Thread(new MyConsumer(consumer, "consumer1")).start();
    }
}
```

程序说明

* 消费者每读取到累计5条消息时，提交offset，为便于测试后续线程断开的情况，启用多线程

输出结果如下：

```
16:30:51,033 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp3] Discovered coordinator 10.20.0.139:9092 (id: 2147483647 rack: null)
16:30:51,036 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp3] Revoking previously assigned partitions []
16:30:51,036 [ Thread-1 ] [ INFO ]:336 - [Consumer clientId=consumer-1, groupId=mcsp3] (Re-)joining group
16:30:51,049 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp3] Successfully joined group with generation 1
16:30:51,050 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp3] Setting newly assigned partitions [foo-0]
name = consumer1, partition = 0, offset = 0, key = null, value = hello
name = consumer1, partition = 0, offset = 1, key = null, value = baby
name = consumer1, partition = 0, offset = 2, key = null, value = so
...
name = consumer1, partition = 0, offset = 16, key = null, value = 8
consumer1 commit
```

可以看出，进行了一次分组分配，把partition0分配给了consumer1，consumer1把所有消息读完之后，更新了offset值，此时，在开一个终端，把name改为consumer2，重新启动一个进程，此时，输出如下：

```java
consumer1上的输出：
16:31:18,052 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp3] Revoking previously assigned partitions [foo-0]
16:31:18,053 [ Thread-1 ] [ INFO ]:336 - [Consumer clientId=consumer-1, groupId=mcsp3] (Re-)joining group
16:31:18,068 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp3] Successfully joined group with generation 2
16:31:18,069 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp3] Setting newly assigned partitions [foo-0]

consumer2上的输出：
16:31:15,567 [ main ] [ INFO ]:109 - Kafka version : 1.0.0
16:31:15,567 [ main ] [ INFO ]:110 - Kafka commitId : aaa7af6d4a11b29d
16:31:15,669 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp3] Discovered coordinator 10.20.0.139:9092 (id: 2147483647 rack: null)
16:31:15,672 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp3] Revoking previously assigned partitions []
16:31:15,672 [ Thread-1 ] [ INFO ]:336 - [Consumer clientId=consumer-1, groupId=mcsp3] (Re-)joining group
16:31:18,066 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp3] Successfully joined group with generation 2
16:31:18,069 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp3] Setting newly assigned partitions []
```

可以看出，由于consumer2的加入，导致重新消费者组的消息均衡策略被重新刷新，现在往foo中发送2条消息，结果如下：只有consumer1有输出，consumer2没有输出，也就是partition0被分配给了consumer1，由于只有2条消息，consumer1并没有提交offset，现在断开consumer1进程，发现consumer2输出如下：

```java
16:32:27,074 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp3] Revoking previously assigned partitions []
16:32:27,074 [ Thread-1 ] [ INFO ]:336 - [Consumer clientId=consumer-1, groupId=mcsp3] (Re-)joining group
16:32:27,081 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp3] Successfully joined group with generation 3
16:32:27,082 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp3] Setting newly assigned partitions [foo-0]
name = consumer2, partition = 0, offset = 17, key = null, value = a
name = consumer2, partition = 0, offset = 18, key = null, value = b
```

没错，消费者组策略又被重新分配了，consumer2输出了刚刚发送的2条消息，这里就导致了一个问题，由于consumer1的异常关闭，导致没有提交最新的offset，导致那2条消息被消费了2次，解决这个问题的办法见扩展六：在消费者监听Topic时添加ConsumerRebalanceListener



情形二：Topic有多个patition时，以topic "muti_part"为例

代码和操作方式跟上述一致，把topic名字改为muti_part即可

当先启动的consumer把消息消费完后，有新消费者加入是，会rebalence，此时，再往该Topic里面发消息时，出现：

```java
consumer1:
17:12:32,879 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp] Revoking previously assigned partitions [muti_part-0, muti_part-1]
17:12:32,879 [ Thread-1 ] [ INFO ]:336 - [Consumer clientId=consumer-1, groupId=mcsp] (Re-)joining group
17:12:32,890 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp] Successfully joined group with generation 4
17:12:32,892 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp] Setting newly assigned partitions [muti_part-0]
name = consumer1, partition = 0, offset = 3, key = null, value = b
name = consumer1, partition = 0, offset = 4, key = null, value = d
name = consumer1, partition = 0, offset = 5, key = null, value = f
name = consumer1, partition = 0, offset = 6, key = null, value = h
  
consumer2:
name = consumer2, partition = 1, offset = 4, key = null, value = a
name = consumer2, partition = 1, offset = 5, key = null, value = c
name = consumer2, partition = 1, offset = 6, key = null, value = e
name = consumer2, partition = 1, offset = 7, key = null, value = g
```

可以看出，每个消费者负责一个partition



##### 扩展六：消费者组中有新消费者加入又没有提交offset时，导致并发情况下rebalance后重复消费数据，添加ConsumerRebalanceListener

```java
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
```

程序说明

自定义MyListener类实现ConsumerRebalanceListener接口，其中onPartitionsRevoked方法表示当某个分区从指定消费者移除时应该做的动作，这里实现为提交每个分区最新的offset值，以免rebalance完成之后消息重复消费

首先启动一个消费者，在启动另一个消费者，第一个消费者输出如下：

```java
22:26:49,555 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp2] Discovered coordinator 47.100.49.129:9092 (id: 2147483647 rack: null)
22:26:49,565 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp2] Revoking previously assigned partitions []
22:26:49,565 [ Thread-1 ] [ INFO ]:336 - [Consumer clientId=consumer-1, groupId=mcsp2] (Re-)joining group
assign partition 0 to consumer1
assign partition 1 to consumer1
22:26:49,645 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp2] Successfully joined group with generation 3
22:26:49,645 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp2] Setting newly assigned partitions [muti_part-0, muti_part-1]
name = consumer1, partition = 0, offset = 0, key = null, value = 2
name = consumer1, partition = 1, offset = 0, key = null, value = 1
name = consumer1, partition = 1, offset = 1, key = null, value = 3
revoke consumer1 from partition 0
commit partition 0 offset 1
22:27:49,735 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp2] Revoking previously assigned partitions [muti_part-0, muti_part-1]
revoke consumer1 from partition 1
commit partition 1 offset 2
22:27:49,765 [ Thread-1 ] [ INFO ]:336 - [Consumer clientId=consumer-1, groupId=mcsp2] (Re-)joining group
22:27:49,795 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp2] Successfully joined group with generation 4
assign partition 0 to consumer1
22:27:49,795 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp2] Setting newly assigned partitions [muti_part-0]
```

可以看出，在没有启动第二个消费者之前，2个分区都被指派给了consumer1，consumer1读取了3条消息，并没有提交

consumer2输出如下：

```java
22:27:48,488 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp2] Discovered coordinator 47.100.49.129:9092 (id: 2147483647 rack: null)
22:27:48,488 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp2] Revoking previously assigned partitions []
22:27:48,488 [ Thread-1 ] [ INFO ]:336 - [Consumer clientId=consumer-1, groupId=mcsp2] (Re-)joining group
assign partition 1 to consumer2
22:27:49,795 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp2] Successfully joined group with generation 4
22:27:49,795 [ Thread-1 ] [ INFO ]:341 - [Consumer clientId=consumer-1, groupId=mcsp2] Setting newly assigned partitions [muti_part-1]
```

可以看出，当consumer2启动时，kafka将所有分区收回再重新分配，收回触发了consumer1的listener接口提交最新的offset，因此consumer2不会重复读到数据

> 注：此种方法只能用于有新的消费者加入组时使用，当消费者异常断开时，依然不会提交offset，若想要保证消费者断开时不会重复消费数据，则可以通过指定partition的方式监听，同时把offset保存起来，原则是不让kafka进行rebalance

