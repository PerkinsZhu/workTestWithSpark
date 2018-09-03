package kafka.java;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Created by PerkinsZhu on 2018/9/3 13:43
 **/
public class KafkaTest {
    Properties properties = null;

    @Before
    public void initProperties() {
        // Producer 配置信息，应该配置在属性文件中
        properties = new Properties();
        //指定要连接的 broker，不需要列出所有的 broker，但建议至少列出2个，以防某个 broker 挂了
        properties.put("bootstrap.servers", "servera.local.com:9092,serverb.local.com:9092,serverc.local.com:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("retries", 3); // 如果发生错误，重试三次
        properties.put("acks", "1"); // 0：不应答，1：leader 应该，all：所有 leader 和 follower 应该
    }


    @Test
    public void testProducer() {
        // 创建 Producer
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        // send 方法是异步的，方法返回并不代表消息发送成功
        producer.send(new ProducerRecord<String, String>("topic0", "message 1"));

        // 如果需要确认消息是否发送成功，以及发送后做一些额外操作，有两种办法
        // 方法 1: 使用 callback
        producer.send(new ProducerRecord<String, String>("topic0", "message 2"), new Callback() {

            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    System.out.println("send message2 failed with " + exception.getMessage());
                } else {
                    // offset 是消息在 partition 中的编号，可以根据 offset 检索消息
                    System.out.println("message2 sent to " + metadata.topic() + ", partition " + metadata.partition() + ", offset " + metadata.offset());
                }

            }

        });

        // 方法2：使用阻塞
        Future<RecordMetadata> sendResult = producer.send(new ProducerRecord<String, String>("topic0", "message 3"));
        try {
            // 阻塞直到发送成功
            RecordMetadata metadata = sendResult.get();
            System.out.println("message3 sent to " + metadata.topic() + ", partition " + metadata.partition() + ", offset " + metadata.offset());
        } catch (Exception e) {
            System.out.println("send message3 failed with " + e.getMessage());
        }

        // producer 需要关闭，放在 finally 里
        producer.close();
    }

    @Test
    public void testConsumer() {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList("topic0"));
        kafkaConsumer.listTopics().forEach((String key, List<PartitionInfo> list) -> {
            System.out.println(key);
            list.forEach((PartitionInfo info) -> System.out.println(info));
        });
    }
}
