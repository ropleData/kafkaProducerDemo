import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AsyncProducerCallbackDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // 0.配置一系列参数
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");//kafka集群，broker-list
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 1);//重试次数
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);//批次大小
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);//等待时间
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);//RecordAccumulator缓冲区大小
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 1.创建一个生产者对象
        Producer<String, String> producer = new KafkaProducer<>(props);

        // 2.调用send方法
        for (int i = 0; i < 100; i++) {
            //回调函数，该方法会在Producer收到ack时调用，为异步调用
            producer.send(new ProducerRecord<String, String>("testKafka1", Integer.toString(i), Integer.toString(i)), (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("消息发送成功->" + metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });

        }
        // 3.关闭生产者
        producer.close();
    }
}

