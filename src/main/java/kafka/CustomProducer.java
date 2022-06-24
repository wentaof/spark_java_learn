package kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @author fengwentao@changjing.ai
 * @date 2022/6/24 18:25
 * @Version 1.0.0
 * @Description TODO
 */
public class CustomProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers","test:9092,test:9093,test:9094");
        //等待所有副本节点应答
        props.put("acks","all");
        props.put("retries",0);
        props.put("batch.size",16384);
        props.put("linger.ms",1); //请求延时
        props.put("buffer.memory", 33554432);
        // key 序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //使用自定义分区函数,发送数据
        props.put("partitioner.class","kafka.CustomPartitioner");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for( int i = 0; i<50;i++){
            producer.send(new ProducerRecord<String, String>("first2", Integer.toString(i), "customPartitioner_" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println(metadata.partition()+ "--->"+metadata.offset());
                }
            });
            Thread.sleep(1000);
        }

        producer.close();
    }
}
