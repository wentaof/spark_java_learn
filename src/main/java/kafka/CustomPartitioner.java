package kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import javax.sound.midi.Soundbank;
import java.util.Map;

/**
 * @author fengwentao@changjing.ai
 * @date 2022/6/24 18:19
 * @Version 1.0.0
 * @Description 自定义分区生产者
 */
public class CustomPartitioner implements Partitioner {
    public static void main(String[] args) {

    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        //System.out.println(topic);
        //System.out.println(key);
        //System.out.println(keyBytes);
        //System.out.println(value);
        //System.out.println(valueBytes);
        //System.out.println(cluster);
        //System.out.println("-------------------------");
        return Integer.parseInt(key.toString()) % 3;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
