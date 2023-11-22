package DataGen.Connectors;

import DataGen.utils.DemoCallBack;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class kafka {
    private final org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;
    private final Boolean isAsync;
    private final String topicName;

    public kafka(String topic, String bootStrapServers, Boolean isAsync) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootStrapServers);
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
        this.isAsync = isAsync;
        this.topicName = topic;
    }

    public void sendMessage(String key, String value) {

        long startTime = System.currentTimeMillis();
        if (isAsync) { // Send asynchronously
            producer.send(
                    new ProducerRecord<String, String>(topicName, key, value),
                    (Callback) new DemoCallBack(startTime, key, value));
        } else { // Send synchronously
            try {
                producer.send(new ProducerRecord<String, String>(topicName, key, value))
                        .get();
                //System.out.println("Sent message: (" + key + ", " + value + ")");
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}

