package bigdata.poc1

import com.google.common.io.Resources
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.log4j.Logger

/**
 * Created by Juan Malacrida on 18-Oct-16.
 */
class Producer implements Runnable {

    static def logger = Logger.getLogger(Producer.class)

    def topic = "";

    def run = true;

    def bootstrapServers = ""

    KafkaProducer<String, String> producer

    def properties = new Properties()

    @Override
    void run() {
        produceMessages()
    }

    private def produceMessages() {
        Resources.getResource("producer.props").withInputStream { stream ->
            properties.load(stream)
            properties.setProperty("bootstrap.servers", bootstrapServers)
            producer = new KafkaProducer<>(properties)
        }

        if (producer == null) {
            logger.error "producer is null"
            return;
        }

        try {
            for (int i = 1; run; i++) {
                producer.send(new ProducerRecord(topic, String.format("{\"type\":\"test\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)))

                if (i % 100 == 0) {
                    producer.send(new ProducerRecord(topic, String.format("{\"type\":\"marker\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)))
                    //producer.send(new ProducerRecord(topic, String.format("{\"type\":\"other\", \"t\":%.3f, \"k\":%d}", System.nanoTime() * 1e-9, i)))
                    producer.flush()
                    logger.info "Sent msg number " + i
                }
                sleep(10 + Random.newInstance().nextInt(1000))
            }
        } catch (Exception e) {
            logger.error e.message
        } finally {
            producer.close()
            logger.info "bye"
        }
    }
}
