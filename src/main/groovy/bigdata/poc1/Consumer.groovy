package bigdata.poc1

import com.google.common.io.Resources
import groovy.json.JsonSlurper
import org.HdrHistogram.Histogram
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.Logger

/**
 * Created by Juan Malacrida on 18-Oct-16.
 */
class Consumer implements Runnable {

    static def logger = Logger.getLogger(Producer.class)

    def topic = ""

    def run = true;

    def bootstrapServers = ""

    KafkaConsumer<String, String> consumer

    def properties = new Properties()

    @Override
    void run() {
        consumeMessagesFrom()
    }

    private def consumeMessagesFrom() {
        def stats = new Histogram(1, 10000000, 2)
        def global = new Histogram(1, 10000000, 2)

        Resources.getResource("consumer.props").withInputStream { stream ->
            properties.load(stream)
            if (properties.getProperty("group.id") == null) {
                properties.setProperty("group.id", "group-" + new Random().nextInt(100000))
            }
            properties.setProperty("bootstrap.servers", bootstrapServers)
            consumer = new KafkaConsumer<>(properties)
        }

        if (consumer == null) {
            logger.error "consumer is null"
            return
        }

        consumer.subscribe([topic])

        while (run) {
            ConsumerRecords<String, String> records = consumer.poll(200);

            records.records(topic).forEach({
                ConsumerRecord<String, String> record = it
                try {
                    def jsonSlurper = new JsonSlurper()
                    def message = jsonSlurper.parseText(record.value())

                    switch (message.type) {
                        case "test":
                            def latency = (long) ((System.nanoTime() * 1e-9 - message.t) * 1000)
                            stats.recordValue(latency)
                            global.recordValue(latency)
                            break
                        case "marker":
                            logger.info String.format("%d messages received in period, latency(min, max, avg, 99%%) = %d, %d, %.1f, %d (ms)",
                                    stats.getTotalCount(),
                                    stats.getValueAtPercentile(0), stats.getValueAtPercentile(100),
                                    stats.getMean(), stats.getValueAtPercentile(99))
                            logger.info String.format("%d messages received overall, latency(min, max, avg, 99%%) = %d, %d, %.1f, %d (ms)",
                                    global.getTotalCount(),
                                    global.getValueAtPercentile(0), global.getValueAtPercentile(100),
                                    global.getMean(), global.getValueAtPercentile(99))
                            logger.info message
                            stats.reset()
                            break
                        default:
                            logger.warn String.format("Not valid message %s%n", message)
                            break
                    }
                } catch (Exception e) {
                    logger.error e.message
                }

            })
        }
        consumer.unsubscribe()
        logger.info "bye"
    }

}
