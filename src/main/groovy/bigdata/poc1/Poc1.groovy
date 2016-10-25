package bigdata.poc1

import org.apache.log4j.Logger

/**
 * Created by Juan Malacrida on 18-Oct-16.
 */
class Poc1 {

    static def logger = Logger.getLogger(Poc1.class)

    static void main(String[] args) {

        logger.info "starting"

        def bootstrapServers = args.length < 1 ? "localhost" : args[0]

        def topic = args.length < 2 ? "POC1" : args[1]

        logger.info String.format("bootstrapServers %s", bootstrapServers)
        logger.info String.format("topic %s", topic)

        def producer = new Producer(topic: topic, bootstrapServers: bootstrapServers)
        def consumer = new Consumer(topic: topic, bootstrapServers: bootstrapServers)

        def producerThread = new Thread(producer)
        def consumerThread = new Thread(consumer)

        println "startall, startp, startc, stopp, stopc, stopall, exit: "
        System.in.eachLine() { line ->
            if (line == "startp" || line == "startall") {
                if (producerThread.state == Thread.State.NEW) {
                    producer.run = true
                    producerThread.start()
                    println "producer started"
                } else {
                    println "producer is in state $producerThread.state"
                }
            }
            if (line == "startc" || line == "startall") {
                if (consumerThread.state == Thread.State.NEW) {
                    consumer.run = true
                    consumerThread.start()
                    println "consumer started"
                } else {
                    println "consumer is in state $consumerThread.state"
                }
            }

            if (line == "stopp" || line == "stopall") {
                producer.run = false
                println "finishing producer"
                while (producerThread.alive);
                println "producer finished"
                producerThread = new Thread(producer)
            }
            if (line == "stopc" || line == "stopall") {
                consumer.run = false
                println "finishing consumer"
                while (consumerThread.alive);
                println "consumer finished"
                consumerThread = new Thread(consumer)
            }

            if (line == "exit") {
                producer.run = false
                consumer.run = false
                println "finishing processes"
                while (producerThread.alive || consumerThread.alive);
                println "processes finished"
                System.exit(0)
            }
            println "startall, startp, startc, stopp, stopc, stopall, exit: "
        }
    }
}
