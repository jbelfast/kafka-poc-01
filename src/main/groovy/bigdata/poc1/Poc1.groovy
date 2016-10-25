package bigdata.poc1

/**
 * Created by Juan Malacrida on 18-Oct-16.
 */
class Poc1 {
    static void main(String[] args) {

        def bootstrapServers = args.length < 1 ? "localhost" : args[0]

        def topic = args.length < 2 ? "POC1" : args[1]

        def producer = new Producer(topic: topic, bootstrapServers: bootstrapServers)
        def consumer = new Consumer(topic: topic, bootstrapServers: bootstrapServers)

        def producerThread = new Thread(producer)
        def consumerThread = new Thread(consumer)

        print "startp, stopp, startc, stopc, exit: "
        System.in.eachLine() { line ->
            if (line == "exit") {
                producer.run = false
                consumer.run = false
                println "finishing processes"
                while (producerThread.alive || consumerThread.alive);
                println "processes finished"
                System.exit(0)
            } else if (line == "startp") {
                if (producerThread.state == Thread.State.NEW) {
                    producer.run = true
                    producerThread.start()
                    println "producer started"
                } else {
                    println "producer is in state $producerThread.state"
                }
            } else if (line == "stopp") {
                producer.run = false
                println "finishing producer"
                while (producerThread.alive);
                println "producer finished"
                producerThread = new Thread(producer)
            } else if (line == "startc") {
                if (consumerThread.state == Thread.State.NEW) {
                    consumer.run = true
                    consumerThread.start()
                    println "consumer started"
                } else {
                    println "consumer is in state $consumerThread.state"
                }
            } else if (line == "stopc") {
                consumer.run = false
                println "finishing consumer"
                while (consumerThread.alive);
                println "consumer finished"
                consumerThread = new Thread(consumer)
            } else {
                println "you entered: $line"
            }
            print "startp, stopp, startc, stopc, exit: "
        }
    }
}
