package myflink

import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import java.lang.Exception

object WordCount {
    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        // Create the execution environment
        val env = StreamExecutionEnvironment.getExecutionEnvironment()

        // If 5000 port is not available, you have to change the port.
        val text: DataStream<String> = env.socketTextStream("localhost", 5000, "\n")

        // Parse the data, group by word, and perform the window and aggregation operations.
        val windowCounts = text.flatMap<Tuple2<String, Int>> { s, collector ->
            s.split("\\s".toRegex()).toTypedArray().forEach { word ->
                collector.collect(Tuple2.of(word, 1))
            }
        }
        .keyBy(0)
        .timeWindow(Time.seconds(10))
        .sum(1)

        // Print the results to the console. Note that here single-threaded printed is used, rather than multi-threading.
        windowCounts.print().setParallelism(1)
        env.execute("Socket Window WordCount")
    }
}
