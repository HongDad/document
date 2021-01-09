package lesson01

import org.apache.flink.streaming.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.socketTextStream("192.168.123.102", 9999)
    dataStream.flatMap( line => line.split(","))
        .map((_,1))
        .keyBy( wc => wc._1)
        .sum(1)
        .print()
    env.execute("WordCount")
  }

}
