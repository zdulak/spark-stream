import java.util.concurrent.TimeUnit
import scala.util.{Failure, Success, Try}

object ForeachStream extends App {
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder()
    .appName("Wed")
    .master("local[*]")
    .getOrCreate()

  val stream = spark
    .readStream
    .format("rate")
    .load

  import scala.concurrent.duration.DurationInt
  import org.apache.spark.sql.streaming.Trigger
  import org.apache.spark.sql.{ForeachWriter, Row}
  val sq = stream
    //  if we use as[T] here, we have foreachWriter[T] instead of foreachWriter[Row]
    //  as[...] is transformation
    .writeStream
    .format("console")
    .trigger(Trigger.ProcessingTime(2.seconds))
    .option("truncate", "false")
    .queryName("MyRate")
    .foreach(new ForeachWriter[Row] {
      import java.io._
      var fileWriter: FileWriter = _
//      epochId is batchId
      override def open(partitionId: Long, epochId: Long): Boolean = {
        Try {
          fileWriter = new FileWriter(s"stream\\rate-$partitionId-$epochId.txt")
        }
        match {
          case Success(value) => true
          case Failure(exception) => {
            close(exception)
            false
          }
        }
      }

      override def process(value: Row): Unit = {
        fileWriter.write(value.toString() + "\n")
//        fileWriter.flush() -> not needed if there is \n
      }

      override def close(errorOrNull: Throwable): Unit = {
        if (fileWriter != null) {
          fileWriter.close()
        }
        if (errorOrNull != null) {
          fileWriter = new FileWriter("stream\\log.txt", true)
          fileWriter.write(errorOrNull.getMessage + "\n")
          fileWriter.close()
        }
      }
    })
    .start

  sq.awaitTermination(10000)
  sq.stop()


  println("cos")
  TimeUnit.HOURS.sleep(2)
}
