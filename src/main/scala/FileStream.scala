object FileStream  extends App {
  import org.apache.spark.sql.SparkSession
  val spark = SparkSession
    .builder()
    .appName("Wed")
    .master("local[*]")
    .getOrCreate()

  val path = if (args.length > 0) args(0) else "data"

  import org.apache.spark.sql.types._
  val schema = StructType(
    StructField("id", LongType, nullable = false) ::
    StructField("name", StringType, nullable = false) :: Nil
  )

  val stream = spark.readStream
    .format("csv")
    .option("header", "true")
    .schema(schema)
    .load(path)

  stream.writeStream.format("console").start.awaitTermination()
}
