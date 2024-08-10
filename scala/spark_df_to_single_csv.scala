import org.apache.spark.sql.{SparkSession, DataFrame}

val spark: SparkSession = SparkSession.builder.appName("Empty Dataframe Handler").getOrCreate
val basePath: String = "dbfs:/"
val fileName: String = "sample.csv"
val tmpDir = s"${basePath}_tmp_${System.currentTimeMillis()}"

val df: DataFrame  // your spark dataframe

df.coalesce(1)
	.write
	.option("header", "true")
	.option("delimiter", ",")
	.csv(tmpDir)

val csvPath = dbutils.fs.ls(tmpDir)
	.filter(file => file.name.endsWith(".csv"))
	.headOption
	.map(_.path)
	.getOrElse(throw new RuntimeException("CSV file not found in the temporary directory."))

dbutils.fs.cp(csvPath, s"${basePath}${fileName}")
println(s"CSV file successfully written to ${basePath}${fileName}")

dbutils.fs.rm(tmpDir, recurse = true)