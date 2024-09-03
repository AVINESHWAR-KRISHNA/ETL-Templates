APP_NAME = "LocalPySpark"
MASTER = "local[*]"  # Use all available cores
DRIVER_MEMORY = "4g"  # Driver memory
EXECUTOR_MEMORY = "2g"  # Executor memory
EXECUTOR_CORES = "2"  # Number of cores per executor
DYNAMIC_ALLOCATION_ENABLED = "true"  # Enable dynamic allocation
DYNAMIC_ALLOCATION_MIN_EXECUTORS = "1"  # Minimum number of executors
DYNAMIC_ALLOCATION_MAX_EXECUTORS = "10"  # Maximum number of executors
DYNAMIC_ALLOCATION_INITIAL_EXECUTORS = "2"  # Initial number of executors
DYNAMIC_ALLOCATION_EXECUTOR_IDLE_TIMEOUT = "60s"  # Timeout for idle executors
SERIALIZER = "org.apache.spark.serializer.KryoSerializer"  # Kryo serialization
KRYO_SERIALIZER_BUFFER_MAX = "512m"  # Kryo buffer size
ARROW_ENABLED = "true"  # Enable Arrow for better performance
UI_PORT = "4040"  # Spark UI port
ARROW_PYSPARK_ENABLED = "true"  # Enable Arrow-based data transfers
AUTO_BROADCAST_JOIN_THRESHOLD = "-1"  # Disable broadcast joins for large datasets
SHUFFLE_PARTITIONS = "8"  # Number of shuffle partitions
EXECUTOR_EXTRA_JAVA_OPTIONS = "-XX:+UseG1GC"  # Use G1 Garbage Collector
NETWORK_TIMEOUT = "800s"  # Network timeout for fault tolerance
ADAPTIVE_EXECUTION_ENABLED = "true"  # Enable adaptive query execution
TASK_MAX_FAILURES = "4"  # Set task failure retry count
SPECULATION_ENABLED = "true"  # Enable speculative execution
SPECULATION_QUANTILE = "0.75"  # Set speculative threshold
SPECULATION_MULTIPLIER = "1.5"  # Set speculative multiplier


from pyspark.sql import SparkSession

try:
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .master(MASTER) \
        .config("spark.driver.memory", DRIVER_MEMORY) \
        .config("spark.executor.memory", EXECUTOR_MEMORY) \
        .config("spark.executor.cores", EXECUTOR_CORES) \
        .config("spark.dynamicAllocation.enabled", DYNAMIC_ALLOCATION_ENABLED) \
        .config("spark.dynamicAllocation.minExecutors", DYNAMIC_ALLOCATION_MIN_EXECUTORS) \
        .config("spark.dynamicAllocation.maxExecutors", DYNAMIC_ALLOCATION_MAX_EXECUTORS) \
        .config("spark.dynamicAllocation.initialExecutors", DYNAMIC_ALLOCATION_INITIAL_EXECUTORS) \
        .config("spark.dynamicAllocation.executorIdleTimeout", DYNAMIC_ALLOCATION_EXECUTOR_IDLE_TIMEOUT) \
        .config("spark.serializer", SERIALIZER) \
        .config("spark.kryoserializer.buffer.max", KRYO_SERIALIZER_BUFFER_MAX) \
        .config("spark.ui.port", UI_PORT) \
        .config("spark.sql.execution.arrow.pyspark.enabled", ARROW_PYSPARK_ENABLED) \
        .config("spark.sql.autoBroadcastJoinThreshold", AUTO_BROADCAST_JOIN_THRESHOLD) \
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS) \
        .config("spark.executor.extraJavaOptions", EXECUTOR_EXTRA_JAVA_OPTIONS) \
        .config("spark.network.timeout", NETWORK_TIMEOUT) \
        .config("spark.sql.adaptive.enabled", ADAPTIVE_EXECUTION_ENABLED) \
        .config("spark.task.maxFailures", TASK_MAX_FAILURES) \
        .config("spark.speculation", SPECULATION_ENABLED) \
        .config("spark.speculation.quantile", SPECULATION_QUANTILE) \
        .config("spark.speculation.multiplier", SPECULATION_MULTIPLIER) \
        .getOrCreate()

    # Your Spark operations go here
    print(f"Spark UI is running on: {spark.sparkContext.uiWebUrl}")

    # Example DataFrame operation
    df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    df.show()

except Exception as e:
    print(f"An error occurred: {str(e)}")
finally:
    if 'spark' in locals():
        spark.stop()
