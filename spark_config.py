import os
import sys
import pyspark

SPARK_HOME = os.path.dirname(pyspark.__file__)

os.environ["SPARK_HOME"] = SPARK_HOME
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = r"C:\Program Files\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Program Files\Python311\python.exe"
os.environ["PYSPARK_PIN_THREAD"] = "true"
os.environ["SPARK_LOCAL_DIRS"] = r"C:\temp\spark"
os.environ["JAVA_TOOL_OPTIONS"] = "-Djava.io.tmpdir=C:/temp/spark"
os.environ["SPARK_SUBMIT_OPTS"] = "-Dlog4j.configuration=file:C:/Users/acer/AppData/Roaming/Python/Python311/site-packages/pyspark/conf/log4j.properties"
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"

sys.path.insert(0, r"C:\Users\acer\AppData\Roaming\Python\Python311\site-packages")