//read txt file
val distFile = sc.textFile("./Downloads/spark/claimant_data.txt")


spark
spark.read.textFile("./Documents/spark/claimant_data.txt")
res22.show

spark.read.option("header",true).textFile("./Documents/spark/claimant_data.txt").show

spark.read.option("header",true).csv("./Documents/spark/claimant_data.csv")

.show

val claimant = spark.read.option("header",true).csv("./Documents/spark/claimant_data.csv").toDF

:type claimant

val claimant = spark.read.option("header",true).option("inferSchema","true").csv("./Documents/spark/claimant_data.csv").toDF


claimant.groupBy($"uniq").count.show


claimant.groupBy($"ssn").count.show


