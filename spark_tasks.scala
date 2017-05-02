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

// objective: assign rows with same ssn a unique key
// Step 1: read claimant_data
// Step 2: assign a unique key to ssn 
// Step 3: assign this new key() to rows that match this ssn

claimant.groupBy("ssn").count().show()

claimant.createOrReplaceTempView("claimant_tbl")
val sqlDF = spark.sql("select * from claimant_tbl")
sqlDF.show()

claimant.groupBy("ssn").min("uniq").show
val claimant_kv = claimant.groupBy("ssn").min("uniq").withColumnRenamed("min(uniq)", "min_uniq")

val claimant_kv = claimant.groupBy("ssn").min("uniq").withColumnRenamed("min(uniq)", "min_uniq").withColumnRenamed("ssn","ssn_uniq")

val joined = claimant.join(claimant_kv,'ssn_uniq==='ssn,"inner").select("uniq","dim","ssn","min_uniq")

joined.select("uniq","min_uniq").distinct().orderBy("uniq").show()



val claimant_new = spark.read.option("header",true).csv("./Documents/spark/claimant_data_new.csv").toDF
val claimant_new = spark.read.option("header",true).option("inferSchema","true").csv("./Documents/spark/claimant_data_new.csv").toDF

val claimant_new_kv = claimant_new.groupBy("ssn").min("uniq").withColumnRenamed("min(uniq)", "min_uniq").withColumnRenamed("ssn","ssn_uniq")
claimant_new_kv.filter($"ssn_uniq"< "'U'").show()
val claimant_new_kv = claimant_new.groupBy("ssn").min("uniq").withColumnRenamed("min(uniq)", "min_uniq").withColumnRenamed("ssn","ssn_uniq").filter($"ssn_uniq"< "'U'")

val joined_new = claimant_new.join(claimant_new_kv,'ssn_uniq==='ssn,"inner").select("uniq","dim","ssn","min_uniq")

joined_new.select("uniq","min_uniq").distinct().orderBy("uniq").show()
