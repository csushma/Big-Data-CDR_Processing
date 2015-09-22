import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext._

/**
 * Created by BaoyeXue on 2015/8/2.
 */

// Load and parse the data
val data = sc.textFile("/bxx140230/output/14_out/part-r-00000")
val cdr = data.map{ line =>
	val fields = line.split('\t')
	(fields(1).toDouble,fields(2).toDouble,fields(3).toDouble,fields(4).toDouble)
}

val parsedData = cdr.map(s => Vectors.dense(s._1,s._2,s._3,s._4)).cache()

// Cluster the data into two classes using KMeans
val numClusters = 5
val numIterations = 20
val clusters = KMeans.train(parsedData, numClusters, numIterations)

// Evaluate clustering by computing Within Set Sum of Squared Errors
val WSSSE = clusters.computeCost(parsedData)
println("Within Set Sum of Squared Errors = " + WSSSE)

//Save Model
clusters.save(sc,"myModelPath")
val sameModel = KMeansModel.load(sc, "myModelPath")

//Predict
val result2 = data.map{ line =>
	val fields = line.split('\t')
	val features = Array[Double](fields(1).toDouble,fields(2).toDouble,fields(3).toDouble,fields(4).toDouble)
	val linevectore = Vectors.dense(features)  
        val prediction = clusters.predict(linevectore)
	fields(0) + " " + fields(1) + " " + fields(2) + " " + fields(3) + " " + fields(4) + " " + prediction
}
result2.saveAsTextFile("file:///usr/lib/hue/prediction")



