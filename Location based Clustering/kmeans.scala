import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

// Load and parse the data
val data = sc.textFile("/usr/lib/hue/input/data_set.csv")
val pdata = data.map(line => line.replaceAll("\"", " "))
val parsedData = pdata.map(s => Vectors.dense(s.split(",").drop(5).take(5).map(_.toDouble))).cache()

// Cluster the data into two classes using KMeans
val numClusters = 3
val numIterations = 20
val clusters = KMeans.train(parsedData, numClusters, numIterations)

// Evaluate clustering by computing Within Set Sum of Squared Errors
val WSSSE = clusters.computeCost(parsedData)
println("Within Set Sum of Squared Errors = " + WSSSE)

//Save Model
clusters.save(sc,"myModelPath")
val sameModel = KMeansModel.load(sc, "myModelPath")

//saving cluster centers
var lines  = List[org.apache.spark.mllib.linalg.Vector]()
var center = org.apache.spark.mllib.linalg.Vectors
for (center <- clusters.clusterCenters) {       
   lines = center::lines;
}

import java.io._
val output = "/usr/lib/hue/output/cluster_centers.txt"
val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output)))
for(x <- lines){
	writer.write(x + "\n");
}

writer.close()

