type Point = (Double, Double)
import java.lang.Math._
def distance(x: Point, y: Point): Double =
    sqrt(pow(x._1 - y._1, 2.0) + pow(x._2 - y._2, 2.0))
	
 def closestPoint(p: Point, centers: Array[Point]): Int = {
     var bestIndex = 0
     var closest = Double.PositiveInfinity
     for (i <- 0 until centers.length) {
       val tempDist = distance(p,centers(i))
       if (tempDist < closest) {
         closest = tempDist
         bestIndex = i
       }
     }
     return bestIndex
   }
	var centers  = List[org.apache.spark.mllib.linalg.Vector]()
	var center = org.apache.spark.mllib.linalg.Vectors
	for (center <- clusters.clusterCenters) {       
		centers = center::centers;
	}

	var center:Array[Point] = new Array[Point](3);
	for (i <- 0 until centers.size) {       
		center(i) = (centers(i)(0),centers(i)(1));
		println(center(i));
	}
   
   var data = sc.textFile("/usr/lib/hue/input/data_set.csv")
   var latlong = data.map(line=> line.split(",")).map(l =>(l(5).toDouble,l(6).toDouble))
   
   var closest = latlong.map(p => (closestPoint(p,center),p))
   
   
   var cluster1 = closest.filter(x => (x._1 == 0)).map(y => y._2)
   var cluster2 = closest.filter(x => (x._1 == 1)).map(y => y._2)
   var cluster3 = closest.filter(x => (x._1 == 2)).map(y => y._2)
   
   var c1 = (cluster1.collect())
   var c2 = cluster2.collect()
   var c3 = cluster3.collect()

	import java.io._
	val output1 = "/usr/lib/hue/output/cluster1.txt";
	val output2 = "/usr/lib/hue/output/cluster2.txt";
	val output3 = "/usr/lib/hue/output/cluster3.txt";
	val writer1 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output1)));
	val writer2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output2)));
	val writer3 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output3)));
	for(x <- c1){
		writer1.write(x + "\n");
	}
	for(x <- c2){
		writer2.write(x + "\n");
	}
	for(x <- c3){
		writer3.write(x + "\n");
	}
	writer1.close();
	writer2.close();
	writer3.close();

   