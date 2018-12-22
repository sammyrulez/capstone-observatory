package observatory

object Main extends App {


  val temps =  Extraction.locateTemperatures(1975,"/stations.csv","/1975.csv")

  print(temps.head)

}
