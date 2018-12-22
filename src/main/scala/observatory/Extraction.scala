package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.scalalang.typed

/**
  * 1st milestone: data extraction
  */
object Extraction {

  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Extraction")
      .config("spark.master", "local")
      .getOrCreate()

  import spark.implicits._

  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    sparkLocateTemperatures(year,stationsFile,temperaturesFile).collect().toSeq
  }

  def sparkLocateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): RDD[(LocalDate, Location, Temperature)] = {
    val stations = spark.sparkContext.textFile(fsPath(stationsFile))
    val temperatures = spark.sparkContext.textFile(fsPath(temperaturesFile))

    val stationsData = stations
      .map(_.split(",").to[Array])
      .map(toStation).filter(_.isDefined).map(_.get)


    val  temperaturesData = temperatures // skip the header line
      .map(_.split(",").to[List])
      .map(toTemps(year))

    temperaturesData.join(stationsData).mapValues( t => (t._1._1, t._2,t._1._2)).values



  }



  def toStation(data:Array[String]): Option[(String,Location)] = {
    data match {
      case Array(stn, wban, lat, lon) => Some((buildID(stn, wban), Location(lat.toDouble, lon.toDouble)))
      case _ => None
    }


  }

  def buildID(stn:String,wban:String): String = {
    stn + "-"+ wban
  }

  def toTemps(year: Year)(data:List[String]): (String,(LocalDate,Temperature)) = {
    val id = data.take(2).mkString("-")
    def toCelcius(f: Double): Double = (f - 32.0) * (5.0/9.0)

    (id,( LocalDate.of(year,data(2).toInt,data(3).toInt),toCelcius(data(4).toDouble)))

  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    val data = spark.sparkContext.parallelize(records.toSeq).toDS()

    data.groupByKey(_._2).agg(typed.avg(_._3)).collect().toSeq

  }

}
