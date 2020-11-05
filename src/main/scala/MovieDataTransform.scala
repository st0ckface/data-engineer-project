
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, when, year, explode}
import org.json4s.DefaultFormats


case class ProductionCompany(name: String, id: Int)

case class Genre(name: String, id: Int)

object MovieDataTransform extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local[4]")
    .appName("MovieDataTransform")
    .getOrCreate()


  val csv = spark.read
    .option("header", "true")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("inferSchema", "true")
    .option("mode", "DROPMALFORMED")
    .csv("data/movies_metadata.csv")

  val schema = "array<struct<id:INT, name:STRING>>"


  val typedCsv = csv.dropDuplicates().filter(col("title").isNotNull.and(col("id").isNotNull))
    .select(
      col("id").cast("long"),
      col("title"),
      when(col("budget").isNull, 0L).otherwise(col("budget").cast("long")).as("budget"),
      when(col("revenue").isNull, 0L).otherwise(col("revenue").cast("long")).as("revenue"),
      when(col("revenue").gt(0L).and(col("budget").gt(0L)), col("revenue").minus(col("budget")).cast("long")).otherwise(null).as("profit"),
      when(col("popularity").isNull, 0).otherwise(col("popularity").cast("double")).as("popularity"),
      from_json(col("production_companies"), schema, Map.empty[String, String]).as("productionCompanies"),
      from_json(col("genres"), schema, Map.empty[String, String]).as("genres"),
      year(col("release_date")).as("releaseYear"))
    .filter( // Make sure we didnt fail type conversions, if we did rows are malformed
      col("id").isNotNull
        .and(col("releaseYear").isNotNull)
        .and(col("budget").isNotNull)
        .and(col("revenue").isNotNull)
        .and(col("popularity").isNotNull)
    )


  /*
    Write out the Movie data
   */
  typedCsv.select("id", "title", "budget", "profit", "releaseYear", "popularity").coalesce(1).write.csv("out/movies")
  /*
    Write out the production company data
   */
  typedCsv.select(explode(col("productionCompanies")).as("company")).select("company.id", "company.name")
    .distinct()
    .coalesce(1)
    .write.csv("out/companies")
  /*
    Write out genre data
   */
  typedCsv.select(explode(col("genres")).as("genre")).select("genre.id", "genre.name")
    .distinct()
    .coalesce(1)
    .write.csv("out/genres")
  /*
    Write out movie <-> company mapping
   */
  typedCsv.select(col("id"), explode(col("productionCompanies")).as("company"))
    .select(col("company.id").as("companyId"), col("id").as("movieId"))
    .coalesce(1)
    .write.csv("out/companyIdToMovieId")
  /*
    Write out genre <-> movie mapping
   */
  typedCsv.select(col("id"), explode(col("genres")).as("genre"))
    .select(col("genre.id").as("genreId"), col("id").as("movieId"))
    .coalesce(1)
    .write.csv("out/genreIdToMovieId")

  spark.stop()

}
