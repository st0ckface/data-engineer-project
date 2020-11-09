
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, from_json, when, year}
import org.json4s.DefaultFormats


case class ProductionCompany(name: String, id: Int)

case class Genre(name: String, id: Int)

object MovieDataTransform extends App {
  val NestedObjectSchema = "array<struct<id:INT, name:STRING>>"
  val BudgetCol = "budget"
  val RevenueCol = "revenue"
  val PopularityCol = "popularity"
  val ProductionCompaniesCol = "production_companies"
  val IdCol = "id"
  val TitleCol = "title"
  val ProfitCol = "profit"
  val GenresCol = "genres"
  val ReleaseDateCol = "release_dates"
  val ReleaseYrCol = "releaseYear"



  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[4]")
    .appName("MovieDataTransform")
    .getOrCreate()


  val csv = dataframeFromCSV("data/movies_metadata.csv")



  val typedCsv = parseDataFrame(csv)
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

  def dataframeFromCSV(path: String)(implicit sparkSession: SparkSession) = {
    spark.read
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("inferSchema", "true")
      .option("mode", "DROPMALFORMED")
      .csv(path)
  }

  def parseDataFrame(rawDataFrame: DataFrame) = {
    rawDataFrame.dropDuplicates().filter(col(TitleCol).isNotNull.and(col(IdCol).isNotNull))
      .select(
        col(IdCol).cast("long"),
        col(TitleCol),
        when(col(BudgetCol).isNull, 0L).otherwise(col(BudgetCol).cast("long")).as(BudgetCol),
        when(col(RevenueCol).isNull, 0L).otherwise(col(RevenueCol).cast("long")).as(RevenueCol),
        when(col(RevenueCol).gt(0L).and(col(BudgetCol).gt(0L)), col(RevenueCol).minus(col(BudgetCol)).cast("long")).otherwise(null).as(ProfitCol),
        when(col(PopularityCol).isNull, 0).otherwise(col(PopularityCol).cast("double")).as(PopularityCol),
        from_json(col(ProductionCompaniesCol), NestedObjectSchema, Map.empty[String, String]).as(ProductionCompaniesCol),
        from_json(col(GenresCol), NestedObjectSchema, Map.empty[String, String]).as(GenresCol),
        year(col(ReleaseDateCol)).as(ReleaseYrCol))
  }



}
