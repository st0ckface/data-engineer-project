
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ColumnNames._

object MovieDataTransform extends App {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[4]")
    .appName("MovieDataTransform")
    .getOrCreate()

  /*
    Initial import of csv data, trying to get parse it in a format that incurs the least amount of data loss
   */
  val csv = dataframeFromCSV("data/movies_metadata.csv")
  /*
   Cast / explode respective columns and filter out rows with nulls
   (null values would have been dropped previously and replaced with a valid zero value)
   */
  val typedCsv = filterMalformedRows(parseDataFrame(csv))
  /*
    Write out the Movie data
   */
  val movies = getMoviesProjection(typedCsv).coalesce(1)
  movies.write.csv("out/movies")
  /*
    Write out the production company data
   */
  val companies = getCompaniesProjection(typedCsv).coalesce(1)
  companies.write.csv("out/companies")
  /*
    Write out genre data
   */
  val genres = getGenrePojection(typedCsv).coalesce(1)
  genres.write.csv("out/genres")


  /*
    Write out movie <-> company mapping
   */
  val mtc = getMovieToCompany(typedCsv).coalesce(1)
  mtc.write.csv("out/companyIdToMovieId")

  /*
    Write out genre <-> movie mapping
   */
  val mtg = getMovieToGenre(typedCsv).coalesce(1)
  mtg.write.csv("out/genreIdToMovieId")

  spark.stop()

  def dataframeFromCSV(path: String)(implicit sparkSession: SparkSession): DataFrame = {
    sparkSession.read
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("inferSchema", "true")
      .option("mode", "DROPMALFORMED") // didnt do as well as I would have hoped at identifying malformed rows
      .csv(path)
  }

  def parseDataFrame(rawDataFrame: DataFrame): DataFrame = {
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

  def filterMalformedRows(typedDataframe: DataFrame): DataFrame = {
    typedDataframe.filter( // Make sure we didnt fail type conversions, if we did rows are malformed
      col(IdCol).isNotNull
        .and(col(ReleaseYrCol).isNotNull)
        .and(col(BudgetCol).isNotNull)
        .and(col(RevenueCol).isNotNull)
        .and(col(PopularityCol).isNotNull)
    )
  }

  def getMoviesProjection(dataFrame: DataFrame): DataFrame = {
    dataFrame.select(IdCol, TitleCol, BudgetCol, ProfitCol, RevenueCol, ReleaseYrCol, PopularityCol)
  }

  def getCompaniesProjection(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(explode(col(ProductionCompaniesCol)).as(Company))
      .select(s"$Company.$IdCol", s"$Company.$NameCol")
      .distinct()
  }

  def getGenrePojection(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(explode(col(GenresCol)).as(Genre))
      .select(s"$Genre.$IdCol", s"$Genre.$NameCol")
      .distinct()
  }

  def getMovieToCompany(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select(col(IdCol), explode(col(ProductionCompaniesCol)).as(Company))
      .select(col(s"$Company.$IdCol").as(CompanyIdCol), col(IdCol).as(MovieIdCol))
  }

  def getMovieToGenre(dataFrame: DataFrame): DataFrame = {
    dataFrame.select(col(IdCol), explode(col(GenresCol)).as(Genre))
      .select(col(s"$Genre.$IdCol").as(GenreIdCol), col(IdCol).as(MovieIdCol))
  }

}
