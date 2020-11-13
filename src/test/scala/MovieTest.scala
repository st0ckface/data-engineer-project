import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import MovieDataTransform._
import ColumnNames._
import org.apache.spark.sql.functions._
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}


class MovieTest extends AnyFunSuite with DataFrameComparer with BeforeAndAfterEach{
  implicit var spark: SparkSession = _
  override def beforeEach() = {
    SparkSession
      .builder()
      .master("local[4]")
      .appName("MovieDataTest")
      .getOrCreate()
  }

  override def afterEach(): Unit = {
    spark.close()
  }

  test("Ensure SQL query v DataFrame equivalence") {
    val csv = dataframeFromCSV("data/movies_metadata.csv")
    val data = filterMalformedRows(parseDataFrame(csv))
    //val data = filterMalformedRows(parseDataFrame(dataframeFromCSV("data/movies_metadata.csv")))
    val movies = getMoviesProjection(data)
    movies.createTempView("movies")
    val companies = getCompaniesProjection(data)
    companies.createTempView("companies")
    val mtc = getMovieToCompany(data)
    mtc.createTempView("movieToCompany")

    //production company budget per year
     val topProductionCompanyRev =  """
      |SELECT c.name, m.releaseYear, sum(revenue) AS totalRev
      |FROM movieToCompany mtc
      |INNER JOIN movies m, companies c WHERE c.id = mtc.companyId AND m.id = mtc.movieId
      |GROUP BY c.name, m.releaseYear
      |ORDER BY totalRev DESC LIMIT 10;
      |""".stripMargin
    val topRevFromSql = spark.sql(topProductionCompanyRev)
    val topRevFromDF = mtc.join(movies, mtc(MovieIdCol) === movies(IdCol))
      .select(mtc(CompanyIdCol), movies(RevenueCol), movies(ReleaseYrCol))
      .groupBy(mtc(CompanyIdCol), movies(ReleaseYrCol))
      .agg(sum(movies(RevenueCol)).as("totalRev"))
      .join(companies, companies(IdCol) === mtc(CompanyIdCol))
      .select(companies(NameCol), movies(ReleaseYrCol), col("totalRev"))
      .orderBy(col("totalRev").desc)
      .limit(10)

    assertLargeDataFrameEquality(topRevFromDF, topRevFromSql)
  }
}