# How to run
`sbt run`

# Data Storage
1. Data model provided is normalized, because it seemed we wanted to be able to query movies by two different multivalue fields (production company and genre)
    *  Depending on the users of this data, it might make sense to precompute all the queires and store the aggregated values.
    * At that point, each time a file is pulled (monthly), the same values would be computed and appended to the previous running totals.
1. Tradeoffs (Storage/Compute/Latency)
    * This data format is space effcient, but everytime a genre or production company is queried for stats the answer is recalculated although it only changes monthly.
    * The downside to rolling up these queries in advance is you would lose information, the dimensions (movie title for instance) that went into the most profitable genre in 2009 would be lost
    * If these queries are low volume, then it might not be worth precomputing the values. You could waste time/money doing that rollup for values that are potentially never used.
    * Likewise, if this is an extremely high volume service, it would make sense to pre compute these values and store them as such, maybe along side the normalized view (which could still be used for answering other types of questions)
    * An alternative, which is much lighter on ingest than the normalized data model, would be to perisist the exploded view (cross product between companies and genres). The storage footprint would be less efficient, but the query burden wouldnt be as heavy as it would be if the values werent exploded. 
1. Monitoring
    * Data quality - It would be good to keep a metric on movies in vs movies out, to understand how many records are being dropped due to formatting issues
    * latencies - What queries are generating the slowest responses? Could those be selectively pre-aggregated if needed?
1. Scaling
    * Ingest - Using a framework like spark, the task for generating this data model could easily be scaled out
    * Query - SQL databases more or less scale vertically, if this is an extremely high throughput service it would make sense to store a denarmalized format of this data or better yet, pre-aggregate.
1. Failure Recovery
    * Seeing as the data is being ingested montly, ingest would be a scheduled job. If you miss that schedule (due to failure, etc), Alert->Investigate->Remediate->Rerun
    * Any recovery from a failure in the query layer would highly depend on what data store is serving the queries, these should be adequately monitored
1. Authorization and Authentication
    * If this data were to be deemed sensitive, access to it could be isolated to only what a user is authorized to do (and only after they've been authenticated). This could be done by using seperate Databases (or Indices if you're using Elasticsearch or Keyspace if you're ugin C*)

1. Testing
    * To test these transformations I would do so on a subset of the data.
        * `csv.limit(50).collectAsList()`, then on a row by row bases build these aggregates in a HashMap that would represent each metric being queried.
        * After thats done, operating on the same subset of data, load each table (Movies, Companies, Genres, and each mapping table) in memeory `spark.createOrReplaceTempView(...)` and execute sql statements for each query and validate the results are the same

1. Next steps:
    * If for some reason, it were decided that this is exactly the data model we want, we  would want to productionize this.
        * Running the same tests on each incoming file before making augmenting our production data store
        * We would need a reliable way to orchestrate and monitor this batch process. Spark scales well, especially for this type of use case (minus the coalesce, which was just to make the results easier to view in this scenario).
        A hands off, it just works, approach might look like a Lambda that is triggered when an object appears in s3, that could kick off a container that handled test, and upon successful completion of that testing, trigger an EMR job to run the spark on the input files.
    * But if the data volume were to explode, the computational effort of the current solution would likely be a burden. We would need to investigate steps to remediate:
        * Can we get the genres and prodcution company info as a separate asset? The current way they are parsed our of the Movie data is inefficient
        * We know what we thought our query pattern was, but what is it in practice? Have needs of the business changed? Is a relational model needed? Is it the most efficient? (discussed in tradeoffs as well)

