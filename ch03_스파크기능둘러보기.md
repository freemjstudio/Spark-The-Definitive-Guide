# 3.1. 운영용 애플리케이션 실행하기
* spark-submit 명령을 사용해 대화형 인터프리터에서 개발한 프로그램을 운영용 애플리케이션으로 전환이 가능하다.
* spark-submit 명령 실행 시 애플리케이션 코드를 클러스터에 전송해 실행시킨다.
* 클러스터에 제출된 애플리케이션은 작업이 종료되거나 에러가 발생할 때까지 실행한다.
* 예시
  ```text 
  /bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master local \
  ./examples/jars/spark-examples_2.11-2.2.0.jar 10
  ```

## 3.2. Dataset: 타입 안정성을 제공하는 구조적 API
* 데이터셋은 자바와 스칼라의 정적 데이터 타입(statically typed)에 맞는 코드를 지원하기 위해 고안된 스파크의 구조적 API이다.
* 타입 안전성을 지원하며 동적 타입 언어인 파이썬과 R에서는 사용할 수가 없다.
* 데이터 프레임은 Row 타입의 객체로 구성된 분산 컬렉션이지만 데이터셋의 경우에는 각 레코드를 자바나 스칼라로 정의한 클래스에 할당하는 고정 타입형 분산 컬렉션이다.
* 타입 안정성을 지원하므로 초기화에 사용하는 클래스 대신 다른 클래스를 사용해 접근할 수 없다.
* 예시
  ```text
  case class Flight(dest_country_name: String,
                 origin_country_name: String,
                 count: BigInt)
  ```
  ```text
  defined class Flight
  ```
  ```text
  val flights = flightData2015.as[Flight]
  ```
  ```text
  flights: org.apache.spark.sql.Dataset[Flight] = [DEST_COUNTRY_NAME: string, ORIGIN_COUNTRY_NAME: string ... 1 more field]
  ```
  ```text
  flights
  .filter(flight_row => flight_row.origin_country_name != "Canada")
  .map(flight_row => flight_row)
  .take(5)  
  ```
  ```text
  res19: Array[Flight] = Array(Flight(United States,Romania,15), Flight(United States,Croatia,1), Flight(United States,Ireland,344), Flight(Egypt,United States,15), Flight(United States,India,62))
  ```
* collect 또는 take 메서드를 호출하면 데이터셋에 매개변수로 지정한 타입의 객체를 반환하게 된다.
* 코드 변경 없이 타입 안정성을 보장할 수 있으며 로컬이나 분산 클러스터 환경에서 데이터를 안전하게 다룰 수 있다.

# 3.3. 구조적 스트리밍
* 스파크 2.2 버전에서 안정화(production-ready) 된 스트림 처리용 고수준 API이다.
  * 구조적 API로 개발된 배치 모드의 연산을 스트리밍 방식으로 변경하여 실행할 수 있으며 지연 시간을 줄이고 증분 처리할 수 있다.
  * 예시
    * 소매 데이터셋 불러오기
      ```text
      val staticDataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("dbfs:/FileStore/data/reatail-data/by-day/by-day/*.csv")
      ```
      ```text
      staticDataFrame:org.apache.spark.sql.DataFrame
      InvoiceNo:string
      StockCode:string
      Description:string
      Quantity:integer
      InvoiceDate:timestamp
      UnitPrice:double
      CustomerID:double
      Country:string
    
      staticDataFrame: org.apache.spark.sql.DataFrame = [InvoiceNo: string, StockCode: string ... 6 more fields]
      ```
    * 소매 데이터셋 출력하기
      ```text
      staticDataFrame.show(false)
      ```
      ```text
      +---------+---------+-----------------------------------+--------+-------------------+---------+----------+--------------+
      |InvoiceNo|StockCode|Description                        |Quantity|InvoiceDate        |UnitPrice|CustomerID|Country       |
      +---------+---------+-----------------------------------+--------+-------------------+---------+----------+--------------+
      |537226   |22811    |SET OF 6 T-LIGHTS CACTI            |6       |2010-12-06 08:34:00|2.95     |15987.0   |United Kingdom|
      |537226   |21713    |CITRONELLA CANDLE FLOWERPOT        |8       |2010-12-06 08:34:00|2.1      |15987.0   |United Kingdom|
      |537226   |22927    |GREEN GIANT GARDEN THERMOMETER     |2       |2010-12-06 08:34:00|5.95     |15987.0   |United Kingdom|
      |537226   |20802    |SMALL GLASS SUNDAE DISH CLEAR      |6       |2010-12-06 08:34:00|1.65     |15987.0   |United Kingdom|
      |537226   |22052    |VINTAGE CARAVAN GIFT WRAP          |25      |2010-12-06 08:34:00|0.42     |15987.0   |United Kingdom|
      |537226   |22705    |WRAP GREEN PEARS                   |25      |2010-12-06 08:34:00|0.42     |15987.0   |United Kingdom|
      |537226   |20781    |GOLD EAR MUFF HEADPHONES           |2       |2010-12-06 08:34:00|5.49     |15987.0   |United Kingdom|
      |537226   |22310    |IVORY KNITTED MUG COSY             |6       |2010-12-06 08:34:00|1.65     |15987.0   |United Kingdom|
      |537226   |22389    |PAPERWEIGHT SAVE THE PLANET        |6       |2010-12-06 08:34:00|2.55     |15987.0   |United Kingdom|
      |537227   |22941    |CHRISTMAS LIGHTS 10 REINDEER       |2       |2010-12-06 08:42:00|8.5      |17677.0   |United Kingdom|
      |537227   |22696    |WICKER WREATH LARGE                |6       |2010-12-06 08:42:00|1.95     |17677.0   |United Kingdom|
      |537227   |22193    |RED DINER WALL CLOCK               |2       |2010-12-06 08:42:00|8.5      |17677.0   |United Kingdom|
      |537227   |21212    |PACK OF 72 RETROSPOT CAKE CASES    |120     |2010-12-06 08:42:00|0.42     |17677.0   |United Kingdom|
      |537227   |21977    |PACK OF 60 PINK PAISLEY CAKE CASES |48      |2010-12-06 08:42:00|0.55     |17677.0   |United Kingdom|
      |537227   |84991    |60 TEATIME FAIRY CAKE CASES        |48      |2010-12-06 08:42:00|0.55     |17677.0   |United Kingdom|
      |537227   |21213    |PACK OF 72 SKULL CAKE CASES        |48      |2010-12-06 08:42:00|0.55     |17677.0   |United Kingdom|
      |537227   |21080    |SET/20 RED RETROSPOT PAPER NAPKINS |12      |2010-12-06 08:42:00|0.85     |17677.0   |United Kingdom|
      |537227   |22632    |HAND WARMER RED RETROSPOT          |48      |2010-12-06 08:42:00|2.1      |17677.0   |United Kingdom|
      |537227   |22315    |200 RED + WHITE BENDY STRAWS       |12      |2010-12-06 08:42:00|1.25     |17677.0   |United Kingdom|
      |537227   |21232    |STRAWBERRY CERAMIC TRINKET BOX     |12      |2010-12-06 08:42:00|1.25     |17677.0   |United Kingdom|
      +---------+---------+-----------------------------------+--------+-------------------+---------+----------+--------------+
      only showing top 20 rows
      ```
    * SQL 코드 활용을 위해 TempView 생성
      ```text
      staticDataFrame.createOrReplaceTempView("retail_data")
      val staticSchema = staticDataFrame.schema
      ```
      ```text
      staticSchema: org.apache.spark.sql.types.StructType = StructType(StructField(InvoiceNo,StringType,true),StructField(StockCode,StringType,true),StructField(Description,StringType,true),StructField(Quantity,IntegerType,true),StructField(InvoiceDate,TimestampType,true),StructField(UnitPrice,DoubleType,true),StructField(CustomerID,DoubleType,true),StructField(Country,StringType,true))
      ```
  * 윈도우 함수는 집계 시에 시계열 컬럼을 기준으로 각 날짜에 대한 전체 데이터를 가지는 윈도우를 구성한다.
  * 윈도우는 간격을 통해 처리 요건을 명시할 수 있기 때문에 날짜와 타임스탬프 처리에 유용하다.
    ```text
    import org.apache.spark.sql.functions.{window, col}
      
    staticDataFrame
    .selectExpr("CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
    .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day"))
    .sum("total_cost")
    .show(5, false)
    ```
    ```text
    +----------+------------------------------------------+------------------+
    |CustomerId|window                                    |sum(total_cost)   |
    +----------+------------------------------------------+------------------+
    |13408.0   |{2010-12-01 00:00:00, 2010-12-02 00:00:00}|1024.6800000000003|
    |17460.0   |{2010-12-01 00:00:00, 2010-12-02 00:00:00}|19.9              |
    |16950.0   |{2010-12-07 00:00:00, 2010-12-08 00:00:00}|172.0             |
    |13269.0   |{2010-12-05 00:00:00, 2010-12-06 00:00:00}|351.43            |
    |12647.0   |{2010-12-05 00:00:00, 2010-12-06 00:00:00}|372.0             |
    +----------+------------------------------------------+------------------+
    only showing top 5 rows
    ```
  * 스트림 생성하기
    ```text
    val streamingDataFrame = spark.readStream
    .schema(staticSchema)
    .option("maxFilesPerTrigger", 1)
    .format("csv")
    .option("header", "true")
    .load("dbfs:/FileStore/data/reatail-data/by-day/by-day/*.csv")
      
    ```
    ```text
    streamingDataFrame: org.apache.spark.sql.DataFrame = [InvoiceNo: string, StockCode: string ... 6 more fields]
    ```
    ```text
    streamingDataFrame.isStreaming
    ```
    ```text
    res25: Boolean = true
    ```
  * 스트리밍 액션 또한 지연 연산이기 때문에 실제 연산을 수행하기 위해서는 액션을 호출해야 한다.
  * 스트리밍 액션은 count 메서드와 같은 액션에 해당하는 메서드를 호출하는 것보다는 트리거가 실행된 다음 데이터를 갱신하게 될 인메모리 테이블에 데이터를 저장하게 된다.
  * 아래 예제의 경우에는 파일마다 트리거를 실행하도록 구성한다.
  * 스파크는 이전 집계값보다 더 큰 값이 발생할 경우에만 인메모리 테이블을 갱신하므로 언제나 가장 큰 값을 얻을 수 있다.
    ```text
    val purchaseByCustomerPerHour = streamingDataFrame
    .selectExpr("CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
    .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 Day"))
    .sum("total_cost")
    ```
    ```text
    purchaseByCustomerPerHour: org.apache.spark.sql.DataFrame = [CustomerId: double, window: struct<start: timestamp, end: timestamp> ... 1 more field]
    ```
    ```text
    purchaseByCustomerPerHour.writeStream
    .format("memory")                // 인메모리 테이블에 저장
    .queryName("customer_purchases") // 인메모리에 저장할 테이블명
    .outputMode("complete")          // 모든 카운트 수행 결과를 테이블에 저장
    .start()
    ```
    ```text
    spark.sql("""select * from customer_purchases order by `sum(total_cost)` desc""").show(5, false)
    ```
    ```text
    +----------+------------------------------------------+------------------+
    |CustomerId|window                                    |sum(total_cost)   |
    +----------+------------------------------------------+------------------+
    |18102.0   |{2010-12-07 00:00:00, 2010-12-08 00:00:00}|25920.37          |
    |null      |{2010-12-06 00:00:00, 2010-12-07 00:00:00}|23395.099999999904|
    |null      |{2010-12-03 00:00:00, 2010-12-04 00:00:00}|23021.99999999999 |
    |null      |{2010-12-09 00:00:00, 2010-12-10 00:00:00}|15354.279999999955|
    |null      |{2010-12-01 00:00:00, 2010-12-02 00:00:00}|12584.299999999988|
    +----------+------------------------------------------+------------------+
    only showing top 5 rows
    ```

# 3.4. 머신러닝과 고급분석
* 스파크는 내장된 머신러닝 알고리즘 라이브러리인 MLib를 사용해 대규모 머신러닝을 수행할 수 있다.
* MLib를 사용하면 preprocessing, muging, model training, predection 등 다양한 기능을 수행할 수 있다.
* 그 외 자세한 내용은 책을 참고

# 3.5. 저수준 API
* 스파크는 RDD를 통해 자바와 파이썬 객체를 다루는 데 필요한 다양한 기본 기능을 제공한다.
* 스파크의 모든 기능은 RDD를 기반으로 만들어졌으며 데이터 프레임 연산 또한 RDD 기반으로 구현이 되어있다. (매우 효율적인 분산 처리를 위해 저수준 명령으로 컴파일 된다._
* 원시 데이터를 읽거나 다루는 용도로 RDD를 사용할 수 있으며 파티션과 같은 물리적 실행 특성을 결정할 수 있으므로 데이터 프레임보다 세밀한 제어가 가능하다.
* RDD는 스칼라 뿐만 아니라 파이썬에서도 사용이 가능하다. 하지만 두 언어의 RDD 구현 방식은 완전히 동일하지 않으며 조금 차이가 있다.
* 예시
  ```text
  spark.sparkContext.parallelize(Seq(1,2,3))
  ```
  ```text
  spark.sparkContext.parallelize(Seq(1,2,3)).toDF().show()
  ```
  ```text
  +-----+
  |value|
  +-----+
  |    1|
  |    2|
  |    3|
  +-----+
  ```
  
# 3.6. SparkR
* SparkR은 스파크를 R 언어로 사용하기 위한 기능으로 스파크가 지원하는 모든 언어에 적용된 원칙과 동일하다.

# 3.7. 스파크의 에코시스템 패키지
* 스파크 커뮤니티에는 만들어낸 패키지 에코시스템과 다양한 기능이 존재한다.
* 스파크 패키지 목록은 누구나 자신이 개발한 패키지를 저장할 수 있는 spark-packages-org에서 확인이 가능하다.