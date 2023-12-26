# Ch.10 스파크 SQL

## 개요
- 스파크 SQL은 스파크에서 가장 중요하고 강력한 기능 중 하나.
- 스파크 SQL은 DataFrame, Dataset API에 통합되어 있다 
  - 즉 SQL, DataFrame 기능 모두 사용 가능 & 동일한 실행 코드로 컴파일)

- DB에 생성된 뷰(view)나 테이블에 SQL쿼리 실행 가능
- 시스템 함수 사용, 사용자 정의 함수 정의 가능
- 쿼리 실행 계획 분석 가능 (=> 워크로드 최적화)

## 10.1 SQL이란

- SQL (Structured Query Launguage, 구조적 질의 언어)
  - 데이터에 대한 관계형 연산을 표한하기 위한 도메인 특화 언어
  - 모든 관계형 데이터베이스에서 사용
  - NoSQL 데이터베이스에서도 쉽게 사용 가능한 변형된 자체 SQL 제공

- 스파크 SQL
  - ANSI SQL:2003 의 일부를 구현
    - 대부분의 SQL DB에서 채택하고 있는 표준 (so 유명 벤치마크도 통과)

## 10.2 빅데이터와 SQL: 아파치 하이브
- 스파크 등장 전
  - 사실상 빅데이터 SQL 접근 계층의 표준 => Hive (made by Facebook)
  - 하둡을 다양한 사업군으로 진출하는데 도움
- 스파크는 RDD를 이용하는 범용 처리 엔진으로 시작했지만, 현재는 많은 이용자가 스파크 SQL 사용 중

## 10.3 빅데이터와 SQL: 스파크 SQL
- Hive 지원하는 상위 호환 기능 지원
  - 스파크 2.0 버전. 자체 개발된 SQL 파서 포함
  - ANSI-SQL, HiveQL 모두 지원
- 스파크 SQL은 DataFrame과의 뛰어난 호환성
  - 다양한 기업에서 강력한 기능으로 자리매김할 수 있는 이유 
  - ex. Facebook 의 스파크 워크로드 (2016) (블로그 내용 p.280-281)
- 스파크 SQL의 강력함을 만드는 핵심 요인 
  - SQL 분석가) Thrift Server 나 SQL 인터페이스에 접속해, 스파크 연산 능력 활용 가능 
  - 데이터 엔지니어 & 과학자) 전체 데이터 처리 파이프라인에 스파크 SQL 사용 가능 
- 스파크 SQL (통합형 API) 으로 인해 가능해진 전체 과정 
  - SQL 로 데이터 조회
  - DataFrame으로 변환 
  - 스파크 MLlib (대규모 머신러닝 알고리즘) 수행 결과를 다른 데이터 소스에 저장 
- 단, 스파크 SQL은 OLAP 데이터베이스로 동작 (OLTP X)
  - OLAP (OnLine Analytic Processing, 온라인 분석용)
  - OLTP (OnLine Transaction Processing, 온라인 트랜잭션 처리)
  - => 따라서 매우 낮은 지연 시간이 필요한 쿼리 수행용도로는 사용 X 
  - 언젠가는 인플레이스 수정(in-place modification) 방식을 지원할수는 있으나, 현재는 사용 불가
  
### 스파크와 하이브의 관계

- 스파크 SQL은 Hive 메타스토어 사용 => 하이브와 연동 good
  - [Hive 메타스토어](https://spidyweb.tistory.com/231)는 여러 세션에서 사용할 테이블 정보 보관
  - (Hive 사용 시) 스파크 SQL 는 Hive Metastore에 접속해서 조회할 파일 수를 최소화 하기 위해 메타데이터 참조함 
    - 기존 하둡 환경의 모든 워크로드를 스파크로 이관하기 좋음 
- Hive Metastore
  - 접속하기위해 필요한 속성
    - spark.sql.hive.metastore.version (default 1.2.1)
    - spark.sql.hive.metastore.jars : HiveMetastoreClient 초기화 방식 변경
  - 스파크는 기본 버전 사용 (호환을 위해 클래스패스에 정의도 가능)
  - Hive Metastore가 저장된 다른 데이터베이스 접속 시 
    - 적합한 클래스 접두사 정의 필요 (ex. MySQL => com.mysql.jdbc)
    - 접두사 정의 후 spark.sql.hive.metastore.sharedPrefixes 속성에 설정하여 스파크, 하이브에서 공유 
  - 사용하던 하이브 메타스토어와 연동 시, [스파크 공식 문서](https://spark.apache.org/docs/latest/sql-programming-guide.html#interacting-with-different-versions-of-hive-metastore)에서 부가 정보 확인

## 10.4 스파크 SQL 쿼리 실행 방법
- 스파크가 SQL 쿼리 실행을 위하여 제공하는 인터페이스
  - (1) 스파크 SQL CLI
    - 사용하려면 스파크 디렉터리에서 다음 명령을 실행
    - ```bash 
      ./bin/spark-sql 
      ```
  - (2) 스파크의 프로그래밍 SQL 인터페이스
    - 스파크에서 지원하는 언어 API로 비정형 SQL 실행 가능
    - SparkSession 객체의 sql 메서드 사용 -> DataFrame 반환
      - 다른 트랜스포메이션과 마찬가지로 즉시 실행 X 지연 처리
    - SQL과 DataFrame은 완벽하게 연동 가능
      - DataFrame 생성 -> SQL 처리 -> DataFrame 반환
    ```python
    spark.sql("SELECT 1+1").show()
    ```
    ```text
    +-------+
    |(1 + 1)|
    +-------+
    |      2|
    +-------+
    ```
    
    ```python
    path= "./data/flight-data/json/2015-summary.json"
    spark.read.json(path).createOrReplaceTempView('some_sql_view')#DataFrame을 SQL에서 사용할 수 있도록 
    ```

    ```python
    spark.sql("""
    SELECT DEST_COUNTRY_NAME, sum(count)
    FROM some_sql_view GROUP BY DEST_COUNTRY_NAME
    """)\
      .where("DEST_COUNTRY_NAME like 'S%'").where("`sum(count)` > 10")\
      .count() # SQL => DF
    ```
    - scala랑 python이랑 같은 문법사용.

  - (3) 스파크 SQL 쓰리프트 JDBC/ODBC 서버      
    - 스파크는 JDBC(Java Database Connectivity) 인터페이스 제공
    - 사용자나 원격 프로그램은 스파크 SQL을 실행하기 위해 이 인터페이스로 스파크 드라이버에 접속
      - 가장 대표적인 활용 사례: 태블로 같은 비즈니스 인텔리전스 소프트웨어를 이용해 스파크에 접속하는 형태
    - 쓰리프트 JDBC / ODBC(Open Database Connectivity) 서버는 하이브 1.2.1 버전의 Hiveserver2에 맞추어 구현됨
      ```text
      DataFrame을 테이블 카탈로그에 등록하면 다른 스파크 애플리케이션에서도 DataFrame 이름을 이용해 이 데이터에 질의를 수행할 수 있다. 
      더욱 흥미로운 점은 스파크 외부의 써드-파티 애플리케이션에서도 표준 JDBC 및 ODBC 프로토콜로 스파크에 접속한 후, 등록된 DataFrame 테이블의 데이터에 SQL 쿼리를 수행할 수 있다는 것이다. 
      스파크 쓰리프트 서버는 이러한 원격 쿼리 기능을 지원하는 스파크 컴포넌트로, JDBC 클라이언트가 요청한 쿼리를 DataFrame API를 이용해 스파크 잡 형태로 실행한다.
      ```

## 10.5 카탈로그
- 카탈로그 (catalog) : 스파크 SQL에서 가장 높은 추상화 단계
  - 테이블에 저장된 데이터에 대한 메타데이터 + 데이터베이스, 테이블, 함수, 뷰에 대한 정보 까지 추상화
- org.apache.spark.sql.catalog.Catalog 패키지
  - 테이블, 데이터베이스, 함수 조회 등 여러 유용한 함수 제공
- 스파크 SQL을 사용하는 또 다른 방식의 프로그래밍 인터페이스 이다
  - spark.sql 함수를 사용해 관련 코드 실행 가능

## 10.6 테이블
- 스파크 SQL을 사용해 유용한 작업을 수행하려면 먼저 테이블을 정의해야함
- 테이블은 명령을 실행할 데이터의 구조라는 점에서 dataframe과 논리적으로 동일
  - 조인, 필터링, 집계 등 여러 데이터 변환 작업 수행 가능
- 스파크에서 테이블을 생성하면 default 데이터베이스에 등록됨
- 테이블을 제거하면 모든 데이터가 제거되므로 조심

### 스파크 관리형 테이블
- 테이블은 두 가지 중요한 정보를 저장함
  - 테이블의 데이터
  - 테이블에 대한 데이터(메타데이터)
- 관리형 테이블: 스파크가 모든 정보를 추적할 수 있는 테이블
- dataframe의 saveAsTable메서드로 관리형 테이블 생성 가능

### 테이블 생성하기

```python
spark.sql("CREATE TABLE flights (dest_country_name string, origin_country_name string, count long) \
          using json options (path './data/flight-data/json/2015-summary.json')")
```

```python
spark.sql('show tables in default')
```
- 데이터베이스 내부 테이블 확인가능

### 외부 테이블 생성하기
- 스파크 SQL은 완벽하게 하이브 SQL과 호환됨
- 기존 하이브 쿼리문을 스파크 SQL로 변환해야 하는 상황이 생길 수 있음
  - 이때 외부 테이블을 생성
- 외부 테이블: 디스크에 저장된 파일을 이용해 정의한 테이블
  - 스파크는 외부 테이블의 메타데이터를 관리함
  - 하지만 데이터 파일은 스파크에서 관리하지 않음
  - create external table구문으로 외부 테이블 생성 가능

- 제가 사용하는 spark에는 hive가 연결 안돼있어서.. 예제는 넘어갑니다.
  - CTAS도 지원하지 않으니, 혼자 실습해보시길

### 테이블에 데이터 삽입하기
- 데이터 삽입은 표준 SQL 문법을 따름
```python
spark.sql("INSERT INTO flights2 \
  SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count FROM some_sql_view LIMIT 20")
```
- 특정 파티션에만 저장하고 싶다면 파티션 명세 추가 가능
```python
spark.sql('''INSERT INTO partitioned_flights \
  PARTITION (DEST_COUNTRY_NAME="UNITED STATES")\
  SELECT count, ORIGIN_COUNTRY_NAME FROM flights\
  WHERE DEST_COUNTRY_NAME='UNITED STATES' LIMIT 12''')
```
### 테이블 메타 데이터 확인

```python
spark.sql("DESCRIBE flights")
```
```text
col_name	     data_type	    comment
dest_country_name	string	     NULL
origin_country_name	string	     NULL
count	                bigint	     NULL
```

### 테이블 메타데이터 갱신하기
- 테이블 메타데이터를 유지하는 것은 가장 최신의 데이터셋을 읽고 있다는 것을 보장할 수 있는 중요한 작업
- 테이블 메타데이터를 갱신할 수 있는 두 가지 명령이 있음
  - refresh table 구문: 테이블과 관련된 모든 캐싱된 항목을 갱신
  - repair table 구문: 새로운 파티션 정보를 수집하는 데 초점


### 테이블 제거
- drop 키워드로 테이블 삭제
  - 테이블 제거 시 테이블의 데이터가 모두 제거됨(외부 테이블은 예외)
- 존재하지 않는 테이블을 제거하려면 오류가 발생
  - 그래서 drop table if exists 구문 사용

```python
spark.sql("drop table if exists flights")
```

### 테이블 캐싱하기
- dataframe처럼 테이블을 캐시하거나 캐시에서 제거 가능
  - 캐시: cache table 구문
  - 캐시 제거: uncache table 구문


## 10.7 뷰
- 뷰는 기존 테이블에 여러 트랜스포메이션 작업 지정
  - 기본적으로 뷰는 ‘단순 쿼리 실행 계획’
  ```text
  "단순 쿼리 실행 계획"이란, Spark가 뷰에 대한 쿼리를 처리하기 위해 생성하는 물리적인 실행 계획이 비교적 간단하다는 것을 의미합니다. 
  이는 대부분의 데이터가 단일 노드에서 처리될 수 있고, 병렬 처리가 크게 필요하지 않은 경우에 해당합니다.
  ```
  - 쿼리 로직 체계화 & 재사용하기 편리
- 스파크가 가진 뷰에 관련된 다양한 개념
  - 전역 뷰 : 디비에 상관없이 사용 가능 / 전체 스파크 애플리케이션에서 볼 수 있음 / 세션이 종료되면 뷰도 사라짐
  - 세션별 뷰 : 현재 세션에만 사용 가능한 임시 뷰

### 뷰 생성하기
- 최종사용자 입장에서 뷰는 테이블처럼 보인다
  - 신규 경로에 모든 데이터를 다시 저장 X
  - 대신 단순하게 쿼리시점에 데이터소스에 트랜스포메이션 수행
  - 트랜스포메이션 예) filter, select, 대규모 group by, rollup

```python
#뷰 생성
spark.sql("create view just_usa_view_3 as select * from flights2 where dest_country_name = 'United States'")

#등록되지 않고 현재 세션에만 사용할 수 있는 임시 뷰 생성
spark.sql("create temp view just_usa_view as select * from flights2 where dest_country_name = 'United States'")

#전역적 임시 뷰 생성
spark.sql("create global temp view just_usa_view as select * from flights where dest_country_name = 'United States'")
```

### 뷰 제거하기
- 테이블 제거와 핵심적인 차이는 뷰는 어떤 데이터도 제거되지 않고 뷰 정의만 제거된다는 점
```python
spark.sql("drop view if exists just_usa_view_3")
```



## 10.8 데이터베이스
- 데이터 베이스 = 여러 테이블을 조직화하기 위한 도구
- 데이터베이스 미정의 시 스파크는 기본 데이터베이스 사용 (default)
- 스파크에서 실행하는 모든 SQL 명령문은 실행 중인 데이터베이스 범위에서 실행
  - 데이터베이스 변경 시, 이전에 생성한 모든 사용자 테이블은 변경 전 데이터베이스에 속해 있으므로 다르게 쿼리해야함
    - => 즉, 다른 데이터베이스 사용 시 데이터베이스 명 + 테이블 명 으로 조회할 것
- SHOW DATABASES 으로 전체 데이터베이스 목록 확인
```python
spark.sql('show databases').show()
```
```text
+---------+
|namespace|
+---------+
|  default|
+---------+
```

### 데이터베이스 생성하기
- CREATE DATABASE 사용
```python
spark.sql('create database some_db')
```
```text
+---------+
|namespace|
+---------+
|  default|
|  some_db|
+---------+
```

### 데이터베이스 설정하기
- USE {databaseName} 으로 쿼리 수행할 데이터베이스 설정
- 다른 테이블에 쿼리 수행시 접두사 사용
- SELECT current_database() : 현재 사용중인 데이터베이스 확


### 데이터베이스 제거하기
- DROP DATABASE (IF EXISTS) 사용
```python
spark.sql('drop database if exists some_db')
#현재 어떤 디비를 사용 중인지 확인
spark.sql('select current_database()').show()
```
```text
+------------------+
|current_database()|
+------------------+
|           default|
+------------------+
```

## 10.9 select 구문
- 스파크 SQL은 ANSI-SQL 요건 충족
  - SELECT 표현식 구조는 p.296 참고
- case…when…then 구문
  - SQL 쿼리 값을 조건에 맞게 처리 가능
  - (like if-else statement)

## 10.10 고급 주제

### 데이터 쿼리 방법 알아보기
- SQL 쿼리 : 특정 명령 집합을 실행하도록 요청하는 SQL 구문
- 조작, 정의, 제어 와 관련된 명령 정의 가능
  - => 본 책은 대부분 조작 관련
- 복합 데이터 타입
  - 표준SQL에는 존재하지 않는 강력한 기능
  - 스파크 SQL의 핵심 복합 데이터 타입 3가지 => 구조체, 리스트, 맵
  - 구조체 : 중첩 데이터 생성/쿼리 방법 제공
```python
spark.sql("create view if not exists nested_data as select (dest_country_name, origin_country_name) as country, count from flights2")
```
  - 리스트 : 값의 배열이나 리스트 사용
    - 집계 함수 collection_list(), collect_set() 으로 생성 가능 (단, 집계 연산 시에만 사용 가능)
      - collect_list: 값의 리스트를 만드는 함수
      - collect_set: 중복 값 없는 배열을 만드는 함수
    - ARRAY로 컬럼에 직접 배열 생성 가능
    - explode() : 저장된 배열의 모든 값을 단일 로우 형태로 분해 (<-> collect())

### 함수
- 스파크 SQL은 다양한 고급 함수 제공 

```python
# 스파크 SQL이 제공하는 전체 함수 목록
spark.sql('show functions').show(10)
```
```text
+--------+
|function|
+--------+
|       !|
|      !=|
|       %|
|       &|
|       *|
|       +|
|       -|
|       /|
|       <|
|      <=|
+--------+
only showing top 10 rows

```
```python
# 사용자 정의 함수 목록
spark.sql('show user functions').show(10)
```
```text
+--------+
|function|
+--------+
+--------+

```

### 서브 쿼리
- 서브 쿼리 (subquery) : 쿼리안에 쿼리 지정
  - SQL 내 정교한 로직 명시 가능
  - => 하나 (스칼라 서브쿼리) 이상의 결과 반환 가능?
- 스파크의 기본 서브쿼리 2가지
  - 상호연관 서브쿼리 (correlated subquery) : 쿼리 외부 범위에 있는 일부 정보 사용 가능
  - 비상호연관 서브쿼리 (uncorrelated subquery) : 외부 범위 정보 사용 X
  ```python
  # 상위 5개의 목적지 국가 정보 조회
  spark.sql('SELECT dest_country_name FROM flights2 \
  GROUP BY dest_country_name ORDER BY sum(count) DESC LIMIT 5')
  ```
  ```text
  **dest_country_name**
  United States
  Costa Rica
  Turks and Caicos ...
  Guyana
  Anguilla
  ``` 
  

- 조건절 서브쿼리 (predicate subquery) 도 지원 => 값에 따라 필터링
  - 상호연관 조건절 서브쿼리 사용
  ```python
  spark.sql('SELECT * FROM flights2 f1 WHERE EXISTS (SELECT 1 FROM flights2 f2\
              WHERE f1.dest_country_name = f2.origin_country_name)\
  AND EXISTS (SELECT 1 FROM flights2 f2\
              WHERE f2.dest_country_name = f1.origin_country_.;zxsname)')
  ```
  
  - 비상호연관 조건절 서브쿼리 사용
  ```python
  spark.sql('SELECT * FROM flights2 \
  WHERE origin_country_name IN (SELECT dest_country_name FROM flights2 \
        GROUP BY dest_country_name ORDER BY sum(count) DESC LIMIT 5)')
  ```
  
- 비상호 스칼라 쿼리(uncorrelated scalar query) 사용 시
  - 잘모르겠음..
  ```python
  from pyspark.sql import SparkSession
  
  # Spark 세션 생성
  spark = SparkSession.builder.appName("example").getOrCreate()
  
  # 데이터 생성
  data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
  columns = ["Name", "Value"]
  df = spark.createDataFrame(data, columns)
  
  # 비상호 스칼라 쿼리
  transformed_df = df.filter("Value > 1").select("Name")
  
  # 비상호 스칼라 액션
  result = transformed_df.collect()
  
  # 결과 출력
  for row in result:
      print(row)
  
  # Spark 세션 종료
  spark.stop()
  ```
  - 각각의 트랜스포메이션(Transformation) 또는 액션(Action)이 독립적으로 실행되어 상호 간의 의존성이 없음.



## +)Additional
1. [serialization](https://hub1234.tistory.com/26)
2. [Hive metastore](https://spidyweb.tistory.com/231)
3. [spark deploy](https://wooono.tistory.com/140)

