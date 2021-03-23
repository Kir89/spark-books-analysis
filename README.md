# Example of data analysis with Spark 3

## Introduction

In this recreational project we will use Spark 3 to:
 - analyze a dataset of books (https://www.kaggle.com/meetnaren/goodreads-best-books?select=book_data.csv)
 - load the first 1000 books (ordered by weighted rating) to a DB
 - compute the top 10 list of genre for the books, starting from the data we previously wrote to DB

We will do everything from scratch so you may find this useful for:
 - Understanding the basics of SparkSQL API in a simple, working, example
 - Install all the necessary tools (IDE, build tools, DB, DB drivers...)
 - Understanding how many issues you can have when you work on random datasets out there (but you already know this, don't you?)

## Tools and setup

In this document we will see how to run the code and the analysis, but first we will need to setup the environment.

For this project the used tools are:
 - **Spark 3.1.1** (https://spark.apache.org/docs/3.1.1/): the unified analytics engine for large-scale data processing. Used:
  - from Maven, as dependency
  - from binaries, in order to use the *spark-shell*.

  I prefer to use Spark from Scala with respect to Python because Scala is a static-typed language, while Python is dynamically typed, and in my opinion this makes Scala a more readable language (if you make types explicit of course) and makes it easier to handle bigger projects.
 - **MariaDB 10.5.9** (https://mariadb.com/kb/en/documentation/): relational DB, known to be born from the same authors of MySQL. I chose this DB because it's one of the most popular DBs, opensource, and, last but not least, cloud ready. In fact, you can find it for example in AWS (https://mariadb.com/kb/en/mariadb-on-amazon-rds/).
 - **Apache Maven 3.6.3** (https://maven.apache.org/guides/getting-started/index.html): build tool. Since my first approach to Scala and Spark, I found SBT less documented and stable, so I preferred to use Maven. That's probably because SBT is newer than Maven, so things may be changed now, but I usually stick to Maven when it comes to builds.
 - **IntelliJ IDEA 2020.1.1 (Community Edition)** (https://www.jetbrains.com/help/idea/2020.1/discover-intellij-idea.html): IDE. In my opinion one of the best IDEs out there. It's complex and resource consuming, but it has a lot of useful functionalities and default keyboard shortcuts are comfortable.
 - **GIT 2.25.1** (https://git-scm.com/docs): code versioning system. Nothing else to say.
 - **Atom** (https://atom.io/): the text editor used to write this documentation. Nice interface, lots of packages, plugins and functionalities.

### Architecture

This is a simple diagram of the architecture of this example

![Books Analysis Architecture](diagrams/BooksAnalysisSpark_arch_whiteBackground_v1.1.png)

The Spark application will have two jobs:
1. The ***load*** job, that will take the source csv as input, make some computations, and load it to the DB, via JDBC connection. The computation will compute the first 1000 books based on some rating.
2. The ***retrieve*** job, that will take the data stored in the DB as input, and do some more computation, in order to create the top 10 genres. This job will output the top list on screen.

#### Why JDBC
As you can see, the amount of data to be transfered to/from the DB is very small. For this reason we can stick to a JDBC connection, usually slower than other methods, but it allows us to easily change the DB by changing the application's configuration and the JDBC driver. For higher throughput MariaDB supports a Columnar Storage engine https://mariadb.com/kb/en/mariadb-columnstore/ with dedicated Bulk Data Adapters, but this is a specific implementation for MariaDB, it would be overkilling, and would not allow us to change the DB so easily. The latter could be useful in some situations.

### Environment setup

I will assume you will work on Debian based Linux distribution (I use Lubuntu). For this reason, some commands (e.g. install new packages) may differ from your O/S version. I will also assume you have already Java Runtime and SDK installed (IntelliJ IDEA already provides a Java JDK). Otherwise you can install it following online tutorials, like this https://linuxize.com/post/install-java-on-ubuntu-18-04/.

NOTE: I will not focus on the installation of IDE and Text Editor. You can find useful documentation/instructions on the links mentioned above.

1. **Data source**. Download the file "book_data.csv.zip" at https://www.kaggle.com/meetnaren/goodreads-best-books?select=book_data.csv. Note: Kaggle account needed. Put the downloaded file in the desired directory and unzip it
```bash
unzip book_data.csv.zip
```
This file contains book data organized in the following columns:
 - **book_authors**: The author(s) of the book, separated by '|'
 - **book_desc**: A description of the book, as found on the Goodreads web page of the book
 - book_edition: Edition of the book
 - book_format: Format of the book, i.e., hardcover, paperback, etc.
 - book_isbn: ISBN of the book, if found on the Goodreads page
 - book_pages: No. of pages
 - **book_rating**: Average rating given by users
 - **book_rating_count**: No. of ratings given by users
 - book_review_count: No. of reviews given by users
 - **book_title**: Name of the book
 - **genres**: Genres that the book belongs to; This is user-provided information. The field, as the "book_authors" is a list separated by '|'
 - **image_url**: URL of the book cover image

  The input fields that will be loaded in the output DB are highlighted in **bald**.

2. **JDBC Driver**. This implementation leverages JDBC connection for the reason explained in the *Architecture* paragraph. Download the JDBC Driver here https://mariadb.com/downloads/#connectors and keep it in a location of your preference. We will use it later.

3. **Spark**. This is needed if you want to explore the data via spark-shell, otherwise you can skip this part because Spark will be downloaded from Maven as a dependency. You can download Spark from here https://spark.apache.org/downloads.html, then you need to extract the file in the desired location. If you would like to test using the JDBC connection you will also have to put the JDBC Driver (see step *2*) in the *jars* folder inside the base folder of your downloaded Spark Distribution. You can then load a JDBC driver with spark-shell with the following code (assuming 2.7.2 MariaDB JDBC version):
```bash
./bin/spark-shell --driver-class-path jars/mariadb-java-client-2.7.2.jar --jars jars/mariadb-java-client-2.7.2.jar
```
Otherwise you can just run the *spark-shell* by issuing the simple command:
```bash
./bin/spark-shell
```

4. **MariaDB**. As mentioned above, for this example project MariaDB has been chosen as the DB where the result of the computation will be stored.
 - Download from here https://mariadb.org/download/ the version 10.5.9 (or the version you prefer) of MariaDB
 - Check the file with the checksum
 - Untar the file in the location you prefer
 ```bash
  tar -xzvf mariadb-${downloaded-version}.tar.gz
  ```
 - Follow the instruction here: https://mariadb.com/kb/en/installing-system-tables-mysql_install_db/. You need to run the install script from the base folder of the mysql installation.
 ```bash
 #Install
 ./scripts/mysql_install_db --user=mysql
 #Start (Set SQL mode to allow I/O from Spark via JDBC)
 ./bin/mysqld_safe --sql-mode='ANSI_QUOTES' --datadir='./data'
 #Test setup
 cd './mysql-test' ; perl mysql-test-run.pl
 ```
 - Connect to mysql to the test DB
 ```bash
 mysql test
 #After the above command you should enter the MariaDB console. Type 'exit;' to quit. If you get any errors about libncurses5 (like I did) try
 sudo apt-get update
 sudo apt-get install libncurses5
 #Then try to get to MariaDB's console again
 #After connection to MariaDB's console, check if you can see databases
 show databases;
 #Then quit MariaDB console
 exit;
 ```
 - Shutdown MariaDB. At the end of your day (not now :) ) you might want to shutdown the MariaDB server. Use the command *SHUTDOWN* to stop MariaDB server (from MariaDB console).

5. **MariaDB: database**. We are now ready to setup MariaDB database and table to store the output data
 ```bash
 #Open mysql client
 mysql
 #Create database. Character set is "unconventional" because of special characters in the data set
 CREATE DATABASE IF NOT EXISTS books_example
 CHARACTER SET = 'utf8mb4'
 COLLATE = 'utf8mb4_general_ci';
 #Create table (column sizes were determined with data analysis. See 'Data considerations' paragraph, nr. 5)
 use books_example;
 CREATE TABLE IF NOT EXISTS ranking (
 book_title VARCHAR(1024),
 book_weight_rating FLOAT,
 book_desc TEXT,
 book_authors VARCHAR(1024),
 genres VARCHAR(512),
 image_url VARCHAR(2048),
 book_rating FLOAT,
 book_rating_count INT
 );
 ```

6. **MariaDB: user and grants**
```bash
#Issue all commands after logging in the MariaDB console
CREATE USER 'sparkjob'@'%';
GRANT INSERT ON books_example.ranking to 'sparkjob'@'%';
GRANT SELECT ON books_example.ranking to 'sparkjob'@'%';
SET PASSWORD FOR 'sparkjob'@'%' = PASSWORD('${pwd}');
```

7. **Maven**
```bash
sudo apt-get update
sudo apt install maven
```

8. **GIT and code download**. You can follow the instructions here https://www.atlassian.com/git/tutorials/install-git#linux in order to install GIT. Then use the command *clone* to copy the repository in a folder you prefer.
```bash
git clone https://github.com/Kir89/spark-books-analysis.git
```

9. **Config file**. The config file contains the details of the Spark Application and the details of DB connection. You can find a template of the configuration, in the resources path. It's called *application-confexample.conf* and you can start from that to make your own configuration (e.g. changing the DB). In any case you will need to change *username* and *password*. Here is how it's organized:
```json
it.kirsoft.examples.booksanalysisspark {
  spark {
    app-name = "books-analysis-spark"
    master = "local[2]"
    log-level = "INFO"
  }
  db {
    url = "jdbc:mariadb://localhost:3306/books_example"
    dbtable = "ranking"
    username = "yourusername"
    password = "yourpassword"
  }
}
```

10. **Build!**
```bash
mvn clean package
```

11. **Run!**. As stated before you have two possible jobs to execute. You need to run following commands to execute them. As you can see, you need to provide the JDBC Driver in the classpath, then the main class to use. The arguments follow this logic:
   * 1st param: name of config file to use
   * 2nd param: job to run. load|retrieve
   * 3rd param: input file path (only needed and used if "load" job executed)
```bash
#Load job
java -cp "booksanalysisspark-1.0-jar-with-dependencies.jar:${LOCATION_FOR_mariadb-java-client-2.7.2.jar}" it.kirsoft.examples.booksanalysisspark.Main application.conf load ${LOCATION_FOR_book_data.csv}
#Retrieve job
java -cp "booksanalysisspark-1.0-jar-with-dependencies.jar:${LOCATION_FOR_mariadb-java-client-2.7.2.jar}" it.kirsoft.examples.booksanalysisspark.Main application.conf retrieve
```

12. **What now?**. At the end of the executions you'll still find the data stored on the DB, so you could:
 - Query the data from MariaDB console
 - Use spark-shell to analyze source data file or data stored in MariaDB
 - Change the code and experiment something new
 - Go on reading to know more about the project

## Data considerations

One of the first things to do when dealing with data is to know the data. Here are some insights I found while working on the input dataset that were useful to define the implemented computational steps.

1. **Reading input data**: double quotes "" create problems when parsing the csv. As a consequence I had some *null* values on casted book ratings and book rating's count. *null* values were partially solved using *.option("escape", """"""")* while reading the csv with Spark. Even with this, there are still some rows that are unusable and must be filtered because of null values on casted values. There are probably other problems in the data that need to be discovered.

2. **Ratings outliers**: there are outliers in the dataset. From the following analysis the rating between 0 and 5 have been kept in

```
casted.select("book_rating_casted").summary().show
+-------+--------------------+                                                  
|summary|  book_rating_casted|
+-------+--------------------+
|  count|               53307|
|   mean|  7.70620028027621E9|
| stddev|2.744357480205038E11|
|    min|                 0.0|
|    25%|                3.83|
|    50%|                4.03|
|    75%|                4.22|
|    max|        9.7818604E12|
+-------+--------------------+
```

3. **Rating's count outliers**: there are outliers in the rating counts too. The following analysis is the proof of that. Accordingly to  https://en.wikipedia.org/wiki/List_of_best-selling_books#More_than_100_million_copies, the top selling book in history sold 120 million copies. Not all the readers will rate the book, but I will still assume 500 million as maximum reasonable number of ratings, to remove outliers and make the algorithm to work in future too.

```
casted.select("book_rating_count_casted").summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "80%", "85%", "90%", "91%", "92%", "93%", "94%", "95%", "96%", "97%", "98%", "99%", "99.5%", "99.6%", "99.7%", "99.8%", "99.9%", "max").show(30)
+-------+------------------------+                                              
|summary|book_rating_count_casted|
+-------+------------------------+
|  count|                   53347|
|   mean|     6.783893445170276E9|
| stddev|    2.575038893285924E11|
|    min|                     0.0|
|    25%|                   391.0|
|    50%|                  2753.0|
|    75%|                 12582.0|
|    80%|                 18571.0|
|    85%|                 29854.0|
|    90%|                 55953.0|
|    91%|                 66457.0|
|    92%|                 79979.0|
|    93%|                 96911.0|
|    94%|                123692.0|
|    95%|                160839.0|
|    96%|                209764.0|
|    97%|                321251.0|
|    98%|                534082.0|
|    99%|                961873.0|
|  99.5%|               1803054.0|
|  99.6%|               2077829.0|
|  99.7%|               2298477.0|
|  99.8%|               2423960.0|
|  99.9%|               3746569.0|
|    max|            9.7884905E12|
+-------+------------------------+
```

4. **Book titles considerations**: looking at some samples of data you can find that there are multiple records for the same book, and it seems it's because of multiple editions (e.g. different countries). However, the rating and the number of ratings is always pretty similar. For this reason I will take just one record for each book based on the title, taking the maximum rating and rating count, for the sake of simplicity.

```
casted.select("book_title", "book_edition", "book_rating_casted", "book_rating_count_casted").filter(lower(col("book_title")).contains("harry potter and the deathly hallows")).orderBy("book_title").show(30, 40)
+------------------------------------+-----------------------------+------------------+------------------------+
|                          book_title|                 book_edition|book_rating_casted|book_rating_count_casted|
+------------------------------------+-----------------------------+------------------+------------------------+
|Harry Potter and the Deathly Hallows|       First American Edition|              4.62|               2068269.0|
|Harry Potter and the Deathly Hallows|         1st Canadian Edition|              4.62|               2068935.0|
|Harry Potter and the Deathly Hallows|           First Edition (UK)|              4.62|               2069089.0|
|Harry Potter and the Deathly Hallows|15th Anniversary Edition (US)|              4.62|               2069138.0|
|Harry Potter and the Deathly Hallows|           First Edition (UK)|              4.62|               2069222.0|
|Harry Potter and the Deathly Hallows|                         null|              4.62|               2069240.0|
|Harry Potter and the Deathly Hallows|                         null|              4.62|               2069370.0|
|Harry Potter and the Deathly Hallows|              Braille Edition|              4.62|               2069424.0|
|Harry Potter and the Deathly Hallows|                   US edition|              4.62|               2069829.0|
|Harry Potter and the Deathly Hallows|               Deluxe Edition|              4.62|               2070021.0|
+------------------------------------+-----------------------------+------------------+------------------------+

casted.select("book_title", "book_edition", "book_rating_casted", "book_rating_count_casted").filter(lower(col("book_title")).contains("little prince")).orderBy("book_title").show(30, 40)
+----------------------------------------+----------------------------------+------------------+------------------------+
|                              book_title|                      book_edition|book_rating_casted|book_rating_count_casted|
+----------------------------------------+----------------------------------+------------------+------------------------+
|                       A Little Princess|                       Illustrated|              4.19|                245795.0|
|                       A Little Princess|                              null|              4.19|                245783.0|
|                       A Little Princess|                  Penguin Classics|              4.19|                245610.0|
|                       A Little Princess|                              null|              4.15|                  3738.0|
|                       A Little Princess|                              null|              4.19|                245721.0|
|Little Princes: One Man's Promise to ...|                              null|              4.27|                 17775.0|
|                       The Little Prince|                              null|              4.29|               1029190.0|
|                       The Little Prince|                              null|              4.29|               1028777.0|
|                       The Little Prince|                              null|              4.29|               1022243.0|
|                       The Little Prince|                              null|              4.29|               1028787.0|
| The Little Prince & Letter to a Hostage|Penguin Twentieth Century Classics|              4.29|               1028667.0|
+----------------------------------------+----------------------------------+------------------+------------------------+

casted.select("book_title", "book_edition", "book_rating_casted", "book_rating_count_casted").filter(lower(col("book_title")).contains("hobbit")).orderBy("book_title").show(30, 40)
+----------------------------------------+-------------------------------------+------------------+------------------------+
|                              book_title|                         book_edition|book_rating_casted|book_rating_count_casted|
+----------------------------------------+-------------------------------------+------------------+------------------------+
|Adventures with Jedi, Geeks, and Hobb...|                                 null|              4.46|                    85.0|
|                               El Hobbit|                                 null|              4.26|               2424366.0|
|                               El hobbit|                                 null|              4.26|               2423913.0|
|                               El hobbit|                          1st edition|              4.26|               2423873.0|
|                                  Hobbit|                                 null|              4.26|               2424470.0|
|J.R.R. Tolkien 4-Book Boxed Set: The ...|        Hobbit Movie Tie-in Boxed set|              4.59|                 99793.0|
|                                O Hobbit|                                 null|              4.26|               2423913.0|
|The Children of Hurin/The Silmarillio...|                               deluxe|              4.71|                   458.0|
|                              The Hobbit|                Deluxe Pocket Edition|              4.26|               2424107.0|
|                              The Hobbit|                                 null|              4.26|               2424429.0|
|                              The Hobbit|                 Movie Tie-In Edition|              4.26|               2424176.0|
|                              The Hobbit|             75th Anniversary Edition|              4.26|               2424307.0|
|                              The Hobbit|             50th Anniversary Edition|              4.26|               2423949.0|
|                              The Hobbit|             75th Anniversary Edition|              4.26|               2424474.0|
|                              The Hobbit|              Collins Modern Classics|              4.26|               2424468.0|
|                              The Hobbit|                                 null|              4.26|               2423646.0|
|                              The Hobbit|                                 null|              4.26|               2423764.0|
|                              The Hobbit|                                 null|              4.26|               2424366.0|
|                              The Hobbit|          Special Collector’s edition|              4.26|               2423922.0|
|                              The Hobbit|                                 null|              4.26|               2422848.0|
|                              The Hobbit|Green Leatherette Collectors Edition |              4.26|               2423960.0|
|      The Hobbit or There and Back Again|                                 null|              4.26|               2424448.0|
|                    The Hobbit, Part One|                                 null|              4.25|                 97664.0|
|               The Hobbit: Graphic Novel|                        Graphic Novel|              4.48|                159099.0|
+----------------------------------------+-------------------------------------+------------------+------------------------+
```

5. **Fields size for DB table**: a separate analysis was needed in order to find the length of fields for the output table on the DB. From the analysis the following lenghts were identified:
 - book_title: 1024
 - book_desc: 32000. This is over the maximum length of VARCHAR. TEXT will be used instead
 - book_authors: 1024
 - genres: 512
 - image_url: 2048

```
scala> val lengthFields = booksInputData
  .withColumn("book_title_length", length(col("book_title")))
  .withColumn("book_desc_length", length(col("book_desc")))
  .withColumn("book_authors_length", length(col("book_authors")))
  .withColumn("genres_length", length(col("genres")))
  .withColumn("image_url_length", length(col("image_url")))
lengthFields: org.apache.spark.sql.DataFrame = [book_authors: string, book_desc: string ... 15 more fields]

scala> lengthFields
  .agg(max("book_title_length"), max("book_desc_length"), max("book_authors_length"), max("genres_length"), max("image_url_length"))
  .show
+----------------------+---------------------+------------------------+------------------+---------------------+
|max(book_title_length)|max(book_desc_length)|max(book_authors_length)|max(genres_length)|max(image_url_length)|
+----------------------+---------------------+------------------------+------------------+---------------------+
|                   615|                24731|                     830|               472|                 1143|
+----------------------+---------------------+------------------------+------------------+---------------------+
```

6. **Same books from different languages**: this issue is not resolved yet with current code. Multiple versions of the same book may be counted as multiple books just because the title has a different language. This may be solved by using another data source that would help to aggregate titles of the same book translated in different languages.

7. **Genres field**: In the following example we took the data written to the DB and see that each book has more than one genre, and that sometimes the same genre is duplicated for the same book (e.g. genre "Science Fiction", second element of the array in the example). For this reason the logic that must be implemented to find the best top 10 genres should take into account duplicates removal.
```
scala> jdbcDF.select("genres").take(20)
res3: Array[org.apache.spark.sql.Row] = Array([Fantasy|Young Adult|Fiction], [Young Adult|Fiction|Science Fiction|Dystopia|Fantasy|Science Fiction], [Young Adult|Fiction|Science Fiction|Dystopia|Fantasy|Science Fiction], [Young Adult|Fiction|Science Fiction|Dystopia|Fantasy|Science Fiction], [Young Adult|Fiction|Science Fiction|Dystopia|Fantasy|Science Fiction|Romance|Adventure|Young Adult|Teen|Apocalyptic|Post Apocalyptic|Action], [Young Adult|Fiction|Science Fiction|Dystopia|Fantasy|Science Fiction], [Young Adult|Fantasy|Romance|Paranormal|Vampires|Fiction|Fantasy|Paranormal], [Classics|Fiction|Historical|Historical Fiction|Academic|School|Literature|Young Adult|Academic|Read For School|Historical|Novels|Young Adult|High School], [Classics|Fiction|Historical|
```

## Computation

### Load job

#### Computing book weight rating
Because of the data consideration (nr. 4) on book titles, the used formula will be ***book_rating * book_rating_count***. Using the "max" function is a simplification, because it could unfortunately lead to the creation of a mix of different records with respect to the input data. For example given a book with title A with two record, one with ***image_url*** B and ***book_desc*** E and the other with ***image_url*** D and ***book_desc*** C, we will have at the end a record with ***image_url*** D and ***book_desc*** E (a new record, not seen in the input data).

#### Ranking computation
For this field I used the ***row_number*** function of Spark, on a window based on the descending order over the weight rating. No partitioning has been used.

#### Write first 1000 books on DB
The DataFrame *limit* function has been used on top of the resulting dataframe from the previous point.

### Retrieve job

#### Write the query to compute the first 10 genres
I assumed to compute the top 10 list based on the 1000 records that were written in the DB. Each book as more than one genre (see *"Data considerations", nr. 7*) so the approach must take into account a deduplication phase. In addition, I leveraged the *book_weight_rating* to score the genres. Thus, in order to compute the top list, the following steps have been implemented:
- Define the table of each distinct genre of each book, followed by the *book_weight_rating* of the book
- Sum the *book_weight_rating* grouping by genre, to find the *genre_weight_rating*
- Order the table by descending order of *genre_weight_rating* and take the first 10 elements

## Testing

Scala Test library has been used to test the majority of the logics included. The build as described above automatically runs these tests. However if you want to run them explicitly you can run the following Maven command
```bash
mvn clean test
```
In order to test the jobs' logic, the code of each job has been splitted in the *run* method and a *computeLogic* method. The latter is calle from the *run* method and contains the job's logic.

## Deployment

In order to deploy the application you must take into account:
 - **Source file**: it must be on the local file system
 - **DB**: it must be reachable on the network and configured with the user of the Application
 - **Runtime**: Java needed, JDBC driver reachable on local file system

In order to make the process easily reproducible and automated, the following choices have been made:
- Limit the interfaces of the system: one command with parameters and a configuration (see *"Environment Setup" nr. 11*)
- Use exit statuses to signal the "caller" about the motivation of errors:
  1. in case of wrong number of parameters in input
  2. in case of errors while reading job configuration
  3. in case of job errors
- Make it easy to change DB: with JDBC interface you can switch DB in two steps.
  1. Add a new configuration
  2. Change the driver in the run commands
- Make a "Fat Jar": the *pom.xml* makes a jar with all the dependencies required to run the application. You can distribute it and it should be easy to run, as far as you also have Java, the source data file, a DB to use and its JDBC Driver.

### What about the cloud

By making some relatively small code changes to the code you could also deploy in the cloud and read data from, for example, **Amazon S3**. Changes include dependencies in the pom.xml file, some lines of code for configuration and of course the new path.

Considering the DB, MariaDB is "cloud ready" (https://mariadb.com/kb/en/mariadb-on-amazon-rds/), and this is another reason I chose this DB. Also, using JDBC, another DB can be used with not a lot of effort (the assumption is still that we are moving small datasets).

You may also want to change the *master* property of Spark in order to run the job, for example, on top of a YARN cluster or other Resource Schedulers.

Since the manipulated data is already public we could choose to deploy the application in a public cloud, valuing more the deployment and management simplicity, rather than privacy concerns.

As deploy tool, there are a lot of tools available, e.g. AWS CodeDeploy, or Jenkins.

Last but not least, proper logging management is needed in order to keep the job monitorable in the cloud.

### Security
Security in Spark is OFF by default. This could mean you are vulnerable to attack by default. Before running in production, all security concerns need to be addressed (https://spark.apache.org/docs/latest/security.html).

For the purpose of this example, no security has been configured on the DB side except the use of a username with password. Remember to configure MariaDB security (or the chosen DB).

## What to do next

There are always things to do and to improve. Here is a list of some things I could do, but I didn't:
1. Proper logging management: in the code I always write messages to stdout. You should avoid this and use a logging library.
2. Parsing arguments: there are a lot of libraries that allow elegant parsing of input arguments and prompting. However I did not have time to use any of it.
3. Improve parsing data. As mentioned in the "Data considerations", there are still some rows that are unusable and must be filtered because of null values on casted values. This could be improved.
4. More granularity for exit codes, to detect failures in DB connections, for example.

If you were surfing the web and you stumbled upon this repo, or if you were searching the Internet to find an example about Spark, I hope you enjoyed the read, or that, at least, you found in this repo some food for thought. :)
