# Stock Portfolio Profits using Monte Carlo simulation in Spark
In this project work I ran the Stock Portfolio Dataset for few stocks on the Apache Spark Framework to extract
to extract profit and losses that occured based on the investment made in them.

### Library Dependencies 

Listed below are different dependencies required for this porjects:

* [Scalatest.FunSuite] - Unit testing framework for the Scala
* [slf4j] - Simple Logging Facade for Java 
* [Typesafe] - to manage configuration files
* [Apache Spark]

All these dependencies are specified in "build.sbt" and will be configured when the project is 
built.  

### Running the Code:

To run the program:
1. Clone the repo on your system
2. Create the fat jar of the the project
```
sbt clean compile assembly
```
3. Now you need to run Hadoop Services:
```
start-all.sh
```
4. Then type the following command to run the spark application using YARN 
```
spark-submit --class MonteCarloSim --master yarn --deploy-mode client jarfile /output
```
### Solution Implemented

Monte Carlo Simulation is a statistical method applied in financial modeling. The simulation relies on the repetition of random samples to achieve numerical results. In this implementation the main ideas is the repeated random sampling of inputs of the random variable and the aggregation of the results. The variable with a probabilistic nature (Change value in the code) is assigned a random value. The model is then calculated based on the random value. The result of the model is recorded, and the process is repeated. The process is repeated hundreds of times. When the simulation is complete, the results can be averaged to determine the estimated value.

The values for the stocks are fetched using the AlphaVantage API, which accepts the Stock symbol. We fetched the data in csv format and created Spark DataFrames for each one of them.
Further manipulations on values were carried out using these Data Frames.

#### Execution Results:
+ DataFrame 1: Once we fetch the data of a stock, dataframe simmilar to below one is created.
```
+-------------------+------+---------+------------------+
|          timestamp| close|LastClose|            Change|
+-------------------+------+---------+------------------+
|2019-07-08 00:00:00|134.84|     null|              null|
|2019-07-09 00:00:00|134.29|   134.84|0.6911056438734419|
|2019-07-10 00:00:00|132.64|   134.29|  0.68698481114936|
|2019-07-11 00:00:00|133.96|   132.64|0.6981107163576235|
|2019-07-12 00:00:00|138.36|   133.96|0.7094365974590054|
|2019-07-15 00:00:00|139.64|   138.36| 0.697762129621669|
|2019-07-16 00:00:00|139.09|   139.64|0.6911758890844956|
|2019-07-17 00:00:00|135.73|   139.09|0.6809951322845895|
|2019-07-18 00:00:00|134.89|   135.73|0.6900480045256244|
|2019-07-19 00:00:00|136.23|   134.89|0.6981018958172076|
|2019-07-22 00:00:00|135.24|   136.23|0.6895070022531822|
|2019-07-23 00:00:00| 138.1|   135.24|0.7036654636969691|
|2019-07-24 00:00:00|131.91|    138.1|0.6704809349343113|
|2019-07-25 00:00:00|134.71|   131.91|0.7037045518247025|
|2019-07-26 00:00:00|132.92|   134.71|0.6864811098833251|
|2019-07-29 00:00:00|134.46|   132.92| 0.698923424068583|
|2019-07-30 00:00:00|132.95|   134.46|0.6875163040855118|
|2019-07-31 00:00:00|131.67|   132.95|0.6883217169252361|
|2019-08-01 00:00:00|126.79|   131.67|0.6744421507287931|
|2019-08-02 00:00:00|124.54|   126.79|0.6842346421125712|
+-------------------+------+---------+------------------+
```
+ The final Profit values evaluated are stored in a Dataframe with Stock name and max Profit that can be made in each.

```
+------+------------------+
|Stocks|            Profit|
+------+------------------+
|   CAT| 99.94521614697946|
|  CSCO|13.540697302137378|
|  EBAY| 7.102708247072532|
|  FAST|6.3617879637359245|
+------+------------------+
```
### Running the Test Cases:

To run the test cases please input the following command on terminal after cloning the repo 
```
sbt clean compile test
```
The test cases runs as follows:
```
[info] MonteCarloTest:
[info] - Able to Load Configuration
[info] - Test for API to fetch Data 
[info] - Profit Calculated correctness
[info] - Each stock has an associated investment
[info] - Correct Stock Symbol is used
[info] Run completed in 10 seconds, 389 milliseconds.
[info] Total number of tests run: 5
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 5, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
[success] Total time: 37 s, completed Nov 25, 2019 11:12:29 PM
```
#### EMR Implementation
Please Refer to the embeded youtube link for details regarding deployment on AWS

[EMR deployment video](https://www.youtube.com/user/kabcdefghijkl/videos?view=0&sort=dd&shelf_id=0 "title")
