# MockData

A tool to generate data.

## Usage

```
mvn package -DskipTests=true
java -cp target/MockData-1.0-SNAPSHOT.jar org.hw.CSVGen -h
```

Default log level is warn, if you want, change it in `simplelogger.properties`.

## Supported
1. CSV
* input: create tablet sql(mysql, doris), lineCount
* output: the 'lineCount' lines csv file

** replaceCols: the columns map which you can return a random value from a pre-existing List<T>, T[] or Class<T extends Enum<?>>.

[ ] support more col type, and improve compatibility for Doris

no header in output csv file
