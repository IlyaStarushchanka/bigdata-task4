# bigdata-task4 "MapReduce Sort"

###Build project 
```
mvn clean install
```
###Run

```
yarn jar ${PathToProject}/bigdata-task4/target/bigdata-task4-1.0-SNAPSHOT-jar-with-dependencies.jar com.epam.bigdata.mapreduce.SecondarySortJob <in> <out>
```

Where

`in` - path to input file,

`out` - path for output,

For example:

```
yarn jar /root/IdeaProjects/bigdata-task4/target/bigdata-task4-1.0-SNAPSHOT-jar-with-dependencies.jar com.epam.bigdata.mapreduce.SecondarySortJob /tmp/admin/dataset /tmp/admin/tagscountout
```
