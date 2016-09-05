#MapReduce task

####subtask 1:
- MapReduce jobs to count amount of all tags in the dataset (with combiner & distributed cache). 
- Output is saved in the text files on HDFS with rows as follow: TAG, COUNT;
- MRUnit tests.

####subtask 2:
- MapReduce jobs to count amount of visits (count(*)) by IP and spends (sum(Binding price)) by IP (combiner & custom writable data type);
- Output is saved as Sequence file compressed with Snappy (key is IP, and value is custom object for visits and spends);
- Counters for getting how many records of browsers were detected is used;
- for parsing User Agent is used [this library](https://github.com/HaraldWalker/user-agent-utils)
- MRUnit tests.

##Build
```
mvn clean install
```

##How to run the first subtask
```
yarn jar ~/mapReduce/target/mapReduce-0.0.1-SNAPSHOT.jar  com.epam.bigdata.q3.task3.mapReduce/TagCount <path_to_input_file> <path_to_output_file> <path_to_file_with_tags_for_excluding>
```

###Example
```
yarn jar /root/marina/BigData_tasks/task_2/mapReduce/target/mapReduce-0.0.1-SNAPSHOT.jar  com.epam.bigdata.q3.task3.mapReduce/TagCount /tmp/admin/homework3/user.profile.tags.us.tag.txt /tmp/admin/homework3/user.profile.tags.us.tag.out.txt
```

##How to run the second subtask
```
yarn jar ~/mapReduce/target/mapReduce-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.epam.bigdata.q3.task3.mapReduce/CountOfVisits <path_to_input_file> <path_to_output_file>
```

###Example
```
yarn jar /root/marina/BigData_tasks/task_2/mapReduce/target/mapReduce-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.epam.bigdata.q3.task3.mapReduce/CountOfVisits /tmp/admin/homework3/stream.20130606-aa.txt /tmp/admin/homework3/part2.out.txt
```
