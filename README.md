#MapReduce task


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
