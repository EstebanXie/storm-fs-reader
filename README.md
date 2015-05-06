# storm-fs-reader
Simple storm project that provides a spout and helper bolts to transparently read from either local or HDFS file systems. The program will handle directories or individual files.

# Usage
To run, first build with:
```
mvn clean package
```

Next, upload to your storm cluster and submit the job as 
```
storm jar storm-fs-reader-1.0.jar <FILE_PATH> <FILE_SCHEMA>
```

To run locally, change ```parser.run()``` in ```main()``` to ```parser.test()```.
 
