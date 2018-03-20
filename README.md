# outis
Anonymise your data stored in your Hadoop cluster (Hive, Hdfs...) 


### Build project
In order to build project, you need gradle 3+.

The project uses the shadow jar plugin to create artifact so in order to get an artifact you'll have to 
launch `gradle shadowJar`.

### Project structure
Project is split into two parts: outis-core and outis-link

* #### Outis Core
Outis core is where the anonymization logic is built.

##### Supported formats
For now, only the following formats are supported:
* Hive
    * Parquet
    * TextFile

##### Anonymization methods
Outis core provides multiple anonymization methods. For each type, you can select the method to apply.
* String:
    * suppression: set the column to None
    * setTo: Relpace each characters of the string by another one.
    * setToBlank: Replace all characters to space.
    * setToX: Replace all characters to 'X'.
    * truncate: Truncate the string to n-characters.
    * substitute: Remplace digits with a random digit and letters with a random letter.

* Numeric:
    * substituteValue: Replace value with another random one. The new value will respect numeric range.

* AnonymizeDate:
Only three date types are supported:
    * Date: will replace with a new date.
    * Timestamp: will be replaced with a new timestamp
    * String: A random date with the same format is generated.

All generated dates are in between 01/01/1920 00:00:00 and the current date of execution.

* #### Outis Link
Outis link will be entry point of the anonymization job.
You can configure the client from this application.
Two links examples are provided: 
>* DatagovLink
>
> A connector with Saagie's Datagovernance application. Then you'll have to launch a spark job.
>
> Launch example:
> ```
> spark-submit \
> --conf "spark.executor.extraJavaOptions='-Dlog4j.configuration=log4j.xml'" \
> --conf spark.ui.showConsoleProgress=false \
> --driver-java-options "-Dlog4j.configuration=log4j.xml" \
> {file} -u <hdfs_user> -t <hive_thrift_server> <datagov_api_datasets_url> <datagov_api_notification_url>
> ```
>* ManualLink
>
> A link to manually provide tables to anonymize.
>
> Launch example:
> ```
> spark-submit \
> --conf "spark.executor.extraJavaOptions='-Dlog4j.configuration=log4j.xml'" \
> --conf spark.ui.showConsoleProgress=false \
> --driver-java-options "-Dlog4j.configuration=log4j.xml" \
> --class=io.saagie.outis.link.ManualClient {file}
> ``` 


