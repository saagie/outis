# My name is nobody
Working with a datalake implies dealing with a lot of data of any type. The one we will focus on will be personal data. GDPR is coming in application soon, everyone must be wary about how are stored personal data. 
Personal data will now only be kept if the following two conditions are met:
* User has consented for his data to be used
* Data usage is allowed for a certain duration

As a datalake is essentially shared by multiples services, data can easily be shared between multiples services which may not be allowed to use or see some information.

Saagie built solutions to make data management as smooth as possible.

First of all, Datagovernance, as its name implies it is a tool for data governance (duh).
Among its functionalities it is possible to qualify datasets and organize them by domains. But what will really interest us is the possibility to declare fields within these datasets as personal data. 

For each dataset you want to anonymize, you’ll provide the user consent’s field, the entry date field (when the row has been inserted or updated) and the delay before anonymization kicks in.
![](datagov-ano.png)

# OUTIS
All is set, but now what?

Well now everything is ready for a second tool to enter in action: [outis](https://github.com/saagie/outis).
![](https://media.giphy.com/media/xT39D7GQo1m3LatZyU/giphy.gif)  
(Caution: wet paint)

The tool is structured in two parts:
* Core where all the anonymization logic is managed
* Link which will be the entry point

This tool relies on Spark so you'll need a Spark cluster in order to run it :).

Good news everyone: it's customizable! You can make your own link and some more features.

## OUTIS Core
The application automatically detects the type of the column to be processed except for date in String then the date format and have to be provided.

**Supported formats**  
For now, only the following formats are supported:
* Hive
    * Parquet
    * TextFile

**Anonymization methods**  
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
    * No mas.
    
* Date:
Only three date types are supported:
    * Date: will replace with a new date.
    * Timestamp: will be replaced with a new timestamp
    * String: A random date with the same format is generated.

All generated dates are in between 01/01/1920 00:00:00 and the current date of execution.

## Outis Link
Outis link is the entry point of the tool. Outis can be plugged to anything but at this time, only two links are provided:

* Datagov Link

This Link is designed to work with Saagie Datagovernance (no way!). All datasets that are flagged as to be anonymized will be processed.
Anonymization's process result (like number of rows that were anonymized and other useful stuff) is sent back to Datagovernance.
Easy peasy.

* Manual Link

You don't want to or can't use Datagovernance (that makes me sad either ways you know)? Well we've also provided an example of manual link implementation.

# Questions
* How I write my own link?

Implement the OutisLink trait and you're good to go!  
Here's the example of the Manual Link :
```
case class ManualLink() extends OutisLink {
  /**
    * This method returns the list of datasets to anonymize.
    *
    * @return
    */
  override def datasetsToAnonimyze(): Either[OutisLinkException, List[DataSet]] = {
    Right(List(ParquetHiveDataset(
      "0",
      "anonymization.table",
      List(
        Column("name", "string"),
        Column("byte_value", "byte"),
        Column("short_value", "short"),
        Column("int_value", "int"),
        Column("long_value", "long"),
        Column("float_value", "float"),
        Column("double_value", "double"),
        Column("decimal_value", "decimal"),
        Column("timestamp_value", "timestamp"),
        Column("date_value", "date"),
        Column("date_string_value", "date", Some("yyyy-MM-dd"))),
      FormatType.PARQUET,
      "anonymization.test_table",
      Column("creation_date", "date", None, Some(-1))
    ),
      TextFileHiveDataset(
        "1",
        "anonymization.test_time",
        List(
          Column("time", "double"),
          Column("comment", "string"),
          Column("user_id", "bigint")
        ),
        FormatType.TEXTFILE,
        "anonymization.test_time",
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        Column("date", "date", Some("yyyy-MM-dd HH:mm:ss.SSS"), Some(365)),
        "\u0001",
        "\u0001"
      )))
  }

  /**
    * The method which will be called when a dataset has been processed.
    *
    * @param anonymizationResult The processed result.
    * @return
    */
  override def notifyDatasetProcessed(anonymizationResult: AnonymizationResult): Either[OutisLinkException, String] = {
    implicit val formats = Serialization.formats(NoTypeHints)
    Right(write(anonymizationResult))
  }
}
```

* Cool, but I want to use my own anonymization methods (and do not want to ask a question)

You can add your own methods. Just provide them within any Link classpath and declare the method you want to use in the OutisConf with it's parameter's list (which should at least conntain the column value).

`OutisProperty[String]("io.saagie.outis.core.anonymize.AnonymizeString", "substitute", List(ColumnValue))`
