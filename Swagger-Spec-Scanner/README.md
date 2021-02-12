# Swagger API Scanner Example

---------------
## Dislaimer
this custom scanner is an example, that can parse swagger spec files & create metadata to import into EDC

### Important Points:-
* it is not an official product & is not supported by Informatica R&D or GCS (don't create a ticket with GCS)
* to use it - you will need to build from source (see Build from Source section below)
* if you find an problem or want to make a comment, you can create a issue on github
* this scanner is not a 100% coverage of the open-api spec
  * example:  the structure of responses for endpoints is not created/documented.  this could be extended (see next point)
* if you want to extend the model/scanner coverage - please feel free to fork/clone this repository.
  * if you improve the scanner - then submit a pull request to update it here for all to benefit
  * the code could use a re-structure (contributors welcome)
---------------

Create a new model  (one-time)
---------------

* In the Catalog Admin UI, go to Manage > Models
* In the left panel, click on the arrow pointing down and “New Custom Model”
* Browse to the standard API xml file and click OK
* Review the newly created model “Swagger API V2”

Create a custom resource type (one-time)
-----------------------------

* In the Catalog Admin UI, go to Manage > Custom Resource Types
* On the left panel, click on the “+” sign
* Enter "Swagger API" as Name, select the "Swagger API V2" Model and select the "Endpoint" as connection types

Generate CSV files from Swagger JSON
------------------------------------

execute the following command:

```
java -jar <PATH_TO_JAR>\Swagger-Spec-Scanner-Demo-0.1.0-SNAPSHOT.jar -swagger <PATH_TO_JSON>\swagger.json [-out <output folder>]
```

for versions prior to v10.2.2hf1 use the original version - Note this version will not zip any files, or be updated
```
java -jar <PATH_TO_JAR>\swaggertoeic.jar -swagger <PATH_TO_JSON>\swagger.json
```


Note:  yaml files work too
Note:  default value for -out is output - the folder will be created if it does not exist

this will generate 6 files:
* objects-Endpoint.csv
* links.csv
* objects-Parameters.csv
* objects-Responses.csv
* objects-swagger.csv
* objects-tags.csv

a zip file named edc-swagger-scanner-result.zip



Create a resource to load the metadata
--------------------------------------

* In the Catalog Admin UI, go to New > Resource
* Enter name and select resource Type as "Swagger API"
* Upload the zip file where prompted
* Click on next, then Save and Run.
* you could also run this as a pre-execution command



# Build From Source

To create Swagger-Spec-Scanner-Demo-<version>-SNAPSHOT.jar   - version is defined in pom.xml

* pre-requisites:
  * java 1.8
  * maven
* steps
  * fork/clone this repository  (if you are contibuting back, best to fork then clone), then update/pull the lastest version (it may change over time)
  * build using maven (see pom.xml)
    ```
    cd Swagger-Spec-Scanner
    mvn clean package
    ```
    the packaged folder will be created - with Swagger-Spec-Scanner-Demo-0.1.0-SNAPSHOT.jar & the folder lib (contains all dependent .jar files).  the other folders can be ignored

    a Swagger-Spec-Scanner-Demo.zip file is created - containing the packaged .jar files, readme.md and the sample folders
  * compiled project will be written to the `packaged` folder, with a lib sub-folder that contains all dependent jar files (opencsv, swagger parser etc)



# History
---------

- 2021-February
  - updated to new(er) custom scanner framework file names (objects*.csv) - works with EDC 10.2.2hf1+  - tested with EDC 10.4.1.3 & 10.5
  - generates .zip file for import into catalog (manually needed before)
  - -out parameter added, default is output  (folder will be created if not already existing)
  - added maven pom.xml for easier build/package process
  - moved files to work with maven (code moved from generator/Java/src to src/java)
  - updated to more recent/current version of swagger parser (see dependencies in pom.xml)
  - re-scaned the 2 samples & replaced result files
- 2018 - initial version  (works with EDC 10.2.2 and before, will not work with v10.2.2hf1+) - using original file naming standards (links.csv and any other *.csv treated as objects))

