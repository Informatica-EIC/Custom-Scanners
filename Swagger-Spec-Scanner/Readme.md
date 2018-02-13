# Swagger API Scanner Example

Create a new model
---------------

* In the Catalog Admin UI, go to Manage > Models
* In the left panel, click on the arrow pointing down and “New Custom Model”
* Browse to the standard API xml file and click OK
* Review the newly created model “Swagger API V2”

Create a custom resource type
-----------------------------

* In the Catalog Admin UI, go to Manage > Custom Resource Types
* On the left panel, click on the “+” sign
* Enter "Swagger API" as Name, select the "Swagger API V2" Model and select the "Endpoint" as connection types

Generate CSV files from Swagger JSON
------------------------------------

execute the following command: 

java -jar <PATH_TO_JAR>\swaggertoeic.jar -swagger <PATH_TO_JSON>\swagger.json

this will generate 6 files:
* Endpoint.csv
* links.csv
* Parameters.csv
* Responses.csv
* swagger.csv
* tags.csv

create a zip file containing all the csv files

Create a resource to load the metadata
--------------------------------------

* In the Catalog Admin UI, go to New > Resource
* Enter name and select resource Type as "Swagger API"
* Upload the zip file where prompted
* Click on next, then Save and Run.




