#!/bin/bash

if [ $# -eq 0 ]
then
   echo 
   echo "denodoScanner.sh [propertyFile]"
   exit 0
fi

propfile=$1
# call the model linker 

java -cp "denodoCustomScanner.jar:lib/*" com.infa.edc.scanner.jdbc.DenodoScanner ${propfile} $2

#complete
echo 'Finished'
