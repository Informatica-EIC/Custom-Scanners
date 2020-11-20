#!/bin/bash

if [ $# -eq 0 ]
then
   echo 
   echo "denodoScanner.sh [propertyFile] [agreeToDisclaimer]"
   exit 0
fi

propfile=$1
# call the model linker 

java -cp "denodoCustomScanner.jar:lib/*" com.infa.edc.scanner.denodo.DenodoScanner ${propfile} $2

#complete
echo 'Finished'
