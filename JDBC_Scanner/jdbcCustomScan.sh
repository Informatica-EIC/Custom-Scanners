#!/bin/bash

if [ $# -eq 0 ]
then
   echo 
   echo "jdbcCustomScanner.sh [propertyFile] [agreeToDisclaimer]"
   exit 0
fi

propfile=$1

java -cp "lib/*" com.infa.edc.scanner.jdbc.GenericScanner $1 agreeToDisclaimer

#complete
echo 'Finished'


pause