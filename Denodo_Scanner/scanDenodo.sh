#!/bin/bash

if [ $# -eq 0 ]
then
   echo
   echo "denodoScanner.sh [propertyFile] [agreeToDisclaimer]"
   exit 0
fi

propfile=$1

# set ssl jvm options (if needed)
# assumption is $INFA_HOME is set - change settings to suit your environment
# add a truststore password for the truststore you are using
# you can use any truststore - including cacerts, after importing the denodo certificate
export SCANNER_TRUSTSTORE=$INFA_HOME/services/shared/security/infa_truststore.jks
export SCANNER_TRUSTSTORE_PWD=
export JAVA_OPTS="-Djavax.net.ssl.trustStore=$SCANNER_TRUSTSTORE -Djavax.net.ssl.trustStorePassword=$SCANNER_TRUSTSTORE_PWD -Djavax.net.ssl.trustStoreType=JKS"

java ${JAVA_OPTS} -cp "denodoCustomScanner.jar:lib/*" com.infa.edc.scanner.denodo.DenodoScanner ${propfile} $2

#complete
echo Finished
