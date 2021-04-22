@echo off
rem - this script assumes that JAVA_HOME is set, and java is in the system path "java -version"  command should return 1.8 or newer

rem set ssl jvm options (if needed)
rem assumption is $INFA_HOME is set - change settings to suit your environment
rem add a truststore password for the truststore you are using
SET SCANNER_TRUSTSTORE=c:\path\to\truststore\infa_truststore.JKS
SET SCANNER_TRUSTSTORE_PWD=
SET JAVA_OPTS=-Djavax.net.ssl.trustStore=%SCANNER_TRUSTSTORE% -Djavax.net.ssl.trustStorePassword=%SCANNER_TRUSTSTORE_PWD% -Djavax.net.ssl.trustStoreType=JKS

rem echo starting...  java -cp "denodoCustomScanner.jar;lib/*" com.infa.edc.scanner.denodo.DenodoScanner %1 %2
call java -cp "denodoCustomScanner.jar;lib/*" com.infa.edc.scanner.denodo.DenodoScanner %1 %2

echo Finished
