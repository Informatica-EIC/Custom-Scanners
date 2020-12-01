
rem - this script assumes that JAVA_HOME is set, and java is in the system path "java -version"  command should return 1.8 or newer

echo starting...  java -cp "lib/*" com.infa.edc.scanner.jdbc.GenericScanner %1 
call java -cp "lib/*" com.infa.edc.scanner.jdbc.GenericScanner %1 agreeToDisclaimer

echo 'Finished'
