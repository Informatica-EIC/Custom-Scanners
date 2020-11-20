
rem - this script assumes that JAVA_HOME is set, and java is in the system path "java -version"  command should return 1.8 or newer

echo starting...  java -cp "denodoCustomScanner.jar;lib/*" com.infa.edc.scanner.jdbc.DenodoScanner %1 %2
call java -cp "denodoCustomScanner.jar;lib/*" com.infa.edc.scanner.jdbc.DenodoScanner %1 %2

echo 'Finished'
