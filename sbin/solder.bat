rem @echo off
setlocal


if "%JAVA_HOME%"=="" set JAVA_HOME=C:\Program Files\Java\zulu21.30.15-ca-jdk21.0.1-win_x64
set SOLDER_LIB=%ENIGMA_INSTALL%\lib

set CLASSPATH=%BASE_CLASSPATH%;%SOLDER_LIB%\*
set MainClass=org.solder.ctest.SolderCLI
set JAVA_OPTS=-Dorg.apache.commons.logging.LogFactory=com.jnk.junit.TestFrameworkLogFactory -Dtalos.logging.level=trace

"%JAVA_HOME%\bin\java.exe" %JAVA_OPTS% -classpath %CLASSPATH% %MainClass%  %*