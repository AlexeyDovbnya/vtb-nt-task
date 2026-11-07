@echo off
cd /d %~dp0..
if not exist dumps mkdir dumps
java -Xms256m -Xmx512m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%cd%\dumps -XX:+ExitOnOutOfMemoryError -jar stub\kafka-stub\target\kafka-stub-0.0.1-SNAPSHOT.jar