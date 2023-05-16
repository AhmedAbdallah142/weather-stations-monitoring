@echo off
for /L %%i in (1,1,10) do (
    start cmd /k java -jar target/data-acquisition-jar-with-dependencies.jar %%i
)
