./gradlew jar
hadoop fs -rm -f -r /user/g.kasparyants/results
rm -rf results
hadoop jar build/libs/hw1.jar hw1.DocumentCount /data/hw1/hw1 /user/g.kasparyants/results
hadoop fs -get /user/g.kasparyants/results
