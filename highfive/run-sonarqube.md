To update the Sonarqub report:

    mvn clean verify sonar:sonar -Dsonar.projectKey=highfive-1 -Dsonar.projectName='highfive-1' -Dsonar.host.url=http://192.168.56.244:9000 -Dsonar.token=sqp_f3d132a2af2e985af44c0f8028be37229f9e6584

See the report at:

     http://192.168.56.244:9000/dashboard?id=hotrod

(use admin / admin1)
