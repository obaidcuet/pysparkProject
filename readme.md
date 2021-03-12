### Intension
- Local Testing
- Dependency management
- Code build
- Modularise pyspark

### local setup consideration
- use pipenv in pycharm
- set HADOOP_HOME to point downloaded location of winutils(for windows)
- project directory structure

### pre-requisites
- setup pycharm
- setup python project with pipenv (install python pyspark inside that)
- setup pytest as testing tools in project
- set HADOOP_HOME environment variable to downloaded location for hadoop winutils 

### Next steps: 
1. isolate business logic in domain folder (done)
2. create another pipeline without sql  (done)
3. prepare test cases based onn columns logic and total output (done)
4. try package and spark-submit to EMR (next)   
5. docker env for dev/test

### References:
- https://github.com/soyelherein/pyspark-cicd-template
- https://github.com/AlexIoannides/pyspark-example-project
- https://github.com/pchrabka/PySpark-PyData
- https://youtu.be/7qMhuVGqGY4
- https://youtu.be/JJmTO95AoqE
