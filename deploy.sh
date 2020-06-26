#!/bin/bash
cd wheel
mvn clean deploy
cd ../spark-sdq
mvn clean deploy
cd ..
