#!/bin/bash

#sudo apt-get -y install gradle
git clone https://github.com/GoogleCloudPlatform/pontem.git
cd pontem/BigQueryWorkloadTester
gradle clean :BigQueryWorkloadTester:build

