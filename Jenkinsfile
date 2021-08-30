pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                echo 'Building..'
                sh "docker build -t spark/batching:${env.BUILD_ID} ."
            }
        }
        stage('Test') {
            when {
              expression {
                currentBuild.result == null || currentBuild.result == 'SUCCESS'
              }
            }
            steps {
                echo 'Testing..'
                sh 'python3 -m venv venv && source venv/bin/activate'
                sh 'pip3 install -U pip && pip3 install -r requirements.txt'
                sh 'pytest tests --junitxml=test-results/results.xml'
                junit 'test-results/results.xml'
            }
        }
    }
}