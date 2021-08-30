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
                sh 'python3 -m venv venv && . venv/bin/activate'
                sh 'python --version'
                sh 'pip3 install -U pip && pip3 install -r requirements.txt'
                sh 'python3 -m pytest tests --junitxml=test-results/results.xml'
                junit 'test-results/results.xml'
            }
        }
    }
}