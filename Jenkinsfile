pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                echo 'Building..'
                sh 'docker build -t spark/batching:${env.BUILD_ID} .'
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
                sh 'pytest tests --junitxml=test-results/results.xml'
                junit 'test-results/results.xml'
            }
        }
    }
}