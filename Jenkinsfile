elifePipeline {
    node('containers-jenkins-plugin') {
        def commit
        def version

        stage 'Checkout', {
            checkout scm
            commit = elifeGitRevision()
            if (env.TAG_NAME) {
                version = env.TAG_NAME - 'v'
            } else {
                version = 'develop'
            }
        }

        stage 'Build and run tests', {
            try {
                sh "make IMAGE_TAG=${commit} ci-build-and-test"
            } finally {
                sh "make ci-clean"
            }
        }

        elifeMainlineOnly {
            stage 'Merge to master', {
                elifeGitMoveToBranch commit, 'master'
            }

            stage 'Push unstable image', {
                def image = DockerImage.elifesciences(this, 'sciencebeam-airflow', commit)
                def unstable_image = image.addSuffixAndTag('_unstable', commit)
                unstable_image.tag('latest').push()
                unstable_image.push()
            }
        }

        elifeTagOnly { repoTag ->
            stage 'Push stable sciencebeam-airflow', {
                def image = DockerImage.elifesciences(this, 'sciencebeam-airflow', commit)
                image.tag('latest').push()
                image.tag(version).push()
            }
        }
    }
}
