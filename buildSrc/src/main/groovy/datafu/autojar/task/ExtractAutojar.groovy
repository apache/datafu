/*
 * Copyright 2013 Ray Holder
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package datafu.autojar.task

import org.gradle.api.GradleException

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction

/**
 * Extract the bundled binary autojar package and prepare it to be forked and
 * executed separately.
 */
class ExtractAutojar extends DefaultTask {

    File extractedFile

    ExtractAutojar() {
        group = "Autojar"
        description = "Extract the runnable Autojar archive to the build directory."

        extractedFile = new File(project.ext.autojarBuildDir, 'autojar-2.1.jar')

        dependsOn = [project.tasks.jar]
        outputs.file(extractedFile)
    }

    @TaskAction
    def extract() {
        logger.info('Extracting Autojar archive to: {}', extractedFile)
        // pull resource out of the classpath and write a copy of it
        extractedFile.parentFile.mkdirs()
        def jarResource = "/autojar-2.1.jar"
        def instance = this.getClass().getClassLoader().loadClass('com.github.rholder.gradle.autojar.task.ExtractAutojar')
        def resourceStream = instance.getResourceAsStream(jarResource)
        if (resourceStream == null) {
            throw new GradleException("Failed to load resouce " + jarResource);
        }
        extractedFile.withOutputStream { os ->
            os << resourceStream
        }
        logger.info('Extracted Autojar archive to: {}, {} bytes', extractedFile, extractedFile.length())
    }
}