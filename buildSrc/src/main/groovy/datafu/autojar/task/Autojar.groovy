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

import org.gradle.api.InvalidUserDataException
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.PublishArtifact
import org.gradle.api.java.archives.Manifest
import org.gradle.api.java.archives.internal.DefaultManifest
import org.gradle.api.logging.Logger
import org.gradle.api.provider.Property
import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.TaskDependency
import org.gradle.api.tasks.bundling.Jar
import org.gradle.util.ConfigureUtil

/**
 * This is the primary Task type used to create Autojar archives.
 */
class Autojar extends JavaExec implements PublishArtifact {

    Logger logger
    Jar baseJar
    Manifest manifest
    ExtractAutojar extractAutojar

    Configuration targetConfiguration

    Property<String> mainClass
    List<String> autojarClasses    // convert these to raw files
    List<String> autojarFiles      // all the class files, etc.

    File autojarBuildDir
    String autojarExtra     // default to -bav
    String autojarClasspath // -c all dependencies, including base jar
    String autojarManifest  // path to final manifest file -m
    File autojarOutput      // -o path to output file

    // publish artifact overrides
    Date publishDate
    String publishClassifier
    String publishType
    String publishExtension

    Autojar() {
        group = "Autojar"
        description = "Create an Autojar runnable archive from the current project using a given main class."

        logger = project.logger

        autojarBuildDir = project.ext.autojarBuildDir
        extractAutojar = project.tasks.extractAutojar

        // default to main project jar
        baseJar = project.tasks.jar

        autojarOutput = new File(autojarBuildDir, generateFilename(baseJar, "autojar"))

        dependsOn = [baseJar, extractAutojar]
        inputs.files([baseJar.getArchivePath().absoluteFile, extractAutojar.extractedFile])
        outputs.file(autojarOutput)
    }

    @TaskAction
    @Override
    public void exec() {
        setMain('org.sourceforge.autojar.Autojar')
        classpath(extractAutojar.extractedFile.absolutePath)

        // munge the autojarClasspath
        autojarClasspath = baseJar.getArchivePath().absolutePath

        // default to runtime configuration if none is specified
        targetConfiguration = targetConfiguration ?: project.configurations.runtime
        def libs = targetConfiguration.resolve()
        libs.each {
            logger.debug("Including dependency: " + it.absolutePath)
            autojarClasspath += ":" + it.absolutePath
        }

        // default to -ba if none is specified
        autojarExtra = autojarExtra ?: "-ba"

        autojarFiles = autojarFiles ?: []

        // convert class notation to raw files for Autojar
        autojarClasses = autojarClasses ?: []
        autojarClasses.each {
            autojarFiles.add(it.replaceAll("\\.", "/") + ".class")
        }

        // default to whatever was in the manifest closure
        manifest = manifest ?: new DefaultManifest(project.fileResolver)

        // infer main class starting point from manifest if it exists
        if(mainClass) {
            autojarFiles.add(mainClass.replaceAll("\\.", "/") + ".class")
            manifest.attributes.put('Main-Class', mainClass)
        }

        // by now we should have at least one file
        if(!autojarFiles) {
            throw new InvalidUserDataException("No files set in autojarFiles and no main class to infer.")
        }

        autojarManifest = writeJarManifestFile(manifest).absolutePath

        def autojarArgs = [autojarExtra, "-c", autojarClasspath, "-m", autojarManifest,"-o", autojarOutput]
        autojarArgs.addAll(autojarFiles)
        logger.info('{}', autojarArgs)

        args(autojarArgs)
        super.exec()
    }

    @Override
    String getExtension() {
        return publishExtension ?: Jar.DEFAULT_EXTENSION
    }

    @Override
    String getType() {
        return publishType ?: Jar.DEFAULT_EXTENSION
    }

    @Override
    String getClassifier() {
        return publishClassifier ?: 'autojar'
    }

    @Override
    File getFile() {
        return autojarOutput
    }

    @Override
    Date getDate() {
        return publishDate ?: new Date(autojarOutput.lastModified())
    }

    @Override
    TaskDependency getBuildDependencies() {
        // we have to build ourself, since we're a Task
        Task thisTask = this
        return new TaskDependency() {
            @Override
            Set<? extends Task> getDependencies(Task task) {
                return [thisTask] as Set
            }
        }
    }

    /**
     * Allow configuration view of a 'manifest' closure similar to the Jar task.
     *
     * @param configureClosure target closure
     */
    Autojar manifest(Closure configureClosure) {
        manifest = manifest ?: new DefaultManifest(project.fileResolver)
        ConfigureUtil.configure(configureClosure, manifest);
        return this;
    }

    /**
     * Return a manifest configured to boot the jar using One-JAR and then
     * passing over control to the configured main class.
     */
    static File writeJarManifestFile(Manifest manifest) {
        File manifestFile = File.createTempFile("manifest", ".mf")
        manifestFile.deleteOnExit()

        manifest.writeTo(manifestFile.path)

        // hack to clip off the first line, Manifest-Version, in the file because Autojar is weird
        def lines = manifestFile.readLines()
        lines.remove(0)
        manifestFile.setText(lines.join("\n"))

        return manifestFile
    }

    /**
     * This is kind of a hack to ensure we get "-classifier.jar" tacked on to
     * archiveName + a valid version.
     */
    static String generateFilename(Jar jar, String classifier) {
        return jar.archiveName - ("." + jar.extension) + "-" + classifier + "." + jar.extension
    }
}