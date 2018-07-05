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

package datafu.autojar

import datafu.autojar.task.Autojar
import datafu.autojar.task.ExtractAutojar
import org.gradle.api.Plugin
import org.gradle.api.Project

/**
 * This plugin rolls up your current project's classes and all of its
 * dependencies into a single flattened jar archive walking each dependency and
 * including only those which are used.
 *
 * At a minimum, the configuration expects to find a custom 'mainClass' when
 * adding the plugin to your own builds, as in:
 *
 * <pre>
 * apply plugin: 'gradle-autojar'
 *
 * task awesomeFunJar(type: Autojar) {
 *     mainClass = 'com.github.rholder.awesome.MyAwesomeMain'
 * }
 * </pre>
 *
 * You can also manually specify additional classes to walk that were otherwise
 * not correctly detected by the plugin's dependency analysis because of
 * reflection use, etc. as well as non-class resources, as in:
 *
 * <pre>
 * apply plugin: 'gradle-autojar'
 *
 * task awesomeFunJar(type: Autojar) {
 *     mainClass = 'com.github.rholder.awesome.MyAwesomeMain'
 *     autojarFiles = ['foo.txt', 'dir/foo2.txt']
 *     autojarClasses = ['org.apache.commons.lang.time.DateUtils']
 * }
 * </pre>
 */
class GradleAutojarPlugin implements Plugin<Project> {

    void apply(Project project) {
        project.apply(plugin:'java')

        project.ext.autojarBuildDir = new File(project.buildDir, "autojar-build")
        project.ext.autojarBuildDir.mkdirs()
        project.ext.Autojar = Autojar.class

        project.tasks.create('extractAutojar', ExtractAutojar.class)
    }
}