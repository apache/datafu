# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Uses jarfish to find all duplicate classes from jars listed in the .classpath

require 'nokogiri'

doc = Nokogiri::XML(File.open('.classpath'))

# Create a classpath.lst file that contains all jars referenced in .classpath
output = File.open('classpath.lst','w')
doc.xpath("//classpathentry[@kind='lib']").each { |x| output.write x["path"]+"\n" }
output.close

unless File.file?("jarfish-1.0-rc7.jar") then
  `wget https://jarfish.googlecode.com/files/jarfish-1.0-rc7.jar`
end 

exec "java -jar jarfish-1.0-rc7.jar dupes classpath.lst"
