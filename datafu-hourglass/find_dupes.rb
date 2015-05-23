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
