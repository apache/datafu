# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at

#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

require 'rubystats'

# Generates 10,000 measurements for three imaginary temperature sensors.

sensors = []
sensors << {:id => 1, :mean => 60.0, :stdev => 5.0}
sensors << {:id => 2, :mean => 50.0, :stdev => 10.0}
sensors << {:id => 3, :mean => 40.0, :stdev => 3.0}

File.open('temperature.tsv','w') do |file|
  sensors.each do |sensor|
    id = sensor[:id]
    dist = Rubystats::NormalDistribution.new(sensor[:mean],sensor[:stdev])
    dist.rng(10000).each do |value|
      file.write "#{id}\t#{value}\n"
    end
  end
end
