require 'rubystats'

# Generates 10,000 measurements for three imaginary temperature sensors.

sensors = []
sensors << {:id => 1, :mean => 60.0, :stdev => 5.0}
sensors << {:id => 2, :mean => 50.0, :stdev => 10.0}
sensors << {:id => 3, :mean => 40.0, :stdev => 3.0}

File.open('temperature.txt','w') do |file|
  sensors.each do |sensor|
    id = sensor[:id]
    dist = Rubystats::NormalDistribution.new(sensor[:mean],sensor[:stdev])
    dist.rng(10000).each do |value|
      file.write "#{id}\t#{value}\n"
    end
  end
end