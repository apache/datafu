# Quantile Example

This is an example of using the *Quantile* and *StreamingQuantile* UDFs to to compute quantiles for some sample temperature data.  It is based on our [blog post](http://datafu.incubator.apache.org/blog/2012/01/10/introducing-datafu.html), which can be read for more details.

Assuming pig 0.12.1 has been downloaded to ~/pig-0.12.1 then the following commands can be used to execute the scripts:

    ruby generate_temperature_data.rb

    ~/pig-0.12.1/bin/pig -x local -f quantile.pig
    ~/pig-0.12.1/bin/pig -x local -f streaming_quantile.pig
