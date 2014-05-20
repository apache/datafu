# Sessionize Example

This is an example of sessionizing a clickstream.  See our [blog post](http://datafu.incubator.apache.org/blog/2013/01/24/datafu-the-wd-40-of-big-data.html) for more details.

Assuming pig 0.12.1 has been downloaded to ~/pig-0.12.1 then the following commands can be used to execute the scripts:

    ~/pig-0.12.1/bin/pig -x local -f sessionize.pig
