### Flume launch on Yandex Cloud Data Proc cluster (Hadoop image 2.0) 
Steps to use Flume on 
- Download Flume 1.9.0 and extract
- Put config `flume/flume.conf` in conf dir `/home/ubuntu/flume`
- Run command `. flume/setup_env.sh`
- In file `/home/ubuntu/flume/bin/flume-ng` change string starts
with `JAVA_OPTS="` to `JAVA_OPTS="-Xmx2048m"`