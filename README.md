mandoline
=============

Mandoline is a java server that provides a simple `JSON` API to GeoMondrian. To install it (ubuntu 14.04):

    # Install build tools
    sudo apt-get install ant
    sudo apt-get install openjdk-7-jdk

    # Retrieve the source
    git clone https://github.com/loganalysis/mandoline.git

Now, you should edit the config.dist file to add your GeoMondrian's connection information to have something like this:

    #Configuration of the database
    dbhost=localhost
    dbport=8080
    # Name of the Xmla Olap4j Driver
    driverName=org.olap4j.driver.xmla.XmlaOlap4jDriver

If your GeoMondrian is installed locally and listens on the port 8080 (for example if installed on a local tomcat).

You can now build and run the application:

    # Make sure the JAVA_HOME is set to the Java 7 JDK:
    export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/

    # Build and run the application
    ant run

## Proxy
In order to compile, `ant` needs to download external libraries on maven repositories.
If you are behind a proxy, you have to uncomment and configure the proxy block in [build.properties](https://github.com/loganalysis/mandoline/blob/master/build.properties)
with your proxy settings.

