Mandoline
=============

Mandoline is a java server that provides a simple `JSON` API to GeoMondrian. To install it (ubuntu 14.04):

    # Install build tools
    sudo apt-get install ant
    sudo apt-get install openjdk-7-jdk

    # Retrieve the source
    git clone https://github.com/loganalysis/mandoline.git

Now, you can either generate a debian package to install it on your system, a runnable jar file to run the server from 
wherever you want, or directly run it in the cloned git directory.

### Debian package
If you want to install `Mandoline` on your system, you can proceed as follows :

    # Generate the .deb package
    ant package
    # A file is created in dist/mandoline.deb, you can then install the package on any Ubuntu 14.04 host you want
    sudo dpkg -i mandoline.deb

A configuration file is generated in `/etc/mandoline.properties`, you should edit it as a superuser to have something like this :

    #Configuration of the database
    dbhost=localhost
    dbport=8080
    # Name of the Xmla Olap4j Driver
    driverName=org.olap4j.driver.xmla.XmlaOlap4jDriver

If your GeoMondrian is installed locally and listens on the port 8080 (for example if installed on a local tomcat).

You can now run the server with :

    # Make sure the JAVA_HOME is set to the Java 7 JDK:
    export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/

    # Run the server
    sudo initctl start mandoline

And stop it with :

    # Stop the server
    sudo initctl stop mandoline


### Runnable jar 
If you want to generate a runnable jar, you can run:

    # Generate the runnable jar
    ant jar

The file `dist/mandoline.jar` is generated and you can run it with :

    # Make sure the JAVA_HOME is set to the Java 7 JDK:
    export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/
    
    # Run the server
    java -jar dist/mandoline.jar

Note that you can run it from wherever you want, but you need to have a `mandoline.properties` file in the folder in which 
you run it from. You can also copy `condig.dist` file which can be found at the root of the git repository and configure it 
as necessary : 

    #Configuration of the database
    dbhost=localhost
    dbport=8080
    # Name of the Xmla Olap4j Driver
    driverName=org.olap4j.driver.xmla.XmlaOlap4jDriver

### Run directly with ant
Finally, you can run the server directly from the cloned git repository with a dedicated ant task.
You should edit the `config.dist` file to add your GeoMondrian's connection information to have something like this : 

    #Configuration of the database
    dbhost=localhost
    dbport=8080
    # Name of the Xmla Olap4j Driver
    driverName=org.olap4j.driver.xmla.XmlaOlap4jDriver

You can now build and run the application :

    # Make sure the JAVA_HOME is set to the Java 7 JDK:
    export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/

    # Build and run the application
    ant run

## Proxy
In order to compile, `ant` needs to download external libraries on maven repositories.
If you are behind a proxy, you have to uncomment and configure the proxy block in [build.properties](https://github.com/loganalysis/mandoline/blob/master/build.properties)
with your proxy settings.

