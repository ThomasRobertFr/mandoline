solap4py-java
=============

Java application needed by [solap4py](https://github.com/loganalysis/solap4py)

Make sure you configure the "config.dist" file with your database configuration.

It works as a server application, you compile everything using `ant` command. Then you can launch the server with `java -jar Solap4py.jar` and you can use Solap4py to request [GeoMondrian](http://www.spatialytics.org/fr/projets/geomondrian/).

## Dependencies

This application use [olap4j](https://github.com/olap4j/olap4j) to interact with GeoMondrian. So you need a GeoMondrian server installed.

## Roadmap

This application aims to provide access to GeoMondrian for [our custom GeoNode](https://github.com/loganalysis/geonode) in order to display geographic business intelligence data.

We are not currently trying to make this application available for an other purpose. If you want to get involved and help, please contact us !

