# TibcoKafkaConnector
A custom Kafka connector for TIBCO version 5.x.
This code uses set and get methods to get variable values.
For this code to work this way, you would need to first add a Java code activity in your TIBCO Designer, and then add input parameters.
Place values in these parameters with a mapper beforehand.
This particular Kafka consumer uses only SSL, so this is why the properties needed are truststore and keystore.
In some cases, Kerberos authentication may be used, which would require extra properties.
This code also uses some debugging with "custom_logger", custom logger is a log4j rolling file logger that prints every debug in a specified file destination.
