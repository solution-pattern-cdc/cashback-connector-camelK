// camel-k: language=java 
// camel-k: property=file:cashback-customer-connector.properties
// camel-k: dependency=camel:jdbc
// camel-k: dependency=mvn:io.quarkus:quarkus-jdbc-postgresql

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.LoggingLevel;

public class CashbackCustomerConnector extends RouteBuilder {
  @Override
  public void configure() throws Exception {

      // Write your routes here, for example:
    from("kafka:{{kafka.customer.topic.name}}?groupId={{kafka.customer.consumer.group}}" +
        "&autoOffsetReset=earliest")
    .routeId("kafka2PostgresqlCustomer")
    .unmarshal().json()
    .log(LoggingLevel.DEBUG, "Customer event received: ${body}")
    .filter().jsonpath("$..[?(@.op =~ /d|t|m/)]")
        .log(LoggingLevel.DEBUG, "Filtering out change event which is not 'create' or 'update'")
        .stop()
    .end()
    .choice()
    .when().jsonpath("$..[?(@.op =~ /c|r/)]").log("Create")
    .setBody(simple("INSERT INTO customer (customer_id, name, status) VALUES (${body[after][customer_id]}, '${body[after][name]}', '${body[after][status]}');"))
    .when().jsonpath("$..[?(@.op == 'u')]").log("Update")
    .setBody(simple("UPDATE customer SET name = '${body[after][name]}', status = '${body[after][status]}' WHERE customer_id = ${body[after][customer_id]};"))
    .end()
    .log(LoggingLevel.DEBUG, "SQL statement: ${body}")
    .to("jdbc:default");
  }
}
