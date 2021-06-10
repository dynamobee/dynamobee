[![Build Status](https://travis-ci.org/dynamobee/dynamobee.svg?branch=master)](https://travis-ci.org/dynamobee/dynamobee)  [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.dynamobee/dynamobee/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.dynamobee/dynamobee) [![Licence](https://img.shields.io/hexpm/l/plug.svg)](https://github.com/dynamobee/dynamobee/blob/master/LICENSE)
---
# DynamoBee
This tool is forked off [mongobee](https://github.com/mongobee/mongobee) repo that is maintained by the @mongobee community.

**dynamobee** is a Java tool which helps you to *manage changes* in your DynamoDB and *synchronize* them with your application.
The concept is very similar to other db migration tools such as [Liquibase](http://www.liquibase.org) or [Flyway](http://flywaydb.org) but *without using XML/JSON/YML files*.

The goal is to keep this tool simple and comfortable to use.


**dynamobee** provides new approach for adding changes (change sets) based on Java classes and methods with appropriate annotations.

## Getting started

### Add a dependency

With Maven
```xml
<dependency>
  <groupId>com.github.dynamobee</groupId>
  <artifactId>dynamobee</artifactId>
</dependency>
```
With Gradle
```groovy
compile 'com.github.dynamobee:dynamobee'
```

### Usage with Spring

You need to instantiate Dynamobee object and provide some configuration.
If you use Spring can be instantiated as a singleton bean in the Spring context. 
In this case the migration process will be executed automatically on startup.

```java
@Bean
public Dynamobee dynamobee(){
  Dynamobee runner = new Dynamobee(db); //DynamoDB Client: com.amazonaws.services.dynamodbv2.AmazonDynamoDB
  runner.setChangeLogsScanPackage(
       "com.example.yourapp.changelogs"); // the package to be scanned for changesets
  
  return runner;
}
```


### Usage without Spring
Using dynamobee without a spring context has similar configuration but you have to remember to run `execute()` method to start a migration process.

```java
Dynamobee runner = new Dynamobee(db); //DynamoDB Client: see com.amazonaws.services.dynamodbv2.AmazonDynamoDB
runner.setChangeLogsScanPackage(
     "com.example.yourapp.changelogs"); // package to scan for changesets

runner.execute();         //  ------> starts migration changesets
```

Above examples provide minimal configuration. `Dynamobee` object provides some other possibilities (setters) to make the tool more flexible:

```java
runner.setChangelogTableName(logColName);   // default is dbchangelog, collection with applied change sets
runner.setEnabled(shouldBeEnabled);              // default is true, migration won't start if set to false
```


### Creating change logs

`ChangeLog` contains bunch of `ChangeSet`s. `ChangeSet` is a single task (set of instructions made on a database). In other words `ChangeLog` is a class annotated with `@ChangeLog` and containing methods annotated with `@ChangeSet`.

```java 
package com.example.yourapp.changelogs;

@ChangeLog
public class DatabaseChangelog {
  
  @ChangeSet(order = "001", id = "someChangeId", author = "testAuthor")
  public void importantWorkToDo(DB db){
     // task implementation
  }


}
```
#### @ChangeLog

Class with change sets must be annotated by `@ChangeLog`. There can be more than one change log class but in that case `order` argument should be provided:

```java
@ChangeLog(order = "001")
public class DatabaseChangelog {
  //...
}
```
ChangeLogs are sorted alphabetically by `order` argument and changesets are applied due to this order.

#### @ChangeSet

Method annotated by @ChangeSet is taken and applied to the database. History of applied change sets is stored in a collection called `dbchangelog` (by default) in your DynamoDB

##### Annotation parameters:

`order` - string for sorting change sets in one changelog. Sorting in alphabetical order, ascending. It can be a number, a date etc.

`id` - name of a change set, **must be unique** for all change logs in a database

`author` - author of a change set

`runAlways` - _[optional, default: false]_ changeset will always be executed but only first execution event will be stored in dbchangelog collection

##### Defining ChangeSet methods
Method annotated by `@ChangeSet` can have one of the following resolved parameters:

- com.amazonaws.services.dynamodbv2.document.DynamoDB
- com.amazonaws.services.dynamodbv2.AmazonDynamoDB
- com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper  
- org.socialsignin.spring.data.dynamodb.core.DynamoDBTemplate
- org.springframework.core.env.Environment

Examples :

```java
@ChangeSet(order = "001", id = "someChangeWithoutArgs", author = "testAuthor")
public void someChange1() {
   // method without arguments can do some non-db changes
}

@ChangeSet(order = "002", id = "someChangeWithDynamoDB", author = "testAuthor")
public void someChange2(DynamoDB dynamoDB) {
  // type: com.amazonaws.services.dynamodbv2.document.DynamoDB
}

@ChangeSet(order = "003", id = "someChangeWithAmazonDynamoDB", author = "testAuthor")
public void someChange3(AmazonDynamoDB amazonDynamoDB) {
  // type: com.amazonaws.services.dynamodbv2.AmazonDynamoDB
}

@ChangeSet(order = "004", id = "someChangeWithDynamoDBTemplate", author = "testAuthor")
public void someChange4(DynamoDBTemplate dynamoDBTemplate) {
  // type: org.socialsignin.spring.data.dynamodb.core.DynamoDBTemplate
}

@ChangeSet(order = "005", id = "someChangeWithDynamoDBTemplateAndEnvironment", author = "testAuthor")
public void someChange5(DynamoDBTemplate dynamoDBTemplate, Environment environment) {
  // type: org.socialsignin.spring.data.dynamodb.core.DynamoDBTemplate
  // type: org.springframework.core.env.Environment
}

@ChangeSet(order = "006", id = "someChangeWithDynamoDBTemplateAndAmazonDynamoDB", author = "testAuthor")
public void someChange6(DynamoDBTemplate dynamoDBTemplate, AmazonDynamoDB amazonDynamoDB) {
  // type: org.socialsignin.spring.data.dynamodb.core.DynamoDBTemplate
  // type: com.amazonaws.services.dynamodbv2.AmazonDynamoDB
}

@ChangeSet(order = "007", id = "someChangeWithDynamoDBTemplateAndAmazonDynamoDB", author = "testAuthor")
public void someChange7(AmazonDynamoDB amazonDynamoDB, DynamoDBTemplate dynamoDBTemplate) {
  // type: com.amazonaws.services.dynamodbv2.AmazonDynamoDB
  // type: org.socialsignin.spring.data.dynamodb.core.DynamoDBTemplate
}

@ChangeSet(order = "008", id = "someChangeWithAmazonDynamoDBAndEnvironment", author = "testAuthor")
public void someChange8(AmazonDynamoDB amazonDynamoDB, Environment environment) {
  // type: com.amazonaws.services.dynamodbv2.AmazonDynamoDB
  // type: org.springframework.core.env.Environment
}

```

### Using Spring profiles
     
**dynamobee** accepts Spring's `org.springframework.context.annotation.Profile` annotation. If a change log or change set class is annotated  with `@Profile`, 
then it is activated for current application profiles.

_Example 1_: annotated change set will be invoked for a `dev` profile
```java
@Profile("dev")
@ChangeSet(author = "testuser", id = "myDevChangest", order = "01")
public void devEnvOnly(DB db){
  // ...
}
```
_Example 2_: all change sets in a changelog will be invoked for a `test` profile
```java
@ChangeLog(order = "1")
@Profile("test")
public class ChangelogForTestEnv{
  @ChangeSet(author = "testuser", id = "myTestChangest", order = "01")
  public void testingEnvOnly(DB db){
    // ...
  } 
}
```

#### Enabling @Profile annotation (option)
      
To enable the `@Profile` integration, please inject `org.springframework.core.env.Environment` to you runner.

```java      
@Bean @Autowired
public Dynamobee dynamobee(Environment environment) {
  Dynamobee runner = new Dynamobee(dynamoDb);
  runner.setSpringEnvironment(environment)
  //... etc
}
```

