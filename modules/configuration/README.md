# Configuration

This module contains the API classes and the implementation for the Ignite Configuration framework.
The idea is to provide the so-called _Unified Configuration_ — a common way of configuring both local Ignite nodes
and remote Ignite clusters. The original concept is described in
[IEP-55](https://cwiki.apache.org/confluence/display/IGNITE/IEP-55+Unified+Configuration).

## Concepts

### Configuration Schema

Type-safe schema of a configuration, which is used for generating public API interfaces and
internal implementations to avoid writing boilerplate code. 

All schema classes must end with the `ConfigurationSchema` suffix.

### Configuration Registry

`ConfigurationRegistry` is the entry point of the module. It is used to register root keys, validators, storages,
polymorphic extensions and to start / stop the component. Refer to the class javadocs for more details.

### Root Key

All Ignite configuration instances can be represented by a forest, where every node has a name, usually referred
to as a _key_. `RootKey` interface represents a type-safe object that holds the _key_ of the root node of the 
configuration tree. 

Instances of this interface are generated automatically and are mandatory for registering the configuration roots.

### Example Schema

An example configuration schema may look like the following:

```java
@ConfigurationRoot(rootName = "root", type = ConfigurationType.LOCAL)
public static class ParentConfigurationSchema {
    @NamedConfigValue
    private NamedElementConfigurationSchema elements;

    @ConfigValue
    private ChildConfigurationSchema child;

    @ConfigValue
    private PolymorphicConfigurationSchema polymorphicChild;
}

@Config
public static class ChildConfigurationSchema {
    @Value(hasDefault = true)
    public String str1 = "foobar";

    @Value
    @Immutable
    public String str2;
}

@PolymorphicConfig
public static class PolymorphicConfigurationSchema {
    @PolymorphicId(hasDefault = true)
    public String typeId = "first";
}

@PolymorphicConfigInstance("first")
public static class FirstPolymorphicInstanceConfigurationSchema extends PolymorphicConfigurationSchema {
    @Value(hasDefault = true)
    public int intVal = 0;
}
```

* `@ConfigurationRoot` marks the root schema. It contains the following properties:
  * `type` property, which can either be `LOCAL` or `DISTRIBUTED`. This property dictates the _storage_ type used 
    to persist the schema — `Vault` or `Metastorage`. `Vault` stores data locally, while `Metastorage` is a distributed
    system that should store only cluster-wide configuration properties;
  * `rootName` property assigns a _key_ to the root node of the tree that will represent 
    the corresponding configuration schema;
* `@Config` is similar to the `@ConfigurationRoot` but represents an inner configuration node;
* `@PolymorphicConfig` is similar to the `@Config` and an abstract class in java, i.e. it cannot be instantiated, but it can be subclassed;
* `@PolymorphicConfigInstance` marks an inheritor of a polymorphic configuration. This annotation has a single property called `value` - 
   a unique identifier among the inheritors of one polymorphic configuration, used to define the type (schema) of the polymorphic configuration we are dealing with now.
* `@ConfigValue` marks a nested schema field. Cyclic dependencies are not allowed;
* `@NamedConfigValue` is similar to `@ConfigValue`, but such fields represent a collection of properties, not a single
  instance. Every element of the collection will have a `String` name, similar to a `Map`.
  `NamedListConfiguration` interface is used to represent this field in the generated configuration classes. 
* `@Value` annotation marks the _leaf_ values. `hasDefault` property can be used to set default values for fields:
  if set to `true`, the default value will be used to initialize the annotated configuration field in case no value 
  has been provided explicitly. This annotation can only be present on fields of the Java primitive or `String` type.
    
  All _leaves_ must be public and corresponding configuration values **must not be null**;
* `@PolymorphicId` is similar to the `@Value`, but is used to store the type of polymorphic configuration (`@PolymorphicConfigInstance#value`), must be a `String` and placed as the first field in a schema;
* `@Immutable` annotation can only be present on fields marked with the `@Value` annotation. Annotated fields cannot be 
  changed after they have been initialized (either manually or by assigning a default value).

### Polymorphic configuration

This is the ability to create various forms of the same configuration.

Let's take an example of an SQL column configuration, suppose it can be one of the following types:
* varchar(max) - string with a maximum length;
* decimal(p,s) - decimal number with a fixed precision (p) and a scale (s);
* datetime(fsp) - date and time with a fractional seconds precision (fsp).

If you do not use polymorphic configuration, then the scheme will look something like this:

```java
@Config
public static class ColumnConfigurationSchema { 
    @Value
    public String type;

    @Value
    public String name;
    
    @Value
    public int maxLength;

    @Value
    public int precision;

    @Value
    public int scale;

    @Value
    public int fsp;
}
```

Such a scheme is redundant and can be confusing when using it, since it is not obvious which fields
are needed for each type of column. Instead, one can use a polymorphic configuration
that will look something like this:

```java
@PolymorphicConfig
public static class ColumnConfigurationSchema { 
    @PolymorphicId
    public String type;

    @Value
    public String name;
}

@PolymorphicConfigInstance("varchar")
public static class VarcharColumnConfigurationSchema extends ColumnConfigurationSchema {
    @Value
    public int maxLength;
}

@PolymorphicConfigInstance("decimal")
public static class DecimalColumnConfigurationSchema extends ColumnConfigurationSchema {
    @Value
    public int precision;

    @Value
    public int scale;
}

@PolymorphicConfigInstance("datetime")
public static class DatetimeColumnConfigurationSchema extends ColumnConfigurationSchema {
    @Value
    public int fsp;
}
```

Thus, a column can only be one of these (varchar, decimal and datetime) types and will contain the
type, name and fields specific to it.

## Generated API

Configuration interfaces are generated at compile time. For the example above, the following code would be generated: 

```java
public interface ParentConfiguration extends ConfigurationTree<ParentView, ParentChange> {
    RootKey<ParentConfiguration, ParentView> KEY = ...;

    NamedConfigurationTree<NamedElementConfiguration, NamedElementView, NamedElementChange> elements();
            
    ChildConfiguration child();

    PolymorphicConfiguration polymorphicChild();

    ParentView value();

    Future<Void> change(Consumer<ParentChange> change);
}

public interface ChildConfiguration extends ConfigurationTree<ChildView, ChildChange> {
    ConfigurationValue<String> str();

    ChildView value();

    Future<Void> change(Consumer<ChildChange> change);
}

public interface PolymorphicConfiguration extends ConfigurationTree<PolymorphicView, PolymorphicChange> {
    // Read only.  
    ConfigurationValue<String> typeId();

    PolymorphicView value();

    Future<Void> change(Consumer<PolymorphicChange> change);
}

public interface FirstPolymorphicInstanceConfiguration extends PolymorphicConfiguration {
    ConfigurationValue<Integer> intVal();
}
```

* `KEY` constant uniquely identifies the configuration root;
* `child()` method can be used to access the child node;
* `value()` method creates a corresponding _snapshot_ (an immutable view) of the configuration node;
* `change()` method should be used to update the values in the configuration tree.

### Configuration Snapshots

`value()` methods return a read-only view of the configuration tree, represented by a special set of _View_ interfaces.
For the example above, the following interfaces would be generated:

```java
public interface ParentView {
    NamedListView<? extends NamedElementView> elements();

    ChildView child();

    PolymorphicView polymorphicChild();
}

public interface ChildView {
    String str();
}

public interface PolymorphicView {
    String typeId();
}

public interface FirstPolymorphicInstanceView extends PolymorphicView {
    int intVal();
}
```

`ParentView#polymorphicChild()` will return a view of a specific type of polymorphic configuration, for example `FirstPolymorphicInstanceView`.

### Changing the configuration

To modify the configuration tree, one should use the `change` method, which executes the update requests 
asynchronously and in a transactional manner. Update requests are represented by a set of `Change` interfaces.
For the example above, the following interfaces would be generated:

```java
public interface ParentChange extends ParentView { 
    ParentChange changeElements(Consumer<NamedListChange<NamedElementChange>> elements);

    ParentChange changeChild(Consumer<ChildChange> child);

    ParentChange changePolymorphicChild(Consumer<PolymorphicChange> polymorphicChild);
}

public interface ChildChange extends ChildView {
    ChildChange changeStr(String str);
}

public interface PolymorphicChange extends FirstPolymorphicView {
    <T extends PolymorphicChange> T convert(Class<T> changeClass);
}

public interface FirstPolymorphicInstanceChange extends FirstPolymorphicInstanceView, PolymorphicChange {
    FirstPolymorphicInstanceChange changeIntVal(int intVal);
}
```

Example of updating all child nodes of the parent configuration in a single transaction:

```java
ParentConfiguration parentCfg = ...;

parentCfg.change(parent ->
    parent.changeChild(child ->
        child.changeStr("newStr1")
    )
).get();

ChildConfiguration childCfg = parentCfg.child();

childCfg.changeStr("newStr2").get();
```

Example of changing the type of a polymorphic configuration:

```java
ParentConfiguration parentCfg = ...;

parentCfg.polymorphicChild()
        .change(polymorphicCfg -> 
            polymorphicCfg.convert(FirstPolymorphicInstanceChange.class).changeIntVal(100)
        ).get();
```

It is possible to execute several change requests for different roots in a single transaction, but all these roots 
_must have the same storage type_. However, this is only possible using the command line tool via the REST API, 
there's no public Java API at the moment.
