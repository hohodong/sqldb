# sqldb


sqldb 是使用 Java实现的简易关系型 SQL 数据库，支持常见的表管理、SQL 数据查询与存储等功能，具备完整的 ACID 事务特性，具备一定的SQL执行优化功能

## 运行方法
运行sqldb/src/main/java/com/database/cli目录下的CommandLineInterface.java主方法，该方法会创建一个数据库实例，并创建一个简单的交互界面。

## 项目结构

### cli

包含数据库命令行界面的所有逻辑。运行CommandLineInterface.java的主方法将创建一个数据库实例，并创建一个简单的交互界面。

#### cli/parser

将用户输入的查询（字符串）转换为表示查询的节点树（解析树）。

#### cli/visitor

帮助遍历由解析器创建的树，并创建数据库可以直接使用的对象。



### concurrency
为数据库添加多粒度锁的框架。
### databox
表示存储在数据库中的值及其类型的类。各种 DataBox 类表示特定类型的值，而 Type 类表示数据库中使用的类型。

例如:
```java
DataBox x = new IntDataBox(42); // The integer value '42'.
Type t = Type.intType();        // The type 'int'.
Type xsType = x.type();         // Get x's type, which is Type.intType().
int y = x.getInt();             // Get x's value: 42.
String s = x.getString();       // An exception is thrown, since x is not a string.
```
### index

实现 B+ 树索引的框架。

### memory

用于缓冲区管理。

`BufferFrame` 类表示单个缓冲区帧（缓冲池中的页面），并支持pin/unpin操作以及对缓冲区帧的读写操作。所有读写操作都需要该帧被固定（通常通过 `requireValidFrame` 方法完成，该方法在必要时由磁盘重新加载数据，然后返回一个固定的帧给页面）。

`BufferManager` 接口是缓冲区管理器的公共接口。

`BufferManagerImpl` 类使用带有可配置驱逐策略的写回缓冲区缓存实现了缓冲区管理器。它负责将页面（通过磁盘空间管理器）提取到缓冲区帧中，并返回 `Page` 对象以允许在内存中操作数据。

`Page` 类表示单个页面。当页面中的数据被访问或修改时，它会将读写操作委托给包含该页面的基础缓冲区帧。

`EvictionPolicy` 接口定义了一些方法，这些方法决定了缓冲区管理器在必要时如何由内存中驱逐页面。这些方法的实现包括 `LRUEvictionPolicy`（用于 LRU）和 `ClockEvictionPolicy`（用于时钟算法）。

### io

用于磁盘空间管理。

`DiskSpaceManager` 接口是**sqldb**磁盘空间管理器的公共接口。

`DiskSpaceMangerImpl` 类是磁盘空间管理器的实现，它将页面组（分区）映射到操作系统级文件，为每个页面分配虚拟页号，并由磁盘加载/写入这些页面。

### query

用于管理和操作查询的类。


`QueryPlan` 类表示执行查询的计划，支持一定程度的查询优化。

### recovery

实现 ARIES 数据库恢复的框架。

### table

整个表和记录的类。

`Table` 类表示数据库中的一个表。

`Schema` 类表示表的 _schema_（列名及其类型的列表）。

`Record` 类表示表的一条记录（一行）。记录由多个 `DataBox` 组成（每个 `DataBox` 对应于该记录所属表的一个列）。

`RecordId` 类标识表中的单条记录。


`PageDirectory` 类是使用页面目录的堆文件的实现。

#### table/stats

`table/stats` 目录包含用于跟踪表统计信息的类。

### Transaction.java

`Transaction` 接口是事务的 _公共_ 接口——它包含用户用来查询和操作数据的方法。

此接口部分由 `AbstractTransaction` 抽象类实现，并在 `Database.Transaction` 内部类中完全实现。

### TransactionContext.java

`TransactionContext` 接口是事务的 _内部_ 接口——它包含与当前事务相关联的方法，内部方法（例如表记录获取）可以利用这些方法。

当前运行事务的事务上下文在 `Database.Transaction` 调用开始时设置（并通过静态 `getCurrentTransaction` 方法可用），并在调用结束时取消设置。

此接口部分由 `AbstractTransactionContext` 抽象类实现，并在 `Database.TransactionContext` 内部类中完全实现。

### Database.java



`Database`类代表整个数据库，它是 **sqldb** 的公共接口。因为所有的工作都需要在事务中完成，用户需要通过`Database#beginTransaction`创建一个事务，然后调用其中用于执行选择、插入和更新等操作的方法。

例如:
```java
Database db = new Database("database-dir");

try (Transaction t1 = db.beginTransaction()) {
    Schema s = new Schema()
            .add("id", Type.intType())
            .add("firstName", Type.stringType(10))
            .add("lastName", Type.stringType(10));

    t1.createTable(s, "table1");

    t1.insert("table1", 1, "Jane", "Doe");
    t1.insert("table1", 2, "John", "Doe");

    t1.commit();
}

try (Transaction t2 = db.beginTransaction()) {
    // SELECT * FROM table1
    Iterator<Record> iter = t2.query("table1").execute();

    System.out.println(iter.next()); // prints [1, John, Doe]
    System.out.println(iter.next()); // prints [2, Jane, Doe]

    t2.commit();
}

db.close();
```



