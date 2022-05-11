---
sidebar_label: Kafka
title: TDengine Kafka Connector
---

TDengine Kafka Connector 本质上是 Kafka Connect 的两个插件: TDengine Source Connector 和 TDengine Sink Connector。借助 Confluent 平台，用户只需提供简单配置文件，就可以消费 Kafka 的数据到 TDengine， 或将 TDengine 的数据推送到 Kafka。

## 什么是 Kafka Connect？

如今 Apache Kafka 已经从最初的消息队列发展成为一个完备的事件流平台。Kafka Connect 就是这个平台的一个组件，用于使其它系统， 比如数据库、云服务、文件系统等能方便地连接到 Kafka。数据既可以通过 Kafka Connect 从其它系统流向 Kafka, 也可以通过 Kafka Connect 从 Kafka 流向其它系统。 

![](Kafka_Connect.png)

Kafka Connect 可以看作由众多插件组成的生态系统。从其它系统读数据的插件称为 Source Connector, 写数据到其它系统的插件称为 Sink Connector。TDengine Source Connector 的作用就是把数据实时地从 TDengine 读出来交给 Kafka Connect。TDengine Sink Connector 的作用就是 从 Kafka Connect 接收数据写入 TDengine 的插件。Source Connector 和 Sink Connector 都不会直接连接 Kafka Broker。 Source Connector 只是把数据转交给 Kafka Connect。 SinkConnector 也只需从 Kafka Connect 接收数据。所以 Kafka Connect 也可以看作Kafka 的一个客户端应用。Kafka Connect 有自己独立的进程，自身的设计也是分布式的。

![](streaming-integration-with-kafka-connect.png)

Kafka 已经提供了生产者和消费者 API 以及客户端库用来与 Kafka 集成，为什么还需要 Kafka Connect？ 因为一个好 Kafka 客户端程序，不是单单生产或消费数据，还需要考虑容错、重启、日志、弹性伸缩、序列化、反序列化等。当开发者自己完成了这一切就开发了一个和 Kafka Connect 类似的东西。与 Kafka 集成是 Kafka Connect 已经解决的问题, 用户不需要重复造轮子，只有在少数边缘场景才需要定制化的集成方案。

## 什么是 Confluent？

Confluent 在 Kafka 的基础上增加很多扩展功能。比如：
1. Schema Registry
2. REST 代理
3. 非 Java 客户端
4. 100+ 打包好的 Kafka Connect 插件
5. 管理和监控 Kafka 的 GUI —— Confluent 控制中心
   
这些扩展功能有的包含在社区版本的 Confluent 中，有的只有企业版能用。
![](confluentPlatform.png)

Confluent 企业版提供了 `confluent` 命令行工具管理各个组件。在本教程我们将多次用到此命令。

## 前提条件

以下本教程的前提条件。

1. Linux 操作系统（ubuntu、centos 或其它都可以）
2. 已安装 Java 8 和 Maven
3. 已安装 Git

## 安装 Confluent

Confluent 提供了 Docker 和二进制包两种安装方式。本教程为了演示简单（无需进入容器操作），采用二进制包的方式安装。在任意目录下执行：
```
curl -O http://packages.confluent.io/archive/7.1/confluent-7.1.1.tar.gz
tar xzf confluent-7.1.1.tar.gz
mv confluent-7.1.1 /opt
```

然后需要把 `$CONFLUENT_HOME/bin` 目录加入 PATH。

``` title=".profile"
export CONFLUENT_HOME=/opt/confluent-7.1.1
PATH=$CONFLUENT_HOME/bin
export PATH
```
以上脚本可以追加到当前用户的 profile 文件（~/.profile 或 ~/.bash_profile）

安装完成之后，可以输入`confluent version`做简单验证：
```
# confluent version
confluent - Confluent CLI

Version:     v2.6.1
Git Ref:     6d920590
Build Date:  2022-02-18T06:14:21Z
Go Version:  go1.17.6 (linux/amd64)
Development: false
```

## 安装 TDengine Connector 插件

## 安装并启动 TDengine


## 参考

1. https://www.confluent.io/what-is-apache-kafka
2. https://developer.confluent.io/learn-kafka/kafka-connect/intro
3. https://docs.confluent.io/platform/current/platform.html
