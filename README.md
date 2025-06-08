# RaftKV
RaftKV是一个基于Go的分布式键值数据库。

RaftKV采用Multi-Raft架构，支持数据分片处理与分片迁移。

RaftKV支持leader选举、日志复制、snapshot等基本功能。

主要实现代码位于src/raft/raft.go
