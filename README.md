# 基于Dragonboat二次封装的Raft库

### 快速开始
```
$ go get github.com/invxp/pollinosis@latest
```
### 支持完整的Raft本地持久化存储库
1. 完善本地持久化状态机
2. 通过PebbleDB进行KV存储
3. 根据业务类型可选择in-memory类型状态机
4. 提供完整的使用 / 测试用例