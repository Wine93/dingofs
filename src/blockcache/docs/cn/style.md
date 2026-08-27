# blockcache 声明排布约定

约束的是**声明写在哪里**，不是命名和实现。命名规范见仓库根 `.clang-tidy`，格式化见 `.clang-format`（Google 风格，80 列）。

目的只有一个：读者靠**位置**就能回答「这个类提供哪些接口、有哪些成员」，不必逐行扫完整个类。

`infiniband/` 是本约定的样板，可直接对照阅读。

## 一、类内顺序

每个 access section（`public:` / `protected:` / `private:`）内部固定七段，空段省略，**段与段之间空一行**：

1. `friend` 声明 —— 集中放在它所属 section 的开头（本仓惯例是 `private:` 段首）
2. 嵌套类型：`using` / `enum` / 嵌套 `struct`、`class`
3. `static constexpr` 常量
4. 静态工厂：`Create` / `Open` / `Register` / `InitOnThisShard`
5. 构造函数 → 析构函数 → `= delete` 的拷贝 → 移动构造/移动赋值
6. 其余成员函数
7. 数据成员

第 5 段的次序（拷贝删除写在析构**之后**）与 Google Style 字面顺序不同，取的是 brpc 的写法，也是本仓既有的多数写法。同一个类里不要两种写法并存。

第 6 段内部再按五道分组，组间空一行：

| 道 | 放什么 | 例 |
|----|--------|-----|
| ① 建立 | 把对象从「构造完了」推到「能用了」 | `Init` `Start` `SetPeerCredits` |
| ② 主路径 | 这个类存在的理由 | `Send` `Call` `Poll` `Admit` `Open` |
| ③ 回调 | 别人完成了什么，回头通知本类 | `OnNewMessage` `OnSendWc` `OnError` |
| ④ 拆除 | 停下来、失败、还资源 | `Shutdown` `Drain` `SetBroken` `FailAll` `Reset` |
| ⑤ 访问器 | `lower_case()` getter/setter、`bool xxx() const` 谓词 | `outstanding()` `failed()` `name()` |

**访问器永远垫底**。这一条优先级最高：override 组内部按基类声明序排（`Session` 跟 `blockcache::Connection` 走），与「访问器垫底」冲突时，访问器垫底赢。

## 二、三条硬约束

- **不写 `// ---- xxx ----` 之类的分段横幅**。分段靠固定顺序加空行，不靠注释。解释「为什么」的既有注释保留。
- **不动数据成员的顺序**。`Infiniband`、`Connection`、`QueuePairGroup`、`Session` 的成员次序写明了是析构次序（"Declaration order is teardown order"），重排会改语义与结构体布局。要分组只加空行。
- 排布调整就只做排布调整：不顺手改名、不改签名、不动函数体。这样改动才能用「排序后逐行相同」和「反汇编逐条相同」验证。

## 三、.cc 的规矩

- **定义顺序逐个镜像 .h 的声明顺序**。这是 Google Style 的明文要求，也是 `check_decl_order.py` 唯一检查的东西。
- **文件级 helper 集中在文件顶部**，第一个成员定义之前，放匿名命名空间或用 `static`。夹在成员定义中间的 helper 会让上面的镜像规则一改就编不过。

## 四、门禁

```bash
python3 src/blockcache/tools/check_decl_order.py src/blockcache/infiniband
```

对每个 `X.cc` 找同名 `X.h`，比对两边都出现的成员的先后次序；不一致就打印 `header:` / `source:` 两行并以 1 退出。只做这一件事：头文件里怎么内联、怎么分段它不管。

## 五、这套顺序的出处

| 来源 | 做法 |
|------|------|
| Google C++ Style Guide《Declaration Order》 | 类型与别名 → 静态常量 → 工厂函数 → 构造与赋值 → 析构 → 其余函数 → 数据成员；并要求 .cc 按声明序定义 |
| brpc `Channel` | 友元集中类首 → 构造/析构 → `DISALLOW_COPY_AND_ASSIGN` → `Init()` → `CallMethod()` → `options()` → 私有 helper → 数据成员垫底 |
| brpc `Socket` | 友元一整块在类首，随后嵌套类型，`public:` 起手是静态常量，再 ctor/dtor，再操作 |
| leveldb / abseil | 同 Google 字面序（拷贝删除写在析构之前，本仓不采纳这一点） |

四家一致的部分——工厂在构造之前、类型与常量最前、数据成员垫底、访问器垫在函数尾部——就是上面第一节。
