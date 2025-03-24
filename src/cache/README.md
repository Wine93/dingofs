Block Cache Layer
===

```
+----------------+
|     Client     |
+----------------+
        |
        | (put、range...)
        v
+----------------+  -----upload----->  +----------------+
|  Block  Cache  |                     |       S3       |
+----------------+  <----download----  +----------------+
        |
        | (stage、removestage、cache、load...)
        v
+----------------+
|  Cache  Store  |
+----------------+
        |
        | (writefile、readfile...)
        v
+----------------+
|      Disk      |
+----------------+
```

Cache Gorup
===

```
CacheGroupNodeOption option;
auto node = std::make_shared<CacheGroupNodeImpl>(option);
int rc = node->Init();

```



```cpp
BlockCacheOption option;
option.stage = true;

auto block_cache = std::make_shared<BlockCache>(option);
int rc = block_cache->Init();
if (rc != 0) {
    std::cerr << "block_cache init failed" << std::endl;
    return -1;
}
```

Remote Cache
===

```cpp
CacheGroupNodeOption option;
auto node = std::make_shared<CacheGroupNodeImpl>(option);
int rc = node->Init();
```

