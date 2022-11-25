# 单元测试

## 1. shardctrler

```shell
# 测试adjustConfig
go test -run AdjustConfig -cover -coverprofile=size_coverage.out
# 查看adjustConfig 覆盖率
go tool cover -func=size_coverage.out
# 查看adjustConfig 覆盖情况
go tool cover -html=size_coverage.out
```



# reference
1. https://brantou.github.io/2017/05/24/go-cover-story/


