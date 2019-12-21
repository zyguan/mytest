module github.com/zyguan/mytest/cmd

go 1.13

require (
	github.com/go-logr/logr v0.1.0
	github.com/go-sql-driver/mysql v1.4.1
	github.com/google/uuid v1.1.1
	github.com/pingcap/errors v0.11.4
	github.com/stretchr/testify v1.4.0
	github.com/zyguan/mytest v0.0.0-20191127121506-8e628ad8e804
	github.com/zyguan/zapglog v0.0.0-20191220071413-48c3b1e72c1f
)

replace github.com/zyguan/mytest => ../
