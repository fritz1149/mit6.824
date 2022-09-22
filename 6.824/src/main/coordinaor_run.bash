rm intermediate-*
rm mr-out-*
go run -race mrcoordinator.go pg-*.txt 1> master_log 2>&1