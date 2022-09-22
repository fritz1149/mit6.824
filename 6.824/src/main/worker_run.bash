n=$1
go build -race -buildmode=plugin ../mrapps/indexer.go
i=0
while (($i < $n))
do
go run -race mrworker.go indexer.so 1> "log$i" 2>&1 &
let "i++"
done