sort mr-out* | grep . > mr-wc-all

go build -race -buildmode=plugin ../mrapps/indexer.go
# ./mrsequential wc.so pg*txt
go run -race mrsequential.go indexer.so pg*.txt
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

cmp mr-wc-all mr-correct-wc.txt