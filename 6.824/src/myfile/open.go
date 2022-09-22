package myfile

import "os"
// import "log"

func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func OpenAnyway(path string) (*os.File, error){
	if !FileExists(path) {
		_, err := os.Create(path)
		if err != nil {
			return nil, err
		}
	}
	return os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0666)
}