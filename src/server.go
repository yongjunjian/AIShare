package main

import (
	"flag"
	"io/ioutil"
	"log"
    "os"
    "io"
	"net/http"
	. "utils"
)

func main() {
	//解析命令行参数
	var confPath string
	flag.StringVar(&confPath, "c", "conf/server.conf", "configure file path")
	flag.Parse()

	//初始化配置
	InitConfig(confPath)

	//初始化日志
	logPath, err := C.GetValue("server", "logPath")
	if err != nil {
		log.Panicf("get conf failed:%s", err.Error())
	}
	InitLog(logPath)
	port, err := C.GetValue("server", "port")
	log.Println("Starting v1 server,port=" + port)
	http.HandleFunc("/uploadImage/", uploadImage)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func uploadImage(w http.ResponseWriter, r *http.Request) {
	reader, err := r.MultipartReader()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}

		log.Printf("FileName=[%s], FormName=[%s]\n", part.FileName(), part.FormName())
		if part.FileName() == "" { // this is FormData
			data, _ := ioutil.ReadAll(part)
			logt.Printf("FormData=[%s]\n", string(data))
		} else { // This is FileData
			dst, _ := os.Create("./" + part.FileName() + ".upload")
			defer dst.Close()
			io.Copy(dst, part)
		}
	}
	res.Write([]byte(`{"code":0,"msg":"success"}`))
}
