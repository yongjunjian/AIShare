package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"flag"
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
    if err != nil{
        log.Panicf("get conf failed:%s",err.Error())
    }
	InitLog(logPath)
	log.Println("Starting v1 server,port=" + port)
	http.HandleFunc("/get_car_record/", getCarRecord)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func upldateImage(res http.ResponseWriter, req *http.Request) {
	body, _ := ioutil.ReadAll(req.Body)
	res.Write([]byte("success" + string(body)))
}
