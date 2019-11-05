package utils
import(
    "time"
	"github.com/Unknwon/goconfig"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"log"
    "strings"
	"os"
    "github.com/colinmarc/hdfs"
    "path"
    "database/sql"
    "fmt"
    "github.com/fatih/set"
    _ "github.com/go-sql-driver/mysql"
)

//日志相关
type MultiWriter struct {
	filew io.Writer
	stdw  io.Writer
}

func (mw *MultiWriter) Write(p []byte) (n int, err error) {
	n1, err := mw.filew.Write(p)
	if err != nil {
		return n1, err
	}
	n2, err := mw.stdw.Write(p)
	if err != nil {
		return n2, err
	}
	return n1, err
}

func InitLog(logPath string){
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(&MultiWriter{
		filew: &lumberjack.Logger{
			Filename: logPath,
		},
		stdw: os.Stderr})
}

//配置相关
var C *goconfig.ConfigFile
func InitConfig(confPath string){
	config, err := goconfig.LoadConfigFile(confPath)
	if err != nil {
		log.Panicf("配置文件不存在或者不合法:%s", confPath)
	}
	C = config
}

func GetBackDays(fromDay string, backDaysNum int) ([]string, error){
    const TIME_LAYOUT = "20060102"
    days := make([]string, 0)
    ts, err := time.Parse(TIME_LAYOUT, fromDay)
    if err != nil{
       return nil, err 
    }
    for i := 0; i < backDaysNum; i++{
        hour,_ := time.ParseDuration("-24h")
        ts = ts.Add(hour)
        days = append(days, ts.Format(TIME_LAYOUT))
    }
    return days,nil
}

func PullData(remotePath string, localTempDir string) (string, error){
	rpcAddress, err := C.GetValue("hadoop", "rpcAddress")
	if err != nil {
		log.Panicf("get config failed:%s", err.Error())
	}
    client, err := hdfs.New(rpcAddress)
    if err != nil{
        log.Panicf("connect hdfs server failed:"+ err.Error())
    }
    defer client.Close()

    localPath := localTempDir+path.Base(remotePath)
	err = client.CopyToLocal(remotePath, localPath)
	if err != nil {
        log.Printf("copy to local failed:"+err.Error())
        return "", err
	}
    return localPath, nil
}

func PushData(localPath string, remoteDir string) error{
	rpcAddress, err := C.GetValue("hadoop", "rpcAddress")
	if err != nil {
		log.Panicf("get config failed:%s", err.Error())
	}
    client, err := hdfs.New(rpcAddress)
    if err != nil{
        log.Panicf("connect hdfs server failed:"+ err.Error())
    }
    defer client.Close()

    remotePath := remoteDir+path.Base(localPath)
	return client.CopyToRemote(localPath, remotePath)
}

func RemoteMkdir(remoteDir string) error{
	rpcAddress, err := C.GetValue("hadoop", "rpcAddress")
	if err != nil {
		log.Panicf("get config failed:%s", err.Error())
	}
    client, err := hdfs.New(rpcAddress)
    if err != nil{
        log.Panicf("connect hdfs server failed:"+ err.Error())
    }
    defer client.Close()
    return client.MkdirAll(remoteDir, 666)
} 

func CheckRemoteFile(path string) bool{
	rpcAddress, err := C.GetValue("hadoop", "rpcAddress")
	if err != nil {
		log.Panicf("get config failed:%s", err.Error())
	}
    client, err := hdfs.New(rpcAddress)
    if err != nil{
        log.Panicf("connect hdfs server failed:"+ err.Error())
    }
    defer client.Close()
    _, err = client.Stat(path)
    if err != nil{
        return false
    }else{
        return true
    }
}

type  TollgateInfo struct{
  NodeId sql.NullInt64 //卡口ID
  ParentId sql.NullInt64 //父ID
  NodeCode sql.NullString  //该卡口的外部code编号
  NodeName sql.NullString  //卡口名称
  NodeDesc sql.NullInt64  //卡口描述
  NodeIp sql.NullString  //卡口IP地址
  NodePort sql.NullInt64 //卡口端口号
  NodeProtocol sql.NullString //卡口协议
  NodeUser sql.NullString  //用户ID
  NodePwd sql.NullString  //密码
  NodeSource sql.NullInt64 //来源类型
  NodeType sql.NullInt64 //节点类型：1分组，2卡口，3电警 4视频接入 5交通乱点
  SearchIndex sql.NullString 
  NodeAttribute sql.NullString //预警参数，json字符串
  CarStyleType sql.NullInt64 //车头车尾,0车头,1车尾,2混合模式
  NodeLocation sql.NullString //卡口坐标
  LastDataTime sql.NullInt64  //上次数据时间
  SysDirectionCode sql.NullInt64 //系统方向(卡口列表方向码)
  PlatformDirection sql.NullString //平台方向
  RecParam sql.NullString  //识别内容信息
  DetectRect sql.NullString  //识别区域
  ComeOrOut sql.NullInt64  //出城入城， 0：普通  1：入城 2：出城
}

func ConnectDB(dbname string) (* sql.DB, error){
	username, err := C.GetValue("mysql", "username")
    if err != nil{
       log.Panicf(err.Error()) 
    }
	password, err := C.GetValue("mysql", "password")
    if err != nil{
       log.Panicf(err.Error()) 
    }
	server, err := C.GetValue("mysql", "server")
    if err != nil{
       log.Panicf(err.Error()) 
    }
	port, err := C.GetValue("mysql", "port")
     if err != nil{
       log.Panicf(err.Error()) 
    }

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",username,
                                        password,
                                        server,port,dbname)
	DB,err := sql.Open("mysql",dsn)
	if err != nil{
		return nil, err
	}
	DB.SetConnMaxLifetime(100*time.Second)  //最大连接周期，超过时间的连接就close
	DB.SetMaxOpenConns(100)//设置最大连接数
	DB.SetMaxIdleConns(16) //设置闲置连接数
    return DB, nil
}
func GetAllTollgates() (*set.Set, error){
    deviceIds := set.New(set.ThreadSafe)
    DB, err := ConnectDB("business") 
    if err != nil {
		log.Printf("connect mysql failed,err:%v", err)
        return nil, err
    }
    rows, err := DB.Query("select nodeId from ai_tollgate_device ")

	defer func() {
		if rows != nil {
			rows.Close() //可以关闭掉未scan连接一直占用
		}
	}()
	if err != nil {
		log.Printf("Query failed,err:%v", err)
		return nil, err
	}
	var info TollgateInfo
	for rows.Next() {
		err = rows.Scan(&info.NodeId) //不scan会导致连接不释放
		if err != nil {
			log.Printf("Scan failed,err:%v", err)
			continue
		}
		v,err := info.NodeId.Value()
		if err == nil{
			deviceIds.Add(int(v.(int64)))
            log.Printf("deviceId:%d", v)
		}
	}
    DB.Close()
    return deviceIds.(*set.Set), nil 
}
func GetOuterTollgates() (*set.Set, error){
    deviceIds := set.New(set.ThreadSafe)
    DB, err := ConnectDB("business") 
    if err != nil {
		log.Printf("connect mysql failed,err:%v", err)
        return nil, err
    }
    rows, err := DB.Query("select nodeId from ai_tollgate_device where comeOrOut=1")

	defer func() {
		if rows != nil {
			rows.Close() //可以关闭掉未scan连接一直占用
		}
	}()
	if err != nil {
		log.Printf("Query failed,err:%v", err)
		return nil, err
	}
	var info TollgateInfo
	for rows.Next() {
		err = rows.Scan(&info.NodeId) //不scan会导致连接不释放
		if err != nil {
			log.Printf("Scan failed,err:%v", err)
			continue
		}
		v,err := info.NodeId.Value()
		if err == nil{
			deviceIds.Add(int(v.(int64)))
            log.Printf("deviceId:%d", v)
		}
	}
    DB.Close()
    return deviceIds.(*set.Set), nil 
}

func FilterPlate(plateNumber string) bool{
   if strings.Contains(plateNumber, "-"){
        return true
   } 
   if strings.Contains(plateNumber, "无车牌"){
        return true
   } 
   if strings.Contains(plateNumber, "未识别"){
        return true
   } 
   if strings.Contains(plateNumber, "车牌过小"){
        return true
   } 
   return false
}
