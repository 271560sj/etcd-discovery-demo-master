package writer

import (
	etcd "github.com/coreos/etcd/client"
	"time"
	"log"
	"context"
	"encoding/json"
	"os"
)
//定义客户端的结构体
type Master struct {
	Name string
	KeyAPIs etcd.KeysAPI
}
//定义请求服务的信息
type SeriveInfo struct {
	IP string
	CPU int
	HostName string
	ServiceName string
	ServiceIP string
	ServicePort string
}

//初始化master
func InitMatser(name string,endpoints []string)  *Master{
	cfg := etcd.Config{
		//设置连接etcd的参数，Config是etcd V2提供的参数;
		// Endpoints是一个字符串数组，表示节点的信息，一般格式为[]string{"http://ip:port"}
		//Transport是传输协议，如果不设置，默认是DefaultTransport
		//HeaderTimeoutPerRequest设置每次请求的超时时间
		Endpoints: endpoints,
		Transport: etcd.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}
	//连接etcd节点
	client,err := etcd.New(cfg)
	if err != nil {
		log.Fatal("Error connect to etcd server",err)
	}
	master := &Master{//设置master的信息，相当于一个service a
		Name: name,//服务名称
		KeyAPIs: etcd.NewKeysAPI(client),//获取key API ,用于操作key,进行增删改操作
	}
	go master.WatcherService()//线程操作，进行监控需要的服务节点
	return master
}

//获取工作节点的信息
func WorkNodeInfos(node *etcd.Node) *SeriveInfo {
	infos := &SeriveInfo{}
	err := json.Unmarshal([]byte(node.Value), infos)
	if err != nil {
		log.Println("Get node infos err",err)
	}
	return infos
}
//监控需要服务的key的信息，并接收数据
func (m *Master)WatcherService()  {
	log.Println("Waiting for service infos")
	//获取key API，用于操作key
	keyApis := m.KeyAPIs
	//监控key的信息
	watch := keyApis.Watcher("service-info",&etcd.WatcherOptions{
		Recursive: true,//如果是目录，进行递归操作，查询目录内部的信息
	})
	for {//循环操作
		res,err := watch.Next(context.Background())//获取key的信息
		if err != nil {//判断获取信息失败
			log.Println("Error receiver workers:",err)
			break
		}
		if res.Action == "set" {//如果是设置key的信息
			infos := WorkNodeInfos(res.Node)
			log.Println("Service info has registried",infos.ServiceName)
		}else if  res.Action == "delete"{//删除key
			//删除操作
			infos := WorkNodeInfos(res.Node)
			log.Println("Service info has been deleted",infos.ServiceName)
		}else if res.Action == "expire" {//服务worker进程终止
			//进程终止操作
			infos := WorkNodeInfos(res.Node)
			log.Println("Service has stopped",infos.ServiceName)
		}else if res.Action == "update" {
			infos := WorkNodeInfos(res.Node)
			log.Println("Service info has been update",infos.ServiceName)
		}else {//其他操作
			infos := WorkNodeInfos(res.Node)
			log.Println("Worker has done nothings",infos.ServiceName)
		}
	}
}
