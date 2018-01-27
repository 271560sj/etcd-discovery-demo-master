package writer

import (
	etcd "github.com/coreos/etcd/client"
	"time"
	"log"
	"context"
	"strconv"
	"encoding/json"
)
//定义客户端的结构体
type Master struct {
	Name string
	KeyAPIs etcd.KeysAPI
}
//定义接收的消息的结构体
type Worker struct {
	IDs string
	KeyWord string
	Infos string
}

type MasterInfo struct {
	IP string
	Port string
	Name string
}
const (
	masterKey = "masterService"
	watcherKey = "workerService"
	ip = "192.168.133.130"
	port = "8056"
	name = "master service"
)

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
	go master.MasterService()//线程操作，进行监控需要的服务节点
	return master
}

func (m *Master)MasterService()  {
	key := masterKey

	registryInfo := &MasterInfo{
		IP: ip,
		Port: port,
		Name: name,
	}

	api := m.KeyAPIs

	i := 0
	for  {
		go registryService(api,key,registryInfo)
		go deleteService(api,key)
		go updateService(api,key)
		go watchWorkers(m)
		i ++
		if i > 30 {
			break
		}
		time.Sleep(time.Second * 3)

	}
}

//注册信息
func registryService(api etcd.KeysAPI,key string,info *MasterInfo)  {

	value, err := json.Marshal(info)
	if err != nil {
		log.Fatal("Registry master service error")
	}else {
		response, _ := api.Set(context.Background(),key,string(value),nil)
		dealWithData(response)
	}

}

//删除注册信息
func deleteService(api etcd.KeysAPI,key string)  {
	response, err := api.Delete(context.Background(),key,nil)

	if err != nil {
		log.Println("Delete master service error")
	}else {
		dealWithData(response)
	}
}

//更新注册信息
func updateService(api etcd.KeysAPI,key string)  {
	update := &MasterInfo{
		IP: ip + ",update",
		Port:port + ",update",
		Name: name + ",update",
	}
	value,_ := json.Marshal(update)
	response, err := api.Update(context.Background(),key,string(value))
	if err != nil {
		log.Println("Update service error")
	}else {
		dealWithData(response)
	}
}
//监控需要服务的key的信息，并接收数据
func watchWorkers(m *Master)  {
	log.Println("Waiting for worker service infos")
	//获取key API，用于操作key
	keyApis := m.KeyAPIs
	//监控key的信息
	watch := keyApis.Watcher(watcherKey,&etcd.WatcherOptions{
		Recursive: true,//如果是目录，进行递归操作，查询目录内部的信息
	})
	for {//循环操作
		res,err := watch.Next(context.Background())//获取key的信息
		if err != nil {//判断获取信息失败
			log.Println("Error receiver workers:",err)
			continue
		}
		dealWithData(res)
	}
}

//进行数据处理
func dealWithData(response *etcd.Response)  {
	action := response.Action
	node := response.Node
	key := node.Key
	value := node.Value
	ttl := node.TTL

	log.Println(action + "," + key + "," + value + "," + strconv.FormatInt(ttl,10))
}
