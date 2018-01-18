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
	BooksInfo map[string]*Books
	KeyAPIs etcd.KeysAPI
}
//定义接收的消息的结构体
type Books struct {
	IDs string
	KeyWord string
	Infos string
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
		BooksInfo: make(map[string]*Books),//记录数据信息
	}
	go master.WatchWorkers()//线程操作，进行监控需要的服务节点
	return master
}

//将接收到的数据存储到本地文件books.json中
func (m *Master)AddBookInfos(info *Books)  {
	book := &Books{
		IDs: info.IDs,
		KeyWord: info.KeyWord,
		Infos: info.Infos,
	}
	m.BooksInfo[info.IDs] = book
	file,err := os.OpenFile("./books.txt",os.O_RDWR|os.O_CREATE|os.O_APPEND,0766)
	if err != nil {
		log.Println("Save book infos error",err)
	}
	books,err := json.Marshal(book)
	if err != nil{
		log.Println("Change struct to json err",err)
	}else {
		file.WriteString(string(books))
	}
}
//更新数据信息
func (m * Master)UpdateBookInfos(info *Books)  {
	book := m.BooksInfo[info.IDs]
	book.Infos = info.Infos
	book.KeyWord = info.KeyWord

}
//获取工作节点的信息
func WorkNodeInfos(node *etcd.Node) *Books {
	log.Println(node.Value)
	infos := &Books{}
	err := json.Unmarshal([]byte(node.Value), infos)
	if err != nil {
		log.Println("Get node infos err",err)
	}
	return infos
}
//监控需要服务的key的信息，并接收数据
func (m *Master)WatchWorkers()  {
	log.Println("Waiting for books infos")
	//获取key API，用于操作key
	keyApis := m.KeyAPIs
	//监控key的信息
	watch := keyApis.Watcher("books/",&etcd.WatcherOptions{
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
			if _,ok := m.BooksInfo[infos.IDs]; ok {//更新key的value信息
				log.Println("Update books infos",infos.IDs)
				m.UpdateBookInfos(infos)
			}else {//添加key的value信息
				log.Println("Add book infos", infos.IDs)
				m.AddBookInfos(infos)
			}
		}else if  res.Action == "delete"{//删除key
			//删除操作
			infos := WorkNodeInfos(res.Node)
			log.Println("Delete book's infos",infos.IDs)
		}else if res.Action == "expire" {//服务worker进程终止
			//进程终止操作
			infos := WorkNodeInfos(res.Node)
			log.Println("Service has stopped",infos.IDs)
			break
		}else {//其他操作
			infos := WorkNodeInfos(res.Node)
			log.Println("Worker has done nothings",infos.IDs)
		}
	}
}
