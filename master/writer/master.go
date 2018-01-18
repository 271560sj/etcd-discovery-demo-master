package writer

import (
	etcd "github.com/coreos/etcd/client"
	"time"
	"log"
	"context"
	"encoding/json"
	"os"
)
type Master struct {
	Name string
	BooksInfo map[string]*Books
	KeyAPIs etcd.KeysAPI
}
type Books struct {
	IDs string
	KeyWord string
	Infos string
}

//初始化master
func InitMatser(name string,endpoints []string)  *Master{
	cfg := etcd.Config{
		Endpoints: endpoints,
		Transport: etcd.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}
	client,err := etcd.New(cfg)
	if err != nil {
		log.Fatal("Error connect to etcd server",err)
	}
	master := &Master{
		Name: name,
		KeyAPIs: etcd.NewKeysAPI(client),
		BooksInfo: make(map[string]*Books),
	}
	go master.WatchWorkers()
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
//接收数据
func (m *Master)WatchWorkers()  {
	log.Println("Waiting for books infos")
	keyApis := m.KeyAPIs
	watch := keyApis.Watcher("books/",&etcd.WatcherOptions{
		Recursive: true,
	})
	for {
		res,err := watch.Next(context.Background())
		if err != nil {
			log.Println("Error receiver workers:",err)
			break
		}
		if res.Action == "set" {
			infos := WorkNodeInfos(res.Node)
			if _,ok := m.BooksInfo[infos.IDs]; ok {
				log.Println("Update books infos",infos.IDs)
				m.UpdateBookInfos(infos)
			}else {
				log.Println("Add book infos", infos.IDs)
				m.AddBookInfos(infos)
			}
		}else if  res.Action == "delete"{
			//删除操作
			infos := WorkNodeInfos(res.Node)
			log.Println("Delete book's infos",infos.IDs)
		}else if res.Action == "expire" {
			//进程终止操作
			infos := WorkNodeInfos(res.Node)
			log.Println("Service has stopped",infos.IDs)
		}else {
			infos := WorkNodeInfos(res.Node)
			log.Println("Worker has done nothings",infos.IDs)
		}
	}
}
