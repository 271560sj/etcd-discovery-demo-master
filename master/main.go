package main

import "./writer"
func main()  {
	//设置endpoint的信息
	endpoints := []string{"http://127.0.0.1:2379"}
	//初始化master service
	master := writer.InitMatser("master",endpoints)
	//开始监控worker的信息
	master.WatcherService()
}
