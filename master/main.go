package main

import "./writer"
func main()  {
	endpoints := []string{"http://127.0.0.1:2379"}
	master := writer.InitMatser("master",endpoints)
	master.WatchWorkers()
}
