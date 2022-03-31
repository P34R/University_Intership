package main

import (
	"KNU_Practice/internal"
	"fmt"
	"math/rand"
	"time"
)

func pseudo_db_tests(){
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	r2:=rand.New(s1)
	var connections []int64
	var sizes []int64
	for i:=0;i<5;i++{
		connections=append(connections,	int64(r1.Intn(200)))
		sizes=append(sizes,int64(r2.Intn(10737418240))) // 10gb
	}
	avgSize := internal.AvgI64(sizes)
	avgConn := internal.AvgI64(connections)
	minimum:=0
	k:=0
	for i:=0;i<5;i++{
		if i!=minimum{
			if connections[i]<=connections[minimum] {
				if sizes[i]<=sizes[minimum]{
					k=1
					minimum=i
				}else if float64(sizes[i])-float64(sizes[minimum])<=0.1*float64(avgSize) {
					if float64(connections[minimum])-float64(connections[i])>=0.1*float64(avgConn){
						k=2
						minimum=i
					}
				}
			}else if float64(connections[i])-float64(connections[minimum])<=0.1*float64(avgConn){
				if sizes[i]<=sizes[minimum]{
					k=3
					minimum=i
				}
			}
		}
	}
	fmt.Println("conns",connections,"avg = ",avgConn)
	fmt.Print("sizes [")
	for i:= range sizes{
		fmt.Printf("%.3f",float64(sizes[i])/(1024.0*1024.0*1024.0))
		if i!= len(sizes)-1{
			fmt.Print(" ")
		}
	}
	fmt.Print("] ")
	fmt.Println("avg = ",float64(avgSize)/(1024.0*1024.0*1024.0))
	fmt.Println("choosen ",minimum, "type ", k)

}
func main() {
	for i:=0;i<2;i++{
		pseudo_db_tests()
		time.Sleep(1000*time.Millisecond)
	}
}

/*
	url1:="host=localhost dbname=practice1 port=5432 user=postgres password=admin sslmode=disable"
	key1:="db_practice1"
	url2:="host=localhost dbname=practice2 port=5432 user=postgres password=admin sslmode=disable"
	key2:="db_practice2"
	url3:="host=localhost dbname=practice3 port=5432 user=postgres password=admin sslmode=disable"
	key3:="db_practice3"
	url4:="host=localhost dbname=practice4 port=5432 user=postgres password=admin sslmode=disable"
	key4:="db_practice4"
	s:= internal.NewDbBalancer()
	if err:=s.List().AddDB(url1,"practice1", key1);err!=nil{
		panic(err)
	}
	if err:=s.List().AddDB(url2,"practice2", key2);err!=nil{
		panic(err)
	}
	if err:=s.List().AddDB(url3,"practice3", key3);err!=nil{
		panic(err)
	}
	if err:=s.List().AddDB(url4,"practice4", key4);err!=nil{
		panic(err)
	}
	for i:=0; i<len(s.List().List);i++{
		fmt.Println(s.List().List[i])
	}
	for i:=500;i!=0;i--{
		u:= strconv.Itoa(i)+ "user" + "1b"
		p:= "pass" + strconv.Itoa(i)
		sql:="insert into \"users\" (\"username\", \"password\") values($1,$2)"
		if _,err:=s.BalancedConnector().Exec(sql,u,p);err!=nil{
			panic(err)
		}
	}
*/