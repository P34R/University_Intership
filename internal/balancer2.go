package internal

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)


type DbBalancer struct {
	dbList *DBList
}
func NewDbBalancer() *DbBalancer{
	return &DbBalancer{
		dbList: NewDBList(),
	}
}
func (s *DbBalancer) List() *DBList{
	if s.dbList!=nil{
		return s.dbList
	}
	return NewDBList()
}
func (s *DbBalancer) SetList(list *DBList) bool{
	if s.dbList==nil{
		s.dbList=list
		return true
	}
	return false
}
func getDBSize(db *sqlx.DB, dbname string) int64 {
	var ret int64
	if err:=db.QueryRow("SELECT pg_database_size($1)", dbname).Scan(&ret); err!=nil{
		panic(err)
	}
	return ret
}
func getDBConnections(db *sqlx.DB, dbname string) int64{
	var count int64
	if err:=db.QueryRow("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=$1",dbname).Scan(&count);err!=nil{
		panic(err)
	}
	return count
}

/*
ну или это
SELECT * FROM pg_stat_activity
SELECT COUNT(*) FROM pg_stat_activity WHERE datname='$db'
SELECT pg_size_pretty( pg_database_size('dbname') );
SELECT pg_database_size('projectdb') returns bigint /1024 =


*/
func AvgI64(sl []int64) int64{
	if len(sl)>1 {
		var a, b int64
		for i := range sl {
			a+=sl[i]
			b+=1
		}
		return a/b
	}
	return -1
}


//BalancedConnector returns ...
func (s *DbBalancer) BalancedConnector() *sqlx.DB {
	if len(s.dbList.List)==0{
		return nil
	}
	if len(s.dbList.List)==1{
		return s.dbList.List[0].db
	}
	var sizes []int64
	var connections []int64
	sizes=make([]int64, len(s.dbList.List))
	connections=make([]int64,len(s.dbList.List))
	for i:=range sizes{
		sizes[i]=getDBSize(s.dbList.List[i].db,s.dbList.List[i].name)
		connections[i]=getDBConnections(s.dbList.List[i].db,s.dbList.List[i].name)
	}
	avgSize := AvgI64(sizes)
	avgConn := AvgI64(connections)
	minimum:=0
	k:=0
	for i:=range s.dbList.List{
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
	fmt.Println(s.dbList.List[minimum].key, "type  ",k)
	return s.dbList.List[minimum].db
}

func (s *DbBalancer) GetConnector(key string) *sqlx.DB{
	if len(s.dbList.List)==0{
		return nil
	}
	for i:=range s.dbList.List{
		if s.dbList.List[i].key==key{
			return s.dbList.List[i].db
		}
	}
	return nil
}

func (s *DbBalancer) BalancedExecute(command string,args...interface{})(sql.Result, error){
	res,err:=s.BalancedConnector().Exec(command,args)
		return res,err
}
func (s *DbBalancer) Execute(key string, command string, args...interface{})(sql.Result,error){
	res,err:=s.GetConnector(key).Exec(command,args)
	return res,err
}