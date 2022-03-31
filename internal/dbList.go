package internal

import (
	"errors"
	"github.com/jmoiron/sqlx"
)
type listElement struct{
	db *sqlx.DB
	name string
	key string
}
//DBList by default have size of 10
type DBList struct {
	List []listElement
}
//NewDBList Creates new DBList instance
func NewDBList() *DBList{

	return &DBList{}
}
//AddDB Adds Database (postgres) to list
func(s *DBList) AddDB(url string,name string,key string) error{
	db,err := sqlx.Open("postgres",url)
	if err!=nil{
		return err
	}
	le:=listElement{
		db:  db,
		name: name,
		key: key,
	}
	s.List= append(s.List, le)
	return nil
}
//findDBPos returns first occurrence of DB with key and flag, if flag==true DB exists in list, else it returns pos=0 and flag=false
func(s *DBList) findDBPos(key string) (pos int, flag bool){
	for i:= range s.List{
		if key==s.List[i].key{
			pos=i
			flag=true
			break
		}
	}
	return pos,flag
}
func(s *DBList) GetDBFromList(key string) *sqlx.DB {
	for i:= range s.List{
		if key==s.List[i].key{
			return s.List[i].db
		}
	}
	return nil
}
//RemoveDB Removes database from list
func(s *DBList) RemoveDB(key string) (error,bool){
	if len(s.List)>0 {
		pos, flag := s.findDBPos(key)
		if flag {
			if err := s.List[pos].db.Close(); err != nil {
				return err, false
			}
		}
		if pos!=len(s.List)-1 {
			s.List = append(s.List[:pos], s.List[pos+1:]...)
		}else{
			s.List=s.List[:len(s.List)-1]
		}
		return nil,true
	}
	return errors.New("out of range, list size is 0"),false
}
