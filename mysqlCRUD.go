/**
*  reference http://www.cnblogs.com/jackylee92/p/6209596.html
*/
package main

import ("fmt"
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
	"time"
)
//reflect in database table of user
type User struct {
	Id       int
	UserName string
	Url      string
	Age      int
}

func addUser(user User,db *sql.DB) int{
	insert_sql:="insert into user(UserName,Url,Age) value (?,?,?)"
	stmt,err:=db.Prepare(insert_sql)
	if err!=nil {
		fmt.Println(err)
		return 0
	}
	res,err:=stmt.Exec(user.UserName,user.Url,user.Age)
	if err!=nil {
		fmt.Println(err)
		return 0
	}
	lastInsertId,err:=res.LastInsertId()
	if err !=nil{
		fmt.Println(err)
		return 0
	}
	return int(lastInsertId)
}
//if rollbackInArray = 0 means no rollback
func addUserInBatch(users []User,db *sql.DB,rollbackInArray int) int{
	insert_sql:="insert into user(UserName,Url,Age) value (?,?,?)"
	tx,err:=db.Begin()
	if err != nil {
		fmt.Println(err)
		return 0
	}
	stmt,err:=tx.Prepare(insert_sql)
	if err != nil {
		fmt.Println(err)
		return 0
	}
	var i int
	var res sql.Result
	for _,user :=range users{
		res,_=stmt.Exec(&user.UserName,&user.Url,&user.Age)
		i++
		if i==rollbackInArray {
			tx.Rollback()
			return 0
		}
	}
	lastId,_:=res.LastInsertId();
	tx.Commit()
	return int(lastId)
}

func findUserById(id int,db *sql.DB)(User){
	var user User
	select_sql:="select * from user where Id =?"
	err:=db.QueryRow(select_sql,id).Scan(&user.Id,&user.UserName,&user.Url,&user.Age)
	if err!=nil {
		fmt.Printf("findUserById err %v\n",err)
		//return empty user
		return user
	}
	return user
}


//return slice object with User
func findAllUser(db *sql.DB)[]User {
	var user User
	var users []User
	select_rows:="select * from user"
	rows,err:=db.Query(select_rows);
	if err != nil {
		fmt.Println(err)
		return nil
	}
	for rows.Next(){
		if err := rows.Scan(&user.Id, &user.UserName, &user.Url, &user.Age);err!=nil {
			fmt.Println(err)
			return nil
		}
		users=append(users,user)
	}
	return users
}

func updateUser(user User,db *sql.DB)  {
	update_sql:="update user set UserName=?,Url=? where Id =?"
	ustmt,err:=db.Prepare(update_sql)
	if err!=nil {
		fmt.Println(err)
		return
	}
	_, err = ustmt.Exec(user.UserName,user.Url,user.Id)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func testAddUser(db *sql.DB) {
	var user  =User{0,"Cock","http://blog.golang.org/cock",22}
	lastUserId:= addUser(user,db);
	fmt.Printf("addUser lastUserId= %d\n",lastUserId)
}

func testAddUserInBatch(db *sql.DB)  {
	//first add user no rollback
	var users=[]User{{0,"Cock","http://blog.golang.org/cock",22},{0,"Hickinbottom","http://blog.golang.org/hickinbottom",21},{0,"Willy","http://blog.golang.org/willy",22},{0,"Nutter","http://blog.golang.org/nutter",25},{0,"Pigg","http://blog.golang.org/Pigg",33}}
	lastId := addUserInBatch(users,db,0)
	fmt.Printf("addUserInPatch rollInArray=0 totalAddUserSize=%d lastId=%d\n",len(users), lastId)
	//second add user use rollback
	users=nil
	users=append(users,User{0,"Jelly","http://blog.golang.org/jelly",35})
	users=append(users,User{0,"Stranger","http://blog.golang.org/stranger",18})
	users=append(users,User{0,"Grave","http://blog.golang.org/grave",27})
	lastId = addUserInBatch(users,db,2)
	fmt.Printf("addUserInPatch rollInArray=2 totalAddUserSize=%d lastId=%d\n",len(users), lastId)
}

func testUpdateUser(db *sql.DB) {
	var user=findUserById(1,db)
	if (User{}==user) {
		fmt.Printf("not find user where Id=1\n")
		return
	}

	fmt.Printf("before updateUser call findUserById:%v\n",user)
	timenow:=time.Now().Format("20060102150405")
	user.UserName ="Crankshaw@"+timenow
	user.Url ="http://blog.golang.org/crankshaw@"+timenow
	updateUser(user,db);
	user=findUserById(1,db)
	fmt.Printf("after updateUser:%v\n",user)
}

func testFindAllUser(db *sql.DB) {
	users:=findAllUser(db)
	//fmt.Printf("%q\n",users)
	fmt.Printf("findAllUser usersLen=%d\n",len(users))
	for _,user:= range users {
		fmt.Printf("%v\n",user)
	}

}

func main() {
	//sql.Open("mysql","yourName:yourPassword@tcp(yourDatabaseIpAddress:3306)/yourDatabaseName?charset=utf8")
	db,err:=sql.Open("mysql","root:123123@tcp(localhost:3306)/test?charset=utf8")
	if err !=nil{
		panic(err.Error())
		fmt.Println(err)
		return
	}
	defer db.Close()
	testAddUser(db)
	testAddUserInBatch(db)
	testUpdateUser(db)
	testFindAllUser(db)
}
