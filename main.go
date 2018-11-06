package main

import (
	"database/sql"
	"encoding/json"
	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"
)

var (
	router *mux.Router
	db *sql.DB
	Conf = Config{}
)

type Config struct {
	Node string `json:Node`
}

type Address_history struct{
	History []Block_transaction_history
}

type Block_transaction_history struct {
	Txid string
	Type string `json:type`
	Value string
	CreateTime string
	Height int
}

const (
	BbName = "chain.db"
 	BlockHeight = "/api/v1/block/height"
 	BlockDetail = "/api/v1/block/details/height/"
 	TransactionDetail = "/api/v1/transaction/"
 	INCOME = "income"
 	SPEND = "spend"
 	ERROR_REQUEST = "Error Request :"
 	ELA = 100000000
)

func main(){
	go SyncChain()
	defer db.Close()
	http.ListenAndServe(":8080",router)
}


func SyncChain() {
	for {
		tx , err := db.Begin()
		if err = sync(tx) ; err != nil {
			log.Printf("Sync Height Error : %v \n" ,  err.Error())
			tx.Rollback()
		}else{
			tx.Commit()
		}
		<-time.After(time.Second * 30)
	}
}

func sync(tx *sql.Tx) error{

	resp , err := Get(Conf.Node + BlockHeight)

	if err != nil {
		return err
	}

	r , err := tx.Query("select height from block_CurrHeight order by id desc limit 1")
	if err != nil {
		return err
	}
	defer r.Close()
	storeHeight :=-1
	if r.Next() {
		r.Scan(&storeHeight)
	}

	chainHeight , ok := resp["Result"]

	if ok {
		if storeHeight == int(chainHeight.(float64)) {
			return nil
		}
		s , err := tx.Prepare("insert into block_CurrHeight (height) values(?)")
		if err != nil {
			return err
		}
		defer s.Close()
		_ , err = s.Exec(chainHeight)
		if err != nil {
			return err
		}
		for curr := storeHeight + 1 ; curr <= int(chainHeight.(float64)) ; curr++{
			resp , err = Get(Conf.Node + BlockDetail + strconv.FormatInt(int64(curr),10))
			txArr := (resp["Result"].(map[string]interface{}))["tx"].([]interface{})
			if len(txArr) == 0 {
				continue
			}
			for _ , v := range txArr {
				stmt , err := tx.Prepare("insert into block_transaction_history (address,txid,type,value,createTime,height) values(?,?,?,?,?,?)")
				if err != nil {
					return err
				}
				defer stmt.Close()
				vm := v.(map[string]interface{})
				txid := vm["txid"].(string)
				time := vm["blocktime"].(float64)
				t := vm["type"].(float64)
				_type := INCOME
				if t == 0 {
					vout :=  vm["vout"].([]interface{})
					for _ , vv := range vout{
						vvm := vv.(map[string]interface{})
						value := vvm["value"].(string)
						address := vvm["address"].(string)
						_ , err := stmt.Exec(address,txid,_type,value,strconv.FormatFloat(time,'f',0,64),curr)
						if err != nil {
							return err
						}
					}
				}else{
					vin :=  vm["vin"].([]interface{})
					spend := make(map[string]float64)
					for _ , vv := range vin{
						vvm := vv.(map[string]interface{})
						vintxid := vvm["txid"].(string)
						vinindex := vvm["vout"].(float64)
						txResp , err := Get(Conf.Node + TransactionDetail+vintxid)
						if err != nil {
							return err
						}
						vout := ((txResp["Result"].(map[string]interface{}))["vout"].([]interface{}))[int(vinindex)]
						voutm := vout.(map[string]interface{})
						address := voutm["address"].(string)
						value, err:= strconv.ParseFloat(voutm["value"].(string),64)
						if err != nil {
							return err
						}
						 v , ok := spend[address]
						 if ok {
							 spend[address] = v + value
						 }else {
							 spend[address] = value
						 }
					}
					vout :=  vm["vout"].([]interface{})
					receive := make(map[string]float64)
					for _ , vv := range vout{
						vvm := vv.(map[string]interface{})
						value, err:= strconv.ParseFloat( vvm["value"].(string),64)
						if err != nil {
							return err
						}
						address := vvm["address"].(string)
						v , ok := receive[address]
						if ok {
							receive[address] = v + value
						}else {
							receive[address] = value
						}
					}

					for k , r := range receive {
						s , ok := spend[k]
						var value float64
						if ok {
							if s > r {
								value = math.Round((s - r)*ELA)
								_type = SPEND
							}else {
								value = r - s
							}
							delete(spend,k)
						}else {
							value = r
						}
						_ , err := stmt.Exec(k,txid,_type,strconv.FormatFloat(value/ELA,'f',8,64),strconv.FormatFloat(time,'f',0,64),curr)
						if err != nil {
							return err
						}
					}

					for k , r := range spend {
						_type = SPEND
						_ , err := stmt.Exec(k,txid,_type,strconv.FormatFloat(r,'f',8,64),strconv.FormatFloat(time,'f',0,64),curr)
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}


func init(){
	router = mux.NewRouter()
	router.HandleFunc("/history/{addr}",history).Methods("GET")
	initDb()
	initConfig()
}


func history(w http.ResponseWriter, r *http.Request){
	params := mux.Vars(r)
	address := params["addr"]
	s , err := db.Prepare("select txid,type,value,createTime,height from block_transaction_history where address = ?")

	if err != nil{
		w.Write([]byte(`{"result":"`+ERROR_REQUEST + err.Error()+`",status:500}`))
		return
	}
	rst , err := s.Query(address)
	if err != nil{
		w.Write([]byte(`{"result":"`+ERROR_REQUEST + err.Error()+`",status:500}`))
		return
	}
	bhs := make([]Block_transaction_history,0)
	for rst.Next() {
		history :=Block_transaction_history{}
		err := rst.Scan(&history.Txid,&history.Type,&history.Value,&history.CreateTime,&history.Height)
		if err != nil {
			w.Write([]byte(`{"result":"`+ERROR_REQUEST + err.Error()+`",status:500}`))
			return
		}
		bhs = append(bhs,history)
	}
	addrHis := Address_history{bhs}
	buf , err := json.Marshal(&addrHis)
	if err != nil {
		w.Write([]byte(`{"result":"`+ERROR_REQUEST + err.Error()+`",status:500}`))
		return
	}
	w.Write([]byte(`{"result":` + string(buf)+`,"status":200}`))
}

func readBody(r *http.Request) (string){
	body , err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err.Error())
		return ""
	}
	return string(body)
}

func Get(url string) (map[string]interface{},error) {
	log.Printf("Request URL = %v \n", url)
	r , err := http.Get(url)
	if err != nil {
		return nil , err
	}
	resp , err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil , err
	}
	rstMap := make(map[string]interface{})
	json.Unmarshal(resp,&rstMap)
	return rstMap , nil
}


func initDb(){
	var err error
	db, err = sql.Open("sqlite3", BbName)
	if err != nil {
		log.Fatal(err)
	}
	initializeTable()
}

func initializeTable(){
	createTableSqlStmtArr := []string{
		`create table IF not exists block_currHeight (id integer not null primary key , height integer not null);`,
		`create table IF not exists block_transaction_history (id integer not null primary key , address varchar(34) not null ,
		txid varchar(64) not null ,type blob not null, value DECIMAL(18,8) not null, createTime blob not null , height integer not null);`,
	}

	for _ , v := range createTableSqlStmtArr {
		log.Printf("Execute sql :%v",v)
		_, err := db.Exec(v)
		if err != nil {
			log.Printf("Error execute sql : %q \n", err, v)
			return
		}
	}

}

func initConfig(){
	file, _ := os.Open("config.json")
	defer file.Close()
	decoder := json.NewDecoder(file)
	err := decoder.Decode(&Conf)
	if err != nil {
		log.Fatal("Error init Config :", err)
	}
}
