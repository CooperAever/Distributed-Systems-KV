package main

import (
	"fmt"
	// "strings"
	// "encoding/json"
	// "os"
	// "strconv"
	// "hash/fnv"
	// "sort"
	// "io/ioutil"
	// "unicode"
	// "bytes"
	"sync"
)
// type KeyValue struct {
// 	Key   string
// 	Value string
// }


// type byKey []KeyValue
// func (kv byKey) Len()int{
// 	return len(kv)
// }

// func (kv byKey) Swap(i,j int){
// 	kv[i],kv[j] = kv[j],kv[i]
// }

// func (kv byKey) Less(i,j int) bool{
// 	return kv[i].Key < kv[j].Key
// }

// func main(){
// 	// var res []KeyValue
// 	// value := "string apple banana pine peach leech gem apple string strung"
// 	// words := strings.Fields(value)
// 	// for _, w := range words {
// 	// 	kv := KeyValue{w, ""}
// 	// 	res = append(res, kv)
// 	// }

// 	// sort.Sort(byKey(res))
// 	// fmt.Println(res)

//  //   	file, _ := os.OpenFile("json.txt" , os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
//  //   	enc := json.NewEncoder(file)
//  //   	for _,value:= range res{
//  //   		enc.Encode(KeyValue{value.Key,value.Value})
//  //   	}

// 	// file.Close()


// 	fp, _ := os.Open("824-mrinput-0.txt")
// 	dec := json.NewDecoder(fp)
// 	for {
//     	var V KeyValue
//     	err := dec.Decode(&V)
//     	if err != nil {
//         	break
//     	}
//     	fmt.Println(V)
// 	}

// 	// strs  := []string {"111","2","3","3","4","5","7","111","2"}
//  // 	for _,str := range strs{
//  // 		fmt.Printf("%s 's hash is %d \n",str,ihash(str)%3)
//  // 	}


// 	// for _,value := range res{
//  //    	r := ihash(value.Key)%3
//  //    	file_name := reduceName("jobName",111,r)
//  //    	file,_ := os.OpenFile(file_name,os.O_WRONLY|os.O_APPEND|os.O_CREATE,0666)
//  //    	enc := json.NewEncoder(file)
//  //    	enc.Encode(KeyValue{value.Key,value.Value})
//  //    	file.Close()
//  //    }


// }

// func ihash(s string) int {
// 	h := fnv.New32a()
// 	h.Write([]byte(s))
// 	return int(h.Sum32() & 0x7fffffff)
// }


// func reduceName(jobName string, mapTask int, reduceTask int) string {
// 	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
// }




// func main() {
//    Ioutil("824-mrinput-0.txt")
//     }

// func Ioutil(name string) {
//     if contents,err := ioutil.ReadFile(name);err == nil {
//         //因为contents是[]byte类型，直接转换成string类型后会多一行空格,需要使用strings.Replace替换换行符
//         result := strings.Replace(string(contents),"\n","",1)
//         res := ihash(result)%3
//         fmt.Println(res)
//         }
// }




func main() {
    // data, err := getFileContent("pg-metamorphosis.txt")

    // if err != nil {
    //     fmt.Println("File reading error", err)
    //     return
    // }
    // fmt.Println(data[0])
    // fmt.Println(data[len(data)-1])

    // var v KeyValue
    // v.Key = data[0]
    // v.Value = data[len(data)-1]
    // fmt.Println(KeyValue{data[0],data[len(data)-1]})
    // num,_ := strconv.Atoi("123")
    // str := strconv.Itoa(123)
    // fmt.Println(num)
    // fmt.Println(str)

    wg := sync.WaitGroup{}
    out := make(chan string)
    for i:= 0;i<10;i++{
    	wg.Add(1)
    	go f1(out,&wg)
    }

    for i:= 0;i<10;i++{
    	out<-"123"
    }
    wg.Wait()
}

func f1(in chan string,wg *sync.WaitGroup) {
	str := <- in
    wg.Done()
    fmt.Printf("print %v at wg : %v\n",str,wg)
}


// func getFileContent(filePath string) ([]string,error){

// 	result := []string {}
// 	b,err := ioutil.ReadFile(filePath)
// 	if err != nil{
// 		return result,err
// 	}

// 	s := string(b)

// 	var buffer bytes.Buffer
// 	for _,char := range s{
// 		if(unicode.IsLetter(char)){
// 			buffer.WriteString(string(char))
// 		}else{
// 			if (buffer.String() != ""){
// 				result = append(result,buffer.String())
// 				buffer.Reset()
// 			}
// 		}
// 	}
// 	if(buffer.String() != ""){
// 		result = append(result,buffer.String())
// 	}
	


// 	return result,nil
// }




