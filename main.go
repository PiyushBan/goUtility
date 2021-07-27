package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type result struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default string
	Extra   string
}

func main() {
	dbUrl := "root:Byjus@2020@tcp(35.154.206.60:3306)/byjus_sf_archival"

	db, err := sql.Open("mysql", dbUrl)

	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	fmt.Println("Database connected Sucessfully")
	sqlQuery := "INSERT into @ (:headerList)" +
		"values" +
		":valueList"
	sqlQuery = strings.Replace(sqlQuery, "@", "Case__c", 1)

	//header logic
	headerSql := "Desc @"
	headerSql = strings.Replace(headerSql, "@", "Case__c", 1)

	rows, err := db.Query(headerSql)

	if err != nil {
		fmt.Println(err.Error())
	}

	var header strings.Builder
	var fields []string = make([]string, 0)
	for rows.Next() {
		var temp result
		rows.Scan(&temp.Field, &temp.Type, &temp.Null, &temp.Key, &temp.Default, &temp.Extra)
		fields = append(fields, temp.Field)
	}
	for index, val := range fields {
		if index != 0 {
			header.WriteString(",")
		}
		header.WriteString(val)
	}
	sqlQuery = strings.Replace(sqlQuery, ":headerList", header.String(), 1)

	// fmt.Println(sqlQuery)

	//reading csv in batches of 100 and inserting into DB;
	filePath := "/Users/piyushbansal/Desktop/tempFile.csv"
	csv_file, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err.Error())
	}
	reader := csv.NewReader(csv_file)
	curr := 0
	var batchSize int = 100
	i := 0
	var valuelist strings.Builder
	for {
		record, err := reader.Read()
		if err == io.EOF {
			fmt.Println("File finished")
			break
		}

		curr++
		if valuelist.Len() != 0 {
			valuelist.WriteString(",")
		}
		valuelist.WriteString("(")
		for idx, value := range record {

			if idx != 0 {
				valuelist.WriteString(",")
			}
			valuelist.WriteString("'")
			valuelist.WriteString(value)
			valuelist.WriteString("'")

		}
		valuelist.WriteString(")")
		if curr == batchSize {
			valuelist.WriteString(";")
			var tmpSQL string = strings.ReplaceAll(sqlQuery, ":valueList", valuelist.String())
			_, err = db.Exec(tmpSQL)
			if err != nil {
				fmt.Println(err.Error())
			} else {
				fmt.Println("batch %s inserted. ", i)
				i++
			}
			valuelist = strings.Builder{}
			db.Close()
			db, _ = sql.Open("mysql", dbUrl)
			curr = 0

		}
	}
	if curr != 0 {
		valuelist.WriteString(";")

		var tmpSQL string = strings.ReplaceAll(sqlQuery, ":valueList", valuelist.String())
		fmt.Println(tmpSQL)
		_, err = db.Exec(tmpSQL)
		if err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Printf("batch %s inserted. ", i)
			i++
		}
		valuelist = strings.Builder{}
		db.Close()
		curr = 0

	}

}
