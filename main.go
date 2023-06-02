package main

import (
	"encoding/json"
	"fmt"
)

type BlockStatus string

const (
	Committed BlockStatus = "committed"
	Pending   BlockStatus = "pending"
)

type Transaction struct {
	TxnId   string
	Value   []Entry
	IsValid bool
	Data    map[string]Entry
}

type Entry struct {
	Val   float64 `json:"val"`
	Ver   float64 `json:"ver"`
	Valid bool    `json:"valid"`
}

type Block interface {
	PushValidTxns(txns []Transaction)
	UpdateBlockStatus(status BlockStatus)
}

type MyBlock struct {
	BlockNumber   int
	PrevBlockHash string
	Status        BlockStatus
	Txns          map[string]Transaction
}

func (b *MyBlock) pushValidTransactions(initialState map[string]Entry, InputTxns []map[string]Entry) map[string]Entry {
	dbEntries := make(map[string]Entry)
	newdb := make(map[string]Entry)

	for k, v := range initialState {
		dbEntries[k] = v
	}

	for _, txn := range InputTxns {
		for key, value := range txn {
			if entry, ok := dbEntries[key]; ok {
				if entry.Ver == value.Ver {
					entry.Val = value.Val
					entry.Ver++
					entry.Valid = true
					newdb[key] = entry
					transaction := Transaction{
						Data: newdb,
					}
					b.Txns[transaction.TxnId] = transaction
				} else {
					entry.Valid = false
					newdb[key] = entry
				}
			}
			if _, ok := newdb[key]; !ok {
				value.Valid = false
				newdb[key] = value
			}
		}
	}
	return newdb
}

func (b *MyBlock) UpdateBlockStatus(status BlockStatus) {
	b.Status = status
}

func main() {
	block := MyBlock{
		BlockNumber:   1,
		Status:        Pending,
		PrevBlockHash: "0xabc123",
		Txns:          make(map[string]Transaction),
	}
	initialState := make(map[string]Entry)
	for i := 1; i <= 1000; i++ {
		key := fmt.Sprintf("SIM%d", i)
		initialState[key] = Entry{Val: float64(i), Ver: 1.0}
	}
	InputTxns := []map[string]Entry{
		{"SIM1": {Val: 2, Ver: 1.0}},
		{"SIM2": {Val: 3, Ver: 1.0}},
		{"SIM3": {Val: 4, Ver: 2.0}},
	}

	block.pushValidTransactions(initialState, InputTxns)
	block.UpdateBlockStatus(Committed)

	output := map[string]interface{}{
		"blockNumber":   block.BlockNumber,
		"prevBlockHash": block.PrevBlockHash,
		"txns":          []map[string]Entry{},
	}

	for _, txn := range block.Txns {
		data := make(map[string]Entry)
		for key, value := range txn.Data {
			data[key] = value
		}
		output["txns"] = append(output["txns"].([]map[string]Entry), data)
	}

	outputJSON, _ := json.Marshal(output)
	fmt.Println(string(outputJSON))
}
