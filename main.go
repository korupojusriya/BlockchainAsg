package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"sync"
	"time"
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
	ProcessingTime time.Time
	Txns          map[string]Transaction
}

func (b *MyBlock) pushValidTransactions(initialState map[string]Entry, InputTxns []map[string]Entry, wg *sync.WaitGroup, txnChan chan Transaction) {
	defer wg.Done()

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

	transaction := Transaction{
		Data:    newdb,
		IsValid: true,
	}

	txnChan <- transaction
}

func (b *MyBlock) UpdateBlockStatus(status BlockStatus) {
	b.Status = status
}

type BlockWriter chan MyBlock

func (bw BlockWriter) WriteBlock(block MyBlock) {
	bw <- block
}
func writeBlocksToFile(blocks []MyBlock) error {
	fileName := "blocks.json"

	blockJSON, err := json.Marshal(blocks)
	if err != nil {
		return fmt.Errorf("error marshaling blocks to JSON: %v", err)
	}

	err = ioutil.WriteFile(fileName, blockJSON, 0644)
	if err != nil {
		return fmt.Errorf("error writing blocks to file: %v", err)
	}

	return nil
}

func fetchBlockDetailsByNumber(blockNumber int) (MyBlock, error) {
	fileName := "blocks.json"

	fileContent, err := ioutil.ReadFile(fileName)
	if err != nil {
		return MyBlock{}, err
	}

	var blocks []MyBlock
	err = json.Unmarshal(fileContent, &blocks)
	if err != nil {
		return MyBlock{}, err
	}

	for _, block := range blocks {
		if block.BlockNumber == blockNumber {
			return block, nil
		}
	}

	return MyBlock{}, fmt.Errorf("block with block number %d not found", blockNumber)
}

func fetchAllBlockDetails() ([]MyBlock, error) {
	fileName := "blocks.json"

	fileContent, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	var blocks []MyBlock
	err = json.Unmarshal(fileContent, &blocks)
	if err != nil {
		return nil, err
	}

	return blocks, nil
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

	transactionsPerBlock := 10

	var wg sync.WaitGroup
	txnChan := make(chan Transaction)
	var blocks []MyBlock

	// ...

	for i := 0; i < len(InputTxns); i += transactionsPerBlock {
		wg.Add(1)
		block.ProcessingTime = time.Now()
		go block.pushValidTransactions(initialState, InputTxns[i:int(math.Min(float64(i+transactionsPerBlock), float64(len(InputTxns))))], &wg, txnChan)
	}

	go func() {
		wg.Wait()
		close(txnChan)
	}()

	for txn := range txnChan {
		block.Txns[txn.TxnId] = txn
	}

	block.UpdateBlockStatus(Committed)

	// ...

	blocks = append(blocks, block)

	err := writeBlocksToFile(blocks)
	if err != nil {
		fmt.Println("Error writing blocks to file:", err)
		return
	}
	output := map[string]interface{}{
		"blockNumber":   block.BlockNumber,
		"prevBlockHash": block.PrevBlockHash,
		"status":        block.Status,
		"processingTime": block.ProcessingTime.String(),
		"txns":          []map[string]Entry{},
	}

	for _, txn := range block.Txns {
		data := make(map[string]Entry)
		for key, value := range txn.Data {
			data[key] = value
		}
		output["txns"] = append(output["txns"].([]map[string]Entry), data)
	}

	outputJSON, _ := json.MarshalIndent(output, "", "  ")
	fmt.Println(string(outputJSON))

	// Fetch block details by block number
	blockNumber := 1
	fetchedBlock, err := fetchBlockDetailsByNumber(blockNumber)
	if err != nil {
		fmt.Printf("Error fetching block details for block number %d: %v\n", blockNumber, err)
	} else {
		fmt.Println("Fetched Block Details:")
		fmt.Printf("Block Number: %d\n", fetchedBlock.BlockNumber)
		fmt.Printf("Prev Block Hash: %s\n", fetchedBlock.PrevBlockHash)
		fmt.Printf("Status: %s\n", fetchedBlock.Status)
		fmt.Printf("Processing Time: %s\n", fetchedBlock.ProcessingTime)
		fmt.Printf("Transactions:\n")
		for txnID, txn := range fetchedBlock.Txns {
			fmt.Printf("  TxnID: %s\n", txnID)
			fmt.Printf("  IsValid: %t\n", txn.IsValid)
			fmt.Printf("  Data:\n")
			for key, entry := range txn.Data {
				fmt.Printf("    Key: %s\n", key)
				fmt.Printf("    Value: %v\n", entry)
			}
		}
	}

	// Fetch details of all blocks
	allBlocks, err := fetchAllBlockDetails()
	if err != nil {
		fmt.Println("Error fetching all block details:", err)
	} else {
		fmt.Println("All Block Details:")
		for _, block := range allBlocks {
			fmt.Printf("Block Number: %d\n", block.BlockNumber)
			fmt.Printf("Prev Block Hash: %s\n", block.PrevBlockHash)
			fmt.Printf("Status: %s\n", block.Status)
			fmt.Printf("Processing Time: %s\n", block.ProcessingTime)
			fmt.Println("Transactions:")
			for txnID, txn := range block.Txns {
				fmt.Printf("  TxnID: %s\n", txnID)
				fmt.Printf("  IsValid: %t\n", txn.IsValid)
				fmt.Println("  Data:")
				for key, entry := range txn.Data {
					fmt.Printf("    Key: %s\n", key)
					fmt.Printf("    Value: %v\n", entry)
				}
			}
			fmt.Println("------------------------")
		}
	}
}
