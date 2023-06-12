package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"
    "strconv"
	"github.com/syndtr/goleveldb/leveldb"
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
	Hash  string  `json:"hash"`
}

type Block interface {
	PushValidTxns(txns []Transaction)
	UpdateBlockStatus(status BlockStatus)
	GetPrevBlockHash() string
}

type MyBlock struct {
	BlockNumber     int
	PrevBlockHash   string
	Status          BlockStatus
	ProcessingTime  time.Duration
	Txns            map[string]Transaction
}

func (b *MyBlock) GetPrevBlockHash() string {
	return b.PrevBlockHash
}

func (b *MyBlock) pushValidTransactions(db *leveldb.DB, InputTxns []map[string]Entry, txnChan chan Transaction, wg *sync.WaitGroup) {
	defer wg.Done()

	newdb := make(map[string]Entry)
	for _, txn := range InputTxns {
		for key, value := range txn {
			hashInput := fmt.Sprintf("%s:%s", b.GetPrevBlockHash(), key)
			hash := sha256.Sum256([]byte(hashInput))

			data, err := db.Get([]byte(key), nil)
			if err == nil {
				var entry Entry
				err = json.Unmarshal(data, &entry)
				if err == nil {
					if entry.Ver == value.Ver {
						entry.Val = value.Val
						entry.Ver++
						entry.Valid = true
					} else {
						entry.Valid = false
					}
					entry.Hash = fmt.Sprintf("%x", hash)
					newdb[key] = entry
				}
			}

			if _, ok := newdb[key]; !ok {
				value.Valid = false
				value.Hash = fmt.Sprintf("%x", hash)
				newdb[key] = value
			}
		}
	}

	transaction := Transaction{
		TxnId:   fmt.Sprintf("%x", sha256.Sum256([]byte(b.PrevBlockHash))),
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
	rand.Seed(time.Now().UnixNano())
	startTime := time.Now()
	db, err := leveldb.OpenFile("leveldb", nil)
	if err != nil {
		fmt.Println("Error opening LevelDB:", err)
		return
	}
	defer db.Close()

	args := os.Args[1:]
	if len(args) < 2 {
		fmt.Println("Please provide values for TRANSACTIONS_PER_BLOCK and TOTAL_TRANSACTIONS.")
		return
	}
	transactionsPerBlock, err := strconv.Atoi(args[0])
	if err != nil || transactionsPerBlock <= 0 {
		fmt.Println("Invalid value for TRANSACTIONS_PER_BLOCK.")
		return
	}

	totalTransactions, err := strconv.Atoi(args[1])
	if err != nil || totalTransactions <= 0 {
		fmt.Println("Invalid value for TOTAL_TRANSACTIONS.")
		return
	}

	totalBlocks := int(math.Ceil(float64(totalTransactions) / float64(transactionsPerBlock)))

	prevBlockHash := "0xabc123"
	blockWriter := make(BlockWriter)

	go func() {
		var blocks []MyBlock
		for block := range blockWriter {
			blocks = append(blocks, block)
			err := writeBlocksToFile(blocks)
			if err != nil {
				fmt.Println("Error writing blocks to file:", err)
				return
			}
		}
	}()

	for blockNumber := 1; blockNumber <= totalBlocks; blockNumber++ {
		block := MyBlock{
			BlockNumber:   blockNumber,
			Status:        Pending,
			PrevBlockHash: prevBlockHash,
			Txns:          make(map[string]Transaction),
		}

		block.ProcessingTime = time.Since(startTime)
		startIndex := (blockNumber - 1) * transactionsPerBlock + 1
		endIndex := blockNumber * transactionsPerBlock
		if endIndex > totalTransactions {
			endIndex = totalTransactions
		}

		InputTxns := make([]map[string]Entry, endIndex-startIndex)
		for i := startIndex; i < endIndex; i++ {
			txn := make(map[string]Entry)
			key := fmt.Sprintf("SIM%d", i+1)
			value := Entry{
				Val:   float64(i + 1),
				Ver:   float64(rand.Intn(10) + 1),
				Valid: true,
			}

			txn[key] = value
			InputTxns[i-startIndex] = txn
		}

		var wg sync.WaitGroup
		txnChan := make(chan Transaction)

		for i := 0; i < len(InputTxns); i += transactionsPerBlock {
			wg.Add(1)
			go block.pushValidTransactions(db, InputTxns[i:int(math.Min(float64(i+transactionsPerBlock), float64(len(InputTxns))))], txnChan, &wg)
		}

		go func() {
			wg.Wait()
			close(txnChan)
		}()

		for txn := range txnChan {
			block.Txns[txn.TxnId] = txn
		}

		block.UpdateBlockStatus(Committed)
		blockWriter.WriteBlock(block)
		prevBlockHash = fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s:%d", prevBlockHash, block.BlockNumber))))
	}
	close(blockWriter)

	for {
		var blockNumber int
		fmt.Print("Enter the block number: ")
		_, err = fmt.Scanln(&blockNumber)
		if err != nil {
			log.Fatal("Error reading block number:", err)
		}
		fetchedBlock, err := fetchBlockDetailsByNumber(blockNumber)
		if err != nil {
			fmt.Println("Error fetching block details:", err)
			return
		}
		fmt.Println("Fetched Block Details:")
		fmt.Println("Block Number:", fetchedBlock.BlockNumber)
		fmt.Println("Prev Block Hash:", fetchedBlock.PrevBlockHash)
		fmt.Println("Block Status:", fetchedBlock.Status)
		fmt.Println("Processing Time:", fetchedBlock.ProcessingTime)
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
		var input string
		fmt.Print("Do you want to fetch another block? (yes/no): ")
		_, err = fmt.Scanln(&input)
		if err != nil {
			log.Fatal("Error reading user input:", err)
		}

		if input != "yes" {
			break
		}
	}
	allBlocks, err := fetchAllBlockDetails()
	if err != nil {
		fmt.Println("Error fetching all block details:", err)
		return
	}

	fmt.Println("\nAll Blocks:")
	for _, b := range allBlocks {
		fmt.Println("Block Number:", b.BlockNumber)
		fmt.Println("Prev Block Hash:", b.PrevBlockHash)
		fmt.Println("Block Status:", b.Status)
		fmt.Println("Processing Time:", b.ProcessingTime)
		fmt.Println("Transactions:")
		for txnID, txn := range b.Txns {
			fmt.Println("  Transaction ID:", txnID)
			fmt.Println("  IsValid:", txn.IsValid)
			fmt.Println("  Data:")
			for key, entry := range txn.Data {
				fmt.Println("    Key:", key)
				fmt.Println("    Value:", entry.Val)
				fmt.Println("    Ver:", entry.Ver)
				fmt.Println("    Valid:", entry.Valid)
				fmt.Println("    Hash:", entry.Hash)
			}
		}
	}

	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)
	fmt.Println("\nTotal Execution Time:", elapsedTime)
}
func getEnvInt(key string, defaultValue int) int {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		fmt.Printf("Invalid value for environment variable %s, using default value\n", key)
		return defaultValue
	}
	return value
}
