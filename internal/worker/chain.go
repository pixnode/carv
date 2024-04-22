package worker

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"math/big"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/pkg/errors"

	"github.com/carv-protocol/verifier/internal/conf"
	"github.com/carv-protocol/verifier/internal/data"
	"github.com/carv-protocol/verifier/pkg/contract"
)

const (
	defaultBlockNumberChanLength = 10
)

type Chains struct {
	cf                            *conf.Bootstrap
	data                          *data.Data
	logger                        *log.Helper
	verifierRepo                  *data.VerifierRepo
	transactionRepo               *data.TransactionRepo
	reportTeeAttestationEventRepo *data.ReportTeeAttestationEventRepo
	cAbis                         []abi.ABI
	ethClients                    []*ethclient.Client
	contractObjs                  []*contract.Contract

	stopChans             []chan struct{}
	blockNumberChans      []chan int64
	latestBlockNumbers    []int64
	processedBlockNumbers []int64
	processingFlags       []bool

	locks []sync.RWMutex
}

func NewChain(ctx context.Context, bootstrap *conf.Bootstrap, data *data.Data, logger *log.Helper, verifierRepo *data.VerifierRepo, transactionRepo *data.TransactionRepo, reportTeeAttestationRepo *data.ReportTeeAttestationEventRepo) (*Chains, error) {
	abiArr := make([]abi.ABI, 0)
	ethclientArr := make([]*ethclient.Client, 0)
	contractObjs := make([]*contract.Contract, 0)
	stopChanArr := make([]chan struct{}, 0)
	blockNumberChanArr := make([]chan int64, 0)
	processedBlockNumbers := make([]int64, len(bootstrap.Chains))
	processingFlags := make([]bool, len(bootstrap.Chains))
	locks := make([]sync.RWMutex, len(bootstrap.Chains))
	lastBlockNumbers := make([]int64, len(bootstrap.Chains))
	for i := 0; i < len(bootstrap.Chains); i++ {

		abiFile, err := os.ReadFile(bootstrap.Contracts[i].Abi)
		if err != nil {
			return nil, err
		}
		cAbi, err := abi.JSON(strings.NewReader(string(abiFile)))
		if err != nil {
			return nil, errors.Wrap(err, "abi json error")
		}
		abiArr = append(abiArr, cAbi)
		ethClient, err := ethclient.DialContext(ctx, bootstrap.Chains[i].RpcUrl)
		if err != nil {
			return nil, errors.Wrapf(err, "new eth client error, rpc url: %s", bootstrap.Chains[i].RpcUrl)
		}
		ethclientArr = append(ethclientArr, ethClient)

		contractObj, err := contract.NewContract(common.HexToAddress(bootstrap.Contracts[i].Addr), ethClient)
		if err != nil {
			return nil, errors.Wrapf(err, "NewContract error")
		}
		contractObjs = append(contractObjs, contractObj)

		stopChanArr = append(stopChanArr, make(chan struct{}))
		blockNumberChanArr = append(blockNumberChanArr, make(chan int64, defaultBlockNumberChanLength))

	}
	chains := &Chains{
		cf:                            bootstrap,
		data:                          data,
		logger:                        logger,
		verifierRepo:                  verifierRepo,
		transactionRepo:               transactionRepo,
		reportTeeAttestationEventRepo: reportTeeAttestationRepo,
		cAbis:                         abiArr,
		ethClients:                    ethclientArr,
		contractObjs:                  contractObjs,
		latestBlockNumbers:            lastBlockNumbers,
		stopChans:                     stopChanArr,
		blockNumberChans:              blockNumberChanArr,
		processedBlockNumbers:         processedBlockNumbers,
		processingFlags:               processingFlags,
		locks:                         locks,
	}
	return chains, nil
}

func (c *Chains) Start(ctx context.Context, chainId int) error {
	startBlockNumber := c.cf.Chains[chainId].StartBlock
	if startBlockNumber == 0 {
		blockNumber, err := c.ethClients[chainId].BlockNumber(ctx)
		if err != nil {
			return errors.Wrapf(err, "chain [%s] get BlockNumber error", c.cf.Chains[chainId].ChainName)
		}
		startBlockNumber = int64(blockNumber)

		c.locks[chainId].Lock()
		c.latestBlockNumbers[chainId] = int64(blockNumber)
		c.locks[chainId].Unlock()
	}

	c.logger.WithContext(ctx).Infof("chain [%s] startBlockNumber: %d", c.cf.Chains[chainId].ChainName, startBlockNumber)

	c.locks[chainId].Lock()
	c.processedBlockNumbers[chainId] = startBlockNumber - 1
	c.blockNumberChans[chainId] <- startBlockNumber
	c.locks[chainId].Unlock()

	go c.run(ctx, chainId)
	go c.stick(ctx, chainId)

	return nil
}

func (c *Chains) Stop(ctx context.Context, chainId int) error {
	c.logger.WithContext(ctx).Infof("chain [%s] stopping", c.cf.Chains[chainId].ChainName)

	close(c.stopChans[chainId])

	// todo stop

	c.logger.WithContext(ctx).Infof("chain [%s] stopped", c.cf.Chains[chainId].ChainName)

	return nil
}

func (c *Chains) run(ctx context.Context, chainId int) {
	for {
		select {
		case <-c.stopChans[chainId]:
			c.logger.WithContext(ctx).Infof("chain run [%s] stopping", c.cf.Chains[chainId].ChainName)
			return
		case blockNumber := <-c.blockNumberChans[chainId]:
			c.Process(ctx, blockNumber, chainId)
		}
	}
}

func (c *Chains) stick(ctx context.Context, chainId int) {
	latestBlockNumberTicker := time.NewTicker(1 * time.Second)
	sendBlockNumberChanTicker := time.NewTicker(1 * time.Second)
	recordStatusTicker := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-c.stopChans[chainId]:
			c.logger.WithContext(ctx).Infof("chain stick [%s] stopping", c.cf.Chains[chainId].ChainName)
			return
		case <-latestBlockNumberTicker.C:
			go c.UpdateLatestBlockNumber(ctx, chainId)
		case <-sendBlockNumberChanTicker.C:
			go c.SendBlockNumberChanByTicker(ctx, chainId)
		case <-recordStatusTicker.C:
			go c.RecordStatus(ctx, chainId)
		}
	}
}

func (c *Chains) RecordStatus(ctx context.Context, chainId int) {
	c.logger.WithContext(ctx).Infof("chain [%s] status, latestBlockNumber: %d, processedBlockNumber: %d, processingFlag: %v, blockNumberChanLength: %d",
		c.cf.Chains[chainId].ChainName, c.GetLatestBlockNumber(chainId), c.GetProcessedBlockNumber(chainId), c.GetProcessingFlag(chainId), c.GetBlockNumberChanLength(chainId))
}

func (c *Chains) UpdateLatestBlockNumber(ctx context.Context, chainId int) {
	blockNumber, err := c.ethClients[chainId].BlockNumber(ctx)
	if err != nil {
		c.logger.WithContext(ctx).Errorf("chain [%s] get BlockNumber error: %s", c.cf.Chains[chainId].ChainName, err)
	}

	c.locks[chainId].Lock()
	c.latestBlockNumbers[chainId] = int64(blockNumber)
	c.locks[chainId].Unlock()
}

func (c *Chains) GetLatestBlockNumber(chainId int) int64 {
	c.locks[chainId].RLock()
	blockNumber := c.latestBlockNumbers[chainId]
	c.locks[chainId].RUnlock()

	return blockNumber
}

func (c *Chains) SetProcessingFlag(b bool, chainId int) {
	c.locks[chainId].Lock()
	c.processingFlags[chainId] = b
	c.locks[chainId].Unlock()
}

func (c *Chains) GetProcessingFlag(chainId int) bool {
	c.locks[chainId].RLock()
	b := c.processingFlags[chainId]
	c.locks[chainId].RUnlock()

	return b
}

func (c *Chains) SetProcessedBlockNumber(processedBlockNumber int64, chainId int) {
	c.locks[chainId].Lock()
	c.processedBlockNumbers[chainId] = processedBlockNumber
	c.locks[chainId].Unlock()
}

func (c *Chains) GetProcessedBlockNumber(chainId int) int64 {
	c.locks[chainId].RLock()
	processedBlockNumber := c.processedBlockNumbers[chainId]
	c.locks[chainId].RUnlock()

	return processedBlockNumber
}

func (c *Chains) GetBlockNumberChanLength(chainId int) int {
	c.locks[chainId].RLock()
	blockNumberChanLength := len(c.blockNumberChans[chainId])
	c.locks[chainId].RUnlock()

	return blockNumberChanLength
}

func (c *Chains) IsBlockNumberChanFull(chainId int) bool {
	c.locks[chainId].RLock()
	blockNumberChanLength := len(c.blockNumberChans[chainId])
	c.locks[chainId].RUnlock()

	return blockNumberChanLength >= defaultBlockNumberChanLength
}

func (c *Chains) SendBlockNumberChanByTicker(ctx context.Context, chainId int) {
	if c.GetProcessingFlag(chainId) {
		return
	}
	if c.IsBlockNumberChanFull(chainId) {
		return
	}

	latestBlockNumber := c.GetLatestBlockNumber(chainId)
	processedBlockNumber := c.GetProcessedBlockNumber(chainId)

	if latestBlockNumber > processedBlockNumber {
		c.locks[chainId].Lock()
		c.blockNumberChans[chainId] <- processedBlockNumber + 1
		c.locks[chainId].Unlock()
	}
}

func (c *Chains) getEndBlockNumber(startBlockNumber int64, chainId int) int64 {
	endBlockNumber := startBlockNumber

	latestBlockNumber := c.GetLatestBlockNumber(chainId)
	if startBlockNumber >= latestBlockNumber {
		return endBlockNumber
	}

	if latestBlockNumber-startBlockNumber > c.cf.Chains[chainId].OffsetBlock {
		endBlockNumber = startBlockNumber + c.cf.Chains[chainId].OffsetBlock
	} else {
		endBlockNumber = latestBlockNumber - 1
	}

	return endBlockNumber
}

func (c *Chains) SendAttestationTrx(ctx context.Context, attestationIds [][32]byte, results []bool, chainId int) (string, error) {

	privateKeyBytes, err := Sm4Decrypt(c, chainId)
	txHash := ""
	if err != nil {
		return txHash, errors.Wrap(err, "pk decrypt error")
	}
	privateKey, err := crypto.HexToECDSA(string(privateKeyBytes))
	if err != nil {
		return txHash, errors.Wrap(err, "pk sm4 HexToECDSA error")
	}
	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return txHash, errors.New("cannot assert type: publicKey is not of type *ecdsa.PublicKey")
	}
	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)

	nonce, err := c.ethClients[chainId].PendingNonceAt(ctx, fromAddress)
	if err != nil {
		return txHash, errors.Wrap(err, "eth client get Nonce error")
	}
	gasPrice, err := c.ethClients[chainId].SuggestGasPrice(ctx)
	if err != nil {
		return txHash, errors.Wrap(err, "eth client get SuggestGasPrice error")
	}

	chainID, err := c.ethClients[chainId].ChainID(ctx)
	if err != nil {
		return txHash, errors.Wrap(err, "eth client get ChainID error")
	}

	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainID)
	if err != nil {
		return txHash, errors.Wrap(err, "bind NewKeyedTransactorWithChainID error")
	}
	auth.Nonce = big.NewInt(int64(nonce))
	//auth.Value = big.NewInt(0)     // in wei
	//auth.GasLimit = uint64(300000) // in units
	auth.GasPrice = gasPrice

	tx, err := c.contractObjs[chainId].VerifyAttestationBatch(auth, attestationIds, results)
	if err != nil {
		return txHash, errors.Wrap(err, "contract VerifyAttestationBatch error")
	}
	txHash = tx.Hash().Hex()

	c.logger.WithContext(ctx).Infof("tx hash: %s", tx.Hash().Hex())

	return txHash, nil
}

func (c *Chains) process(ctx context.Context, startBlockNumber, endBlockNumber int64, chainId int) error {
	q := ethereum.FilterQuery{
		FromBlock: big.NewInt(startBlockNumber),
		ToBlock:   big.NewInt(endBlockNumber),
		Addresses: []common.Address{
			common.HexToAddress(c.cf.Contracts[chainId].Addr),
		},
		Topics: [][]common.Hash{
			{
				common.HexToHash(c.cf.Contracts[chainId].Topic),
			},
		},
	}

	cLogs, err := c.ethClients[chainId].FilterLogs(ctx, q)
	if err != nil {
		return errors.Wrap(err, "client FilterLogs error")
	}

	var logInfoList []LogInfo
	var attestationIds [][32]byte
	var result []bool

	for _, cLog := range cLogs {
		unpackedData, err := c.contractObjs[chainId].ParseReportTeeAttestation(cLog)
		if err != nil {
			return errors.Wrap(err, "contract ParseReportTeeAttestation error")
		}

		logInfoList = append(logInfoList, LogInfo{
			BlockNumber:        cLog.BlockNumber,
			ContractAddress:    cLog.Address,
			TxHash:             cLog.TxHash,
			TxIndex:            unpackedData.Raw.TxIndex,
			TeeAddress:         unpackedData.TeeAddress,
			CampaignId:         unpackedData.CampaignId,
			AttestationIdBytes: unpackedData.AttestationId,
			AttestationIdStr:   hex.EncodeToString(unpackedData.AttestationId[:]),
			Attestation:        unpackedData.Attestation,
		})

		attestationIds = append(attestationIds, unpackedData.AttestationId)
		// Verify attestation
		isTrue, err := verifyAttestation(c, unpackedData.Attestation, chainId)
		if !isTrue {
			result = append(result, false)
			continue
		}
		result = append(result, true)

	}

	c.logger.WithContext(ctx).Infof("logInfoList: %+v", logInfoList)

	// send transaction to chain
	if len(attestationIds) == 0 {
		return nil
	}
	_, err = c.SendAttestationTrx(ctx, attestationIds, result, chainId)

	if err != nil {
		return err
	}

	return nil
}

func (c *Chains) Process(ctx context.Context, startBlockNumber int64, chainId int) {
	processedBlockNumber := c.GetProcessedBlockNumber(chainId)
	if processedBlockNumber != startBlockNumber-1 {
		c.logger.WithContext(ctx).Warnf("chain [%s] Process, startBlockNumber: %d, processedBlockNumber: %d", c.cf.Chains[chainId].ChainName, startBlockNumber, processedBlockNumber)
		return
	}

	c.SetProcessingFlag(true, chainId)
	defer c.SetProcessingFlag(false, chainId)

	endBlockNumber := c.getEndBlockNumber(startBlockNumber, chainId)

	c.logger.WithContext(ctx).Infof("chain [%s] Process, latestBlockNumber: %d, startBlockNumber: %d, endBlockNumber: %d", c.cf.Chains[chainId].ChainName, c.GetLatestBlockNumber(chainId), startBlockNumber, endBlockNumber)

	err := c.process(ctx, startBlockNumber, endBlockNumber, chainId)
	if err != nil {
		c.logger.WithContext(ctx).Errorf("chain [%s] Process, process error: %s", c.cf.Chains[chainId].ChainName, err)
		return
	}

	c.SetProcessedBlockNumber(endBlockNumber, chainId)

	if c.IsBlockNumberChanFull(chainId) {
		return
	}

	latestBlockNumber := c.GetLatestBlockNumber(chainId)
	c.locks[chainId].Lock()
	if latestBlockNumber > endBlockNumber {
		c.blockNumberChans[chainId] <- endBlockNumber + 1
	}
	c.locks[chainId].Unlock()
}
