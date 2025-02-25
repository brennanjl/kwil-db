package server

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"time"

	"github.com/kwilteam/kwil-db/cmd/kwild/config"
	"github.com/kwilteam/kwil-db/core/utils/url"
	"github.com/kwilteam/kwil-db/internal/abci/cometbft"

	cmtCfg "github.com/cometbft/cometbft/config"
	cmtEd "github.com/cometbft/cometbft/crypto/ed25519"
	cmttypes "github.com/cometbft/cometbft/types"
)

// cleanListenAddr tries to ensure the address has a scheme and port, as
// required by cometbft for its listen address settings. If it cannot parse, it
// is returned as-is so cometbft can try it (this is a best effort helper).
func cleanListenAddr(addr, defaultPort string) string {
	u, err := url.ParseURL(addr)
	if err != nil { // just see if cometbft takes it
		return addr
	}

	parsed := u.URL()
	if u.Port == 0 {
		// If port not included or explicitly set to 0, use the default.
		_, port, _ := net.SplitHostPort(u.Target)
		if port == "" {
			parsed.Host = net.JoinHostPort(u.Target, defaultPort)
		}
	}
	return parsed.String()
}

func portFromURL(u string) string {
	rpcAddress, err := url.ParseURL(u)
	if err != nil {
		return "0"
	}
	return strconv.Itoa(rpcAddress.Port)
}

// newCometConfig creates a new CometBFT config for use with NewCometBftNode.
// This applies The operator's settings from the Kwil config as well as applying
// some overrides to the defaults for Kwil.
//
// NOTE: this is somewhat error prone, so care must be taken to update this
// function when fields are added to KwildConfig.ChainCfg.
func newCometConfig(cfg *config.KwildConfig) *cmtCfg.Config {
	// Begin with CometBFT's default chain config.
	nodeCfg := cmtCfg.DefaultConfig()

	// Override defaults with our own if we do not expose them to the user.

	// Recheck should be the default, but make sure.
	nodeCfg.Mempool.Recheck = true

	// Translate the entire config.
	userChainCfg := cfg.ChainCfg

	if userChainCfg.Moniker != "" {
		nodeCfg.Moniker = userChainCfg.Moniker
	}

	nodeCfg.RPC.ListenAddress = cleanListenAddr(userChainCfg.RPC.ListenAddress,
		portFromURL(nodeCfg.RPC.ListenAddress))
	nodeCfg.RPC.TLSCertFile = cfg.AppCfg.TLSCertFile
	nodeCfg.RPC.TLSKeyFile = cfg.AppCfg.TLSKeyFile
	nodeCfg.RPC.TimeoutBroadcastTxCommit = time.Duration(userChainCfg.RPC.BroadcastTxTimeout)

	nodeCfg.P2P.ListenAddress = cleanListenAddr(userChainCfg.P2P.ListenAddress,
		portFromURL(nodeCfg.P2P.ListenAddress))
	nodeCfg.P2P.ExternalAddress = userChainCfg.P2P.ExternalAddress
	nodeCfg.P2P.PersistentPeers = userChainCfg.P2P.PersistentPeers
	nodeCfg.P2P.AddrBookStrict = userChainCfg.P2P.AddrBookStrict
	nodeCfg.P2P.MaxNumInboundPeers = userChainCfg.P2P.MaxNumInboundPeers
	nodeCfg.P2P.MaxNumOutboundPeers = userChainCfg.P2P.MaxNumOutboundPeers
	nodeCfg.P2P.UnconditionalPeerIDs = userChainCfg.P2P.UnconditionalPeerIDs
	nodeCfg.P2P.PexReactor = userChainCfg.P2P.PexReactor
	nodeCfg.P2P.AllowDuplicateIP = cfg.ChainCfg.P2P.AllowDuplicateIP
	nodeCfg.P2P.HandshakeTimeout = time.Duration(userChainCfg.P2P.HandshakeTimeout)
	nodeCfg.P2P.DialTimeout = time.Duration(userChainCfg.P2P.DialTimeout)
	nodeCfg.P2P.SeedMode = userChainCfg.P2P.SeedMode
	nodeCfg.P2P.Seeds = userChainCfg.P2P.Seeds

	nodeCfg.Mempool.Size = userChainCfg.Mempool.Size
	nodeCfg.Mempool.CacheSize = userChainCfg.Mempool.CacheSize
	nodeCfg.Mempool.MaxTxBytes = userChainCfg.Mempool.MaxTxBytes
	nodeCfg.Mempool.MaxTxsBytes = int64(userChainCfg.Mempool.MaxTxsBytes)

	nodeCfg.Consensus.TimeoutPropose = time.Duration(userChainCfg.Consensus.TimeoutPropose)
	nodeCfg.Consensus.TimeoutPrevote = time.Duration(userChainCfg.Consensus.TimeoutPrevote)
	nodeCfg.Consensus.TimeoutPrecommit = time.Duration(userChainCfg.Consensus.TimeoutPrecommit)
	nodeCfg.Consensus.TimeoutCommit = time.Duration(userChainCfg.Consensus.TimeoutCommit)

	nodeCfg.StateSync.Enable = false
	// nodeCfg.StateSync.Enable = userChainCfg.StateSync.Enable
	// nodeCfg.StateSync.TempDir = userChainCfg.StateSync.TempDir
	// nodeCfg.StateSync.RPCServers = userChainCfg.StateSync.RPCServers
	// nodeCfg.StateSync.DiscoveryTime = userChainCfg.StateSync.DiscoveryTime
	// nodeCfg.StateSync.ChunkRequestTimeout = userChainCfg.StateSync.ChunkRequestTimeout

	// Standardize the paths.
	nodeCfg.DBPath = cometbft.DataDir // i.e. "data", we do not allow users to change

	chainRoot := filepath.Join(cfg.RootDir, abciDirName)
	nodeCfg.SetRoot(chainRoot)
	// NOTE: The Genesis field is the one in cometbft's GenesisDoc, which is
	// different from kwild's, which contains more fields (and not string
	// int64). The documented genesis.json in kwild's root directory is:
	//   filepath.Join(cfg.RootDir, cometbft.GenesisJSONName)
	// This file is only used to reflect the in-memory genesis config provided
	// to cometbft via a GenesisDocProvider. It it is not used by cometbft.
	nodeCfg.Genesis = filepath.Join(chainRoot, "config", cometbft.GenesisJSONName)
	nodeCfg.P2P.AddrBook = cometbft.AddrBookPath(chainRoot)
	// For the same reasons described for the genesis.json path above, clear the
	// node and validator file fields since they are provided in-memory.
	nodeCfg.PrivValidatorKey = ""
	nodeCfg.PrivValidatorState = ""
	nodeCfg.NodeKey = ""

	return nodeCfg
}

// Used by cometbft while initializing the node to extract the genesis configuration
func extractGenesisDoc(g *config.GenesisConfig) (*cmttypes.GenesisDoc, error) {

	consensusParams := &cmttypes.ConsensusParams{
		Block: cmttypes.BlockParams{ // TODO: set MaxBytes to -1 so we can do the truncation in PrepareProposal after our other processing
			MaxBytes: g.ConsensusParams.Block.MaxBytes,
			MaxGas:   g.ConsensusParams.Block.MaxGas,
		},
		Evidence: cmttypes.EvidenceParams{
			MaxAgeNumBlocks: g.ConsensusParams.Evidence.MaxAgeNumBlocks,
			MaxAgeDuration:  g.ConsensusParams.Evidence.MaxAgeDuration,
			MaxBytes:        g.ConsensusParams.Evidence.MaxBytes,
		},
		Version: cmttypes.VersionParams{
			App: g.ConsensusParams.Version.App,
		},
		Validator: cmttypes.ValidatorParams{
			PubKeyTypes: g.ConsensusParams.Validator.PubKeyTypes,
		},
		ABCI: cmttypes.ABCIParams{
			VoteExtensionsEnableHeight: 0, // disabled for now
		},
	}

	genDoc := &cmttypes.GenesisDoc{
		ChainID:         g.ChainID,
		GenesisTime:     g.GenesisTime,
		InitialHeight:   g.InitialHeight,
		AppHash:         g.DataAppHash,
		ConsensusParams: consensusParams,
	}

	for _, v := range g.Validators {
		if len(v.PubKey) != cmtEd.PubKeySize {
			return nil, fmt.Errorf("pubkey is incorrect size: %v", v.PubKey.String())
		}
		pubKey := cmtEd.PubKey(v.PubKey)
		genDoc.Validators = append(genDoc.Validators, cmttypes.GenesisValidator{
			Address: pubKey.Address(),
			PubKey:  pubKey,
			Power:   v.Power,
			Name:    v.Name,
		})
	}
	return genDoc, nil
}
