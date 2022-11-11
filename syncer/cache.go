package syncer

type cache struct {
	//store *store.Store
	//tokenHolders map[string]*store.TokenHolder
	//tokens       map[string]*store.Token
	//ownerships   map[string]*store.TokenOwnership
}

//func newCache(s *store.Store) *cache {
//	return &cache{
//		store:        s,
//		tokenHolders: map[string]*store.TokenHolder{},
//		tokens:       map[string]*store.Token{},
//		ownerships:   map[string]*store.TokenOwnership{},
//	}
//}
//
//func (s *cache) writeTokenHolder(substreamsKey string, quantity uint64, atBlock bstream.BlockRef) (err error) {
//	holderAddr, contractAddr, err := explodeTokenHolderKey(substreamsKey)
//	if err != nil {
//		return fmt.Errorf("explode store delta key: %w", err)
//	}
//
//	cacheKey := s.tokenHolderKey(holderAddr, contractAddr)
//	var holder *store.TokenHolder
//	var found bool
//	holder, found = s.tokenHolders[cacheKey]
//	if !found {
//		holder, err = s.store.GetTokenHolder(holderAddr, contractAddr)
//		if err != nil && err != store.NotFound {
//			return fmt.Errorf("unable to get token holder: %w", err)
//		}
//		if err == store.NotFound {
//			holder = &store.TokenHolder{
//				EthAddress:      holderAddr,
//				ContractAddress: contractAddr,
//			}
//		}
//	}
//
//	holder.Quantity = quantity
//	holder.UpdatedBlockNum = atBlock.Num()
//
//	s.tokenHolders[cacheKey] = holder
//	return nil
//}
//
//func (s *cache) writeTokenOwnership(pbtoken *pberc721.TokenOwnership, atBlock bstream.BlockRef) (err error) {
//	contractAddr := hex.EncodeToString(pbtoken.Token.Contract)
//	tokenID := fmt.Sprintf("%d", pbtoken.Token.TokenId)
//	ownerAddr := hex.EncodeToString(pbtoken.Token.Recipient)
//	event := pbtoken.ActityType.String()
//
//	validKey := false
//	i := uint64(0)
//	cacheKey := ""
//	for !validKey {
//		cacheKey = s.ownershipKey(contractAddr, tokenID, ownerAddr, event, i)
//		if _, found := s.ownerships[cacheKey]; !found {
//			validKey = true
//		}
//		i++
//	}
//
//	token := &store.TokenOwnership{
//		ContractAddress: contractAddr,
//		TokenID:         tokenID,
//		OwnerAddress:    ownerAddr,
//		BlockNum:        pbtoken.Token.BlockNumber,
//		Timestamp:       pbtoken.ActivityAt,
//	}
//	if pbtoken.ActityType == pberc721.TokenOwnership_ADDED {
//		token.Quantity = 1
//	} else if pbtoken.ActityType == pberc721.TokenOwnership_LOSSED {
//		token.Quantity = 0
//	} else {
//		return fmt.Errorf("inclay ownership type: %s", pbtoken.ActityType)
//	}
//	s.ownerships[cacheKey] = token
//	return nil
//}
//
//func (s *cache) writeToken(pbtoken *pberc721.Token, atBlock bstream.BlockRef) (err error) {
//	contractAddr := hex.EncodeToString(pbtoken.Contract)
//	recipientAddr := hex.EncodeToString(pbtoken.Recipient)
//	tokenID := fmt.Sprintf("%d", pbtoken.TokenId)
//
//	cacheKey := s.tokenKey(contractAddr, tokenID)
//	var token *store.Token
//	var found bool
//	token, found = s.tokens[cacheKey]
//	if !found {
//		token, err = s.store.GetToken(contractAddr, tokenID)
//		if err != nil && err != store.NotFound {
//			return fmt.Errorf("unable to get token: %w", err)
//		}
//		if err == store.NotFound {
//			token = &store.Token{
//				ContractAddress: contractAddr,
//				TokenID:         tokenID,
//				CreatedBlockNum: atBlock.Num(),
//			}
//		}
//	}
//
//	token.OwnerAddress = recipientAddr
//	token.UpdatedBlockNum = atBlock.Num()
//
//	s.tokens[cacheKey] = token
//	return nil
//}
//
//func (s *cache) tokenHolderKey(ethAddress, contractAddress string) string {
//	return fmt.Sprintf("%s:%s", ethAddress, contractAddress)
//}
//
//func (s *cache) tokenKey(contractAddress, tokenID string) string {
//	return fmt.Sprintf("%s:%s", contractAddress, tokenID)
//}
//
//func (s *cache) ownershipKey(contractAddress, tokenID, owner, event string, increment uint64) string {
//	return fmt.Sprintf("%s:%s:%s:%s:%d", contractAddress, tokenID, owner, event, increment)
//}
//
//func (s *cache) Flush(cursor *store.Cursor) error {
//	err := s.store.Transaction(func(_ *store.Store) error {
//		for _, v := range s.tokenHolders {
//			if v.Created() {
//				if _, err := s.store.UpdateTokenHolder(v); err != nil {
//					return fmt.Errorf("unable to create token holder: %w", err)
//				}
//				continue
//			}
//			if _, err := s.store.CreateTokenHolder(v); err != nil {
//				return fmt.Errorf("unable to update token holder: %w", err)
//			}
//		}
//
//		for _, v := range s.tokens {
//			if v.Created() {
//				if _, err := s.store.UpdateToken(v); err != nil {
//					return fmt.Errorf("unable to create token: %w", err)
//				}
//				continue
//			}
//			if _, err := s.store.CreateToken(v); err != nil {
//				return fmt.Errorf("unable to update token: %w", err)
//			}
//		}
//
//		for _, v := range s.ownerships {
//			if _, err := s.store.CreateTokenOwnership(v); err != nil {
//				return fmt.Errorf("unable to update ownership: %w", err)
//			}
//		}
//
//		if _, err := s.store.updateCursor(cursor); err != nil {
//			return fmt.Errorf("failed to create or update cursor: %w", err)
//		}
//		return nil
//	})
//	if err != nil {
//		return fmt.Errorf("failed to flush: %w", err)
//	}
//
//	FlushCount.Inc()
//
//	return nil
//}
//
//func (s *cache) Reset() error {
//	s.tokenHolders = map[string]*store.TokenHolder{}
//	s.tokens = map[string]*store.Token{}
//	s.ownerships = map[string]*store.TokenOwnership{}
//	return nil
//}
//
//func explodeTokenHolderKey(key string) (ethAddress string, contractAddress string, err error) {
//	chunks := strings.Split(key, ":")
//	if len(chunks) != 3 {
//		return "", "", fmt.Errorf("expected key format to be total:<holder-address>:<contract-addr>")
//	}
//	return chunks[1], chunks[2], nil
//}
