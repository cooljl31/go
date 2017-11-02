package common_trades

import (
	"github.com/stellar/go/services/horizon/internal/test"
	. "github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/xdr"
)

func getTestAsset(code string) xdr.Asset {
	var codeBytes [4]byte
	copy(codeBytes[:], []byte(code))
	ca4 := xdr.AssetAlphaNum4{Issuer: getTestAccount(), AssetCode: codeBytes}
	return xdr.Asset{Type: xdr.AssetTypeAssetTypeCreditAlphanum4, AlphaNum4: &ca4, AlphaNum12: nil}
}

var accCounter byte

func getTestAccount() xdr.AccountId {
	accCounter++
	acc, _ := xdr.NewAccountId(xdr.PublicKeyTypePublicKeyTypeEd25519, xdr.Uint256{accCounter})
	return acc
}

var opCounter int64

func ingestTestTrade(
	q *Q,
	assetSold xdr.Asset,
	assetBought xdr.Asset,
	seller xdr.AccountId,
	buyer xdr.AccountId,
	amountSold int64,
	amountBought int64,
	timestamp int64) error {

	trade := xdr.ClaimOfferAtom{}
	trade.AmountBought = xdr.Int64(amountBought)
	trade.SellerId = seller
	trade.AmountSold = xdr.Int64(amountSold)
	trade.AssetBought = assetBought
	trade.AssetSold = assetSold

	opCounter++
	return q.InsertTrade(opCounter, 0, buyer, trade, timestamp)
}

func PopulateTestTrades(tt *test.T, startTs int64, numOfTrades int, delta int64) (ass1 xdr.Asset, ass2 xdr.Asset) {
	q := &Q{tt.HorizonSession()}

	acc1 := getTestAccount()
	acc2 := getTestAccount()
	ass1 = getTestAsset("usd")
	ass2 = getTestAsset("euro")

	for i := 1; i <= numOfTrades; i++ {
		err := ingestTestTrade(q, ass1, ass2, acc1, acc2, int64(i*100), int64(i*100)*int64(i), startTs+(delta*int64(i-1)))
		tt.Assert.NoError(err)
	}

	var trades []Trade
	// All trades
	err := q.Trades().Select(&trades)
	if tt.Assert.NoError(err) {
		tt.Assert.Len(trades, 10)
	}

	return
}
