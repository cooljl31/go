package history

import (
	"testing"

	"github.com/stellar/go/services/horizon/internal/db2"
	"github.com/stellar/go/services/horizon/internal/test"
	"github.com/stellar/go/xdr"
	"fmt"

	sq "github.com/Masterminds/squirrel"
)

func TestTradeQueries(t *testing.T) {
	tt := test.Start(t).Scenario("kahuna")
	defer tt.Finish()
	q := &Q{tt.HorizonSession()}
	var trades []Trade

	// All trades
	err := q.Trades().Select(&trades)
	if tt.Assert.NoError(err) {
		tt.Assert.Len(trades, 4)
	}

	// Paging
	pq := db2.MustPageQuery(trades[0].PagingToken(), "asc", 1)
	var pt []Trade

	err = q.Trades().Page(pq).Select(&pt)
	if tt.Assert.NoError(err) {
		tt.Assert.Len(pt, 1)
		tt.Assert.Equal(trades[1], pt[0])
	}

	// Cursor bounds checking
	pq = db2.MustPageQuery("", "desc", 1)
	err = q.Trades().Page(pq).Select(&pt)
	tt.Assert.NoError(err)

	// test for asset pairs
	q.TradesForAssetPair(2, 3).Select(&trades)
	tt.Assert.Len(trades, 0)

	q.TradesForAssetPair(1, 2).Select(&trades)
	tt.Assert.Len(trades, 1)

	tt.Assert.Equal(xdr.Int64(2000000000), trades[0].BaseAmount)
	tt.Assert.Equal(xdr.Int64(1000000000), trades[0].CounterAmount)
	tt.Assert.Equal(true, trades[0].BaseIsSeller)

	//EXPERIMENTAL
	var tradeAggs []TradeAggregation
	tradesQ := q.BucketTradesForAssetPair(1, 2, 5000)
	tradesQ.SelectAggregateByBucket(&tradeAggs)
	fmt.Println(tradesQ.Err)
	fmt.Println(tradeAggs)

	tradesQ = q.BucketTradesForAssetPair(2, 1, 5000)
	tradesQ.SelectAggregateByBucket(&tradeAggs)
	fmt.Println(tradesQ.Err)
	fmt.Println(tradeAggs)
}

var tradeInserter = sq.Insert("history_trades").Columns(
	"history_operation_id",
	"\"order\"",
	"ledger_closed_at",
	"offer_id",
	"base_account_id",
	"base_asset_id",
	"base_amount",
	"counter_account_id",
	"counter_asset_id",
	"counter_amount",
	"base_is_seller",
)

var assetInserter = sq.Insert("history_assets").Columns(
	"asset_type",
	"asset_code",
	"asset_issuer",
)

//history_assets (asset_type, asset_code, asset_issuer) VALUES (?,?,?) RETURNING id`,

func insertAsset(assetType string, assetCode string, assetIssuer string) int64 {
	assetInserter.Values()
	return 0
}

func insertTrade() int64 {
	return 0
}

func TestTradeAggQueries(t *testing.T) {
	//tt := test.Start(t)
	//q := &Q{tt.HorizonSession()}
	//q.GetCreateAssetID()
	//
	//
	//insertBuilder.Values(
	//	opid,
	//	order,
	//	time.Unix(ledgerClosedAt, 0).UTC(),
	//	trade.OfferId,
	//	baseAccountId,
	//	baseAssetId,
	//	baseAmount,
	//	counterAccountId,
	//	counterAssetId,
	//	counterAmount,
	//	soldAssetId < boughtAssetId,
	//)
	//
	//insertBuilder.Into()
	//
	//q.TradesForAssetPair()

}
