package history_test

import (
	"testing"

	"github.com/stellar/go/services/horizon/internal/db2"
	. "github.com/stellar/go/services/horizon/internal/db2/history"
	. "github.com/stellar/go/services/horizon/internal/test/common_trades"
	"github.com/stellar/go/services/horizon/internal/test"
	"github.com/stellar/go/xdr"
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
}

func TestTradeAggQueries(t *testing.T) {
	tt := test.Start(t).Scenario("base")
	defer tt.Finish()

	const startMillis = int64(0)
	const numOfTrades = 10
	const delta = int64(60 * 1000)

	ass1, ass2 := PopulateTestTrades(tt, startMillis, numOfTrades, delta)
	q := &Q{tt.HorizonSession()}

	//test one bucket for all
	var aggs []TradeAggregation

	expected := TradeAggregation{startMillis, 10, 5500, 38500, 5.5, 10, 1, 1, 10}
	err := q.BucketTradesForAssetPairAssets(ass1, ass2, 1000*60*60).
		FromStartTime(startMillis).
		FromEndTime(startMillis + delta*(numOfTrades+1)).
		SelectAggregateByBucket(&aggs)
	if tt.Assert.NoError(err) {
		if tt.Assert.Len(aggs, 1) {
			tt.Assert.Equal(expected, aggs[0])
		}
	}

	//test one bucket for all - reverse
	expected = TradeAggregation{startMillis, 10, 38500, 5500, 0.2928968253968254, 1, 0.1, 1, 0.1}
	err = q.BucketTradesForAssetPairAssets(ass2, ass1, 1000*60*60).
		FromStartTime(startMillis).
		FromEndTime(startMillis + delta*(numOfTrades+1)).
		SelectAggregateByBucket(&aggs)
	if tt.Assert.NoError(err) {
		if tt.Assert.Len(aggs, 1) {
			tt.Assert.Equal(expected, aggs[0])
		}
	}

	//Test one bucket each, sample test one aggregation
	expected = TradeAggregation{240000, 1, 500, 2500, 5, 5, 5, 5, 5}
	err = q.BucketTradesForAssetPairAssets(ass1, ass2, 1000*60).
		FromStartTime(startMillis).
		FromEndTime(startMillis + delta*(numOfTrades+1)).
		SelectAggregateByBucket(&aggs)
	if tt.Assert.NoError(err) {
		if tt.Assert.Len(aggs, 10) {
			tt.Assert.Equal(aggs[4], expected)
		}
	}

	//Test two bucket each, sample test one aggregation
	expected = TradeAggregation{240000, 2, 1100, 6100, 5.5, 6, 5, 5, 6}
	err = q.BucketTradesForAssetPairAssets(ass1, ass2, 1000*60*2).
		FromStartTime(startMillis).
		FromEndTime(startMillis + delta*(numOfTrades+1)).
		SelectAggregateByBucket(&aggs)
	if tt.Assert.NoError(err) {
		if tt.Assert.Len(aggs, 5) {
			tt.Assert.Equal(aggs[2], expected)
		}
	}
}
