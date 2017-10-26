package horizon

import (
	"errors"

	"github.com/stellar/go/services/horizon/internal/db2"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/services/horizon/internal/render/hal"
	"github.com/stellar/go/services/horizon/internal/resource"
	"github.com/stellar/go/xdr"
	"net/url"
	"time"
)

type TradeIndexAction struct {
	Action
	BaseAssetFilter       xdr.Asset
	HasBaseAssetFilter    bool
	CounterAssetFilter    xdr.Asset
	HasCounterAssetFilter bool
	PagingParams          db2.PageQuery
	Records               []history.Trade
	Page                  hal.Page
}

// JSON is a method for actions.JSON
func (action *TradeIndexAction) JSON() {
	action.Do(
		action.EnsureHistoryFreshness,
		action.loadParams,
		action.loadRecords,
		action.loadPage,
		func() {
			hal.Render(action.W, action.Page)
		},
	)
}

// loadParams sets action.Query from the request params
func (action *TradeIndexAction) loadParams() {
	action.PagingParams = action.GetPageQuery()
	action.BaseAssetFilter, action.HasBaseAssetFilter = action.MaybeGetAsset("base_")
	action.CounterAssetFilter, action.HasCounterAssetFilter = action.MaybeGetAsset("counter_")
}

// loadRecords populates action.Records
func (action *TradeIndexAction) loadRecords() {
	trades := action.HistoryQ().Trades()

	if action.HasBaseAssetFilter {

		baseAssetId, err := action.HistoryQ().GetAssetID(action.BaseAssetFilter)
		if err != nil {
			action.Err = err
			return
		}

		if action.HasCounterAssetFilter {

			counterAssetId, err := action.HistoryQ().GetAssetID(action.CounterAssetFilter)
			if err != nil {
				action.Err = err
				return
			}
			trades = action.HistoryQ().TradesForAssetPair(baseAssetId, counterAssetId)
		} else {
			action.Err = errors.New("this endpoint supports asset pairs but only one asset supplied")
			return
		}
	}
	action.Err = trades.Page(action.PagingParams).Select(&action.Records)
}

// loadPage populates action.Page
func (action *TradeIndexAction) loadPage() {
	for _, record := range action.Records {
		var res resource.Trade

		action.Err = res.Populate(action.Ctx, record)
		if action.Err != nil {
			return
		}

		action.Page.Add(res)
	}

	action.Page.BaseURL = action.BaseURL()
	action.Page.BasePath = action.Path()
	action.Page.Limit = action.PagingParams.Limit
	action.Page.Cursor = action.PagingParams.Cursor
	action.Page.Order = action.PagingParams.Order
	action.Page.PopulateLinks()
}

type TradeAggregateAction struct {
	Action
	BaseAssetFilter    xdr.Asset
	CounterAssetFilter xdr.Asset
	StartTimeFilter    int64
	EndTimeFilter      int64
	ResolutionFilter   int64
	PagingParams       db2.PageQuery
	Records            []history.TradeAggregation
	Page               hal.Page
}

// JSON is a method for actions.JSON
func (action *TradeAggregateAction) JSON() {
	action.Do(
		action.EnsureHistoryFreshness,
		action.loadParams,
		action.loadRecords,
		action.loadPage,
		func() {
			hal.Render(action.W, action.Page)
		},
	)
}

func (action *TradeAggregateAction) loadParams() {
	action.PagingParams = action.GetPageQuery()
	action.BaseAssetFilter = action.GetAsset("base_")
	action.CounterAssetFilter = action.GetAsset("counter_")
	action.StartTimeFilter = action.GetInt64("start_time")
	action.EndTimeFilter = action.GetInt64("end_time")
	action.ResolutionFilter = action.GetInt64("resolution")
}

// loadRecords populates action.Records
func (action *TradeAggregateAction) loadRecords() {

	baseAssetId, err := action.HistoryQ().GetAssetID(action.BaseAssetFilter)
	if err != nil {
		action.Err = err
		return
	}

	counterAssetId, err := action.HistoryQ().GetAssetID(action.CounterAssetFilter)
	if err != nil {
		action.Err = err
		return
	}

	bucketedTrades := action.HistoryQ().BucketTradesForAssetPair(baseAssetId, counterAssetId, action.ResolutionFilter)

	if action.StartTimeFilter > 0 {
		if action.StartTimeFilter%action.ResolutionFilter != 0 {
			// Push lower boundary to next bucket start
			action.StartTimeFilter =
				int64(action.StartTimeFilter/action.ResolutionFilter) * (action.ResolutionFilter + 1)
		}
		bucketedTrades.FromStartTime(time.Unix(action.StartTimeFilter, 0))
	}

	if action.EndTimeFilter > 0 {
		// Pull upper boundary to previous bucket start
		action.EndTimeFilter = int64(action.EndTimeFilter/action.ResolutionFilter) * action.ResolutionFilter
		bucketedTrades.FromEndTime(time.Unix(action.EndTimeFilter, 0))
	}

	action.Err = bucketedTrades.SelectAggregateByBucket(&action.Records)
}

func (action *TradeAggregateAction) loadPage() {
	for _, record := range action.Records {
		var res resource.TradeAggregation

		action.Err = res.Populate(action.Ctx, record)
		if action.Err != nil {
			return
		}

		action.Page.Add(res)
	}

	action.Page.BaseURL = action.BaseURL()
	action.Page.BasePath = action.Path()
	action.Page.Limit = action.PagingParams.Limit
	action.Page.Cursor = action.PagingParams.Cursor
	action.Page.Order = action.PagingParams.Order

	//TODO: there has to be a nicer way to transfer all params
	q := make(url.Values)
	q.Add("base_asset_type", action.GetString("base_asset_type"))
	q.Add("base_asset_code", action.GetString("base_asset_code"))
	q.Add("base_asset_issuer", action.GetString("base_asset_issuer"))
	q.Add("counter_asset_type", action.GetString("counter_asset_type"))
	q.Add("counter_asset_code", action.GetString("counter_asset_code"))
	q.Add("counter_asset_issuer", action.GetString("counter_asset_issuer"))
	q.Add("order", action.GetString("order"))
	q.Add("limit", action.GetString("limit"))
	q.Add("start_time", string(action.StartTimeFilter))
	q.Add("end_time", string(action.EndTimeFilter))
	q.Add("resolution", string(action.ResolutionFilter))

	base := action.Page.BasePath
	action.Page.Links.Self = hal.NewLink(base + "?" + q.Encode())

	if len(action.Records) < int(action.PagingParams.Limit) {
		action.Page.Links.Next = hal.NewLink("")
	} else {
		if action.PagingParams.Order == "asc" {
			newStartTime := action.Records[len(action.Records)-1].Timestamp + action.ResolutionFilter
			if newStartTime >= action.EndTimeFilter {
				action.Page.Links.Next = hal.NewLink("")
			} else {
				q.Set("start_time", string(newStartTime))
				action.Page.Links.Next = hal.NewLink(base + "?" + q.Encode())
			}
		} else { //desc
			newEndTime := action.Records[0].Timestamp
			if newEndTime <= action.StartTimeFilter {
				action.Page.Links.Next = hal.NewLink("")
			} else {
				q.Set("end_time", string(newEndTime))
				action.Page.Links.Next = hal.NewLink(base + "?" + q.Encode())
			}
		}
	}
	action.Page.Links.Prev = hal.NewLink("")
}
