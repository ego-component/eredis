package eredis

import (
	"net/http"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gotomicro/ego/core/emetric"
	"github.com/gotomicro/ego/server/egovernor"
	jsoniter "github.com/json-iterator/go"
)

var instances = sync.Map{}

type storeRedis struct {
	ClientCluster *redis.ClusterClient
	ClientStub    *redis.Client
}

func init() {
	type redisStatus struct {
		Redis map[string]interface{} `json:"redis"`
	}
	var rets = redisStatus{
		Redis: make(map[string]interface{}, 0),
	}
	egovernor.HandleFunc("/debug/redis/stats", func(w http.ResponseWriter, r *http.Request) {
		rets.Redis = stats()
		_ = jsoniter.NewEncoder(w).Encode(rets)
	})
	go monitor()
}

func monitor() {
	for {
		instances.Range(func(key, val interface{}) bool {
			name := key.(string)
			obj := val.(*storeRedis)
			var poolStats *redis.PoolStats
			if obj.ClientStub != nil {
				poolStats = obj.ClientStub.PoolStats()
			}
			if obj.ClientCluster != nil {
				poolStats = obj.ClientCluster.PoolStats()
			}

			if poolStats != nil {
				emetric.ClientStatsGauge.Set(float64(poolStats.Hits), emetric.TypeRedis, name, "hits")
				emetric.ClientStatsGauge.Set(float64(poolStats.Misses), emetric.TypeRedis, name, "misses")
				emetric.ClientStatsGauge.Set(float64(poolStats.Timeouts), emetric.TypeRedis, name, "timeouts")
				emetric.ClientStatsGauge.Set(float64(poolStats.TotalConns), emetric.TypeRedis, name, "total_conns")
				emetric.ClientStatsGauge.Set(float64(poolStats.IdleConns), emetric.TypeRedis, name, "idle_conns")
				emetric.ClientStatsGauge.Set(float64(poolStats.StaleConns), emetric.TypeRedis, name, "stale_conns")
			}
			return true
		})
		time.Sleep(time.Second * 10)
	}
}

// stats
func stats() (stats map[string]interface{}) {
	stats = make(map[string]interface{})
	instances.Range(func(key, val interface{}) bool {
		name := key.(string)
		obj := val.(*storeRedis)
		if obj.ClientStub != nil {
			stats[name] = obj.ClientStub.PoolStats()
		}
		if obj.ClientCluster != nil {
			stats[name] = obj.ClientCluster.PoolStats()
		}
		return true
	})

	return
}
