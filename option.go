package eredis

import (
	"github.com/redis/go-redis/v9"
)

// WithStub set mode to "stub"
func WithStub() Option {
	return func(c *Container) {
		c.config.Mode = StubMode
	}
}

// WithStub set mode to "cluster"
func WithCluster() Option {
	return func(c *Container) {
		c.config.Mode = ClusterMode
	}
}

// WithStub set mode to "sentinel"
func WithSentinel() Option {
	return func(c *Container) {
		c.config.Mode = SentinelMode
	}
}

// withInterceptor 注入拦截器
func withInterceptor(interceptors ...redis.Hook) Option {
	return func(c *Container) {
		if c.config.interceptors == nil {
			c.config.interceptors = make([]redis.Hook, 0, len(interceptors))
		}
		c.config.interceptors = append(c.config.interceptors, interceptors...)
	}
}

// WithPassword set password
func WithPassword(password string) Option {
	return func(c *Container) {
		c.config.Password = password
	}
}

// WithAddr set address
func WithAddr(addr string) Option {
	return func(c *Container) {
		c.config.Addr = addr
	}
}

// WithAddrs set addresses
func WithAddrs(addrs []string) Option {
	return func(c *Container) {
		c.config.Addrs = addrs
	}
}

// WithMasterName set masterName for sentinel mode
func WithMasterName(masterName string) Option {
	return func(c *Container) {
		c.config.MasterName = masterName
	}
}

// WithPoolSize set pool size
func WithPoolSize(poolSize int) Option {
	return func(c *Container) {
		c.config.PoolSize = poolSize
	}
}

// WithDB set db number
func WithDB(db int) Option {
	return func(c *Container) {
		c.config.DB = db
	}
}
