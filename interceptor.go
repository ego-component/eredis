package eredis

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/extra/rediscmd/v8"
	"github.com/go-redis/redis/v8"
	"github.com/gotomicro/ego/core/eapp"
	"github.com/gotomicro/ego/core/elog"
	"github.com/gotomicro/ego/core/emetric"
	"github.com/gotomicro/ego/core/etrace"
	"github.com/gotomicro/ego/core/transport"
	"github.com/gotomicro/ego/core/util/xdebug"
	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"

	"go.opentelemetry.io/otel/trace"
)

type eredisContextKeyType struct{}

var ctxBegKey = eredisContextKeyType{}

type interceptor struct {
	beforeProcess         func(ctx context.Context, cmd redis.Cmder) (context.Context, error)
	afterProcess          func(ctx context.Context, cmd redis.Cmder) error
	beforeProcessPipeline func(ctx context.Context, cmds []redis.Cmder) (context.Context, error)
	afterProcessPipeline  func(ctx context.Context, cmds []redis.Cmder) error
}

func (i *interceptor) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	return i.beforeProcess(ctx, cmd)
}

func (i *interceptor) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	return i.afterProcess(ctx, cmd)
}

func (i *interceptor) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	return i.beforeProcessPipeline(ctx, cmds)
}

func (i *interceptor) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	return i.afterProcessPipeline(ctx, cmds)
}

func newInterceptor(compName string, config *config, logger *elog.Component) *interceptor {
	return &interceptor{
		beforeProcess: func(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
			return ctx, nil
		},
		afterProcess: func(ctx context.Context, cmd redis.Cmder) error {
			return nil
		},
		beforeProcessPipeline: func(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
			return ctx, nil
		},
		afterProcessPipeline: func(ctx context.Context, cmds []redis.Cmder) error {
			return nil
		},
	}
}

func (i *interceptor) setBeforeProcess(p func(ctx context.Context, cmd redis.Cmder) (context.Context, error)) *interceptor {
	i.beforeProcess = p
	return i
}

func (i *interceptor) setAfterProcess(p func(ctx context.Context, cmd redis.Cmder) error) *interceptor {
	i.afterProcess = p
	return i
}

func (i *interceptor) setBeforeProcessPipeline(p func(ctx context.Context, cmds []redis.Cmder) (context.Context, error)) *interceptor {
	i.beforeProcessPipeline = p
	return i
}

func (i *interceptor) setAfterProcessPipeline(p func(ctx context.Context, cmds []redis.Cmder) error) *interceptor {
	i.afterProcessPipeline = p
	return i
}

func fixedInterceptor(compName string, config *config, logger *elog.Component) *interceptor {
	return newInterceptor(compName, config, logger).
		setBeforeProcess(func(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
			return context.WithValue(ctx, ctxBegKey, time.Now()), nil
		}).
		setAfterProcess(func(ctx context.Context, cmd redis.Cmder) error {
			var err = cmd.Err()
			// go-redis script的error做了prefix处理
			// https://github.com/go-redis/redis/blob/master/script.go#L61
			if err != nil && !strings.HasPrefix(err.Error(), "NOSCRIPT ") {
				err = fmt.Errorf("eredis exec command %s fail, %w", cmd.Name(), err)
			}
			return err
		})
}

func debugInterceptor(compName string, config *config, logger *elog.Component) *interceptor {
	addr := config.AddrString()

	return newInterceptor(compName, config, logger).setAfterProcess(
		func(ctx context.Context, cmd redis.Cmder) error {
			if !eapp.IsDevelopmentMode() {
				return cmd.Err()
			}
			cost := time.Since(ctx.Value(ctxBegKey).(time.Time))
			err := cmd.Err()
			if err != nil {
				log.Println("[eredis.response]",
					xdebug.MakeReqAndResError(fileWithLineNum(), compName, addr, cost, fmt.Sprintf("%v", cmd.Args()), err.Error()),
				)
			} else {
				log.Println("[eredis.response]",
					xdebug.MakeReqAndResInfo(fileWithLineNum(), compName, addr, cost, fmt.Sprintf("%v", cmd.Args()), response(cmd)),
				)
			}
			return err
		},
	)
}

func metricInterceptor(compName string, config *config, logger *elog.Component) *interceptor {
	addr := config.AddrString()

	return newInterceptor(compName, config, logger).setAfterProcess(
		func(ctx context.Context, cmd redis.Cmder) error {
			cost := time.Since(ctx.Value(ctxBegKey).(time.Time))
			err := cmd.Err()
			emetric.ClientHandleHistogram.WithLabelValues(emetric.TypeRedis, compName, cmd.Name(), addr).Observe(cost.Seconds())
			if err != nil {
				if errors.Is(err, redis.Nil) {
					emetric.ClientHandleCounter.Inc(emetric.TypeRedis, compName, cmd.Name(), addr, "Empty")
					return err
				}
				emetric.ClientHandleCounter.Inc(emetric.TypeRedis, compName, cmd.Name(), addr, "Error")
				return err
			}
			emetric.ClientHandleCounter.Inc(emetric.TypeRedis, compName, cmd.Name(), addr, "OK")
			return nil
		},
	)
}

func accessInterceptor(compName string, config *config, logger *elog.Component) *interceptor {
	return newInterceptor(compName, config, logger).setAfterProcess(
		func(ctx context.Context, cmd redis.Cmder) error {
			var fields = make([]elog.Field, 0, 15+transport.CustomContextKeysLength())
			var err = cmd.Err()
			cost := time.Since(ctx.Value(ctxBegKey).(time.Time))
			fields = append(fields,
				elog.FieldComponentName(compName),
				elog.FieldMethod(cmd.Name()),
				elog.FieldCost(cost))

			if config.EnableAccessInterceptorReq {
				fields = append(fields, elog.Any("req", cmd.Args()))
			}
			if config.EnableAccessInterceptorRes && err == nil {
				fields = append(fields, elog.Any("res", response(cmd)))
			}

			// 开启了链路，那么就记录链路id
			if etrace.IsGlobalTracerRegistered() {
				fields = append(fields, elog.FieldTid(etrace.ExtractTraceID(ctx)))
			}

			// 支持自定义log
			for _, key := range transport.CustomContextKeys() {
				if value := getContextValue(ctx, key); value != "" {
					fields = append(fields, elog.FieldCustomKeyValue(key, value))
				}
			}
			event := "normal"
			isSlowLog := false
			if config.SlowLogThreshold > time.Duration(0) && cost > config.SlowLogThreshold {
				isSlowLog = true
				event = "slow"
			}

			// error metric
			if err != nil {
				fields = append(fields, elog.FieldEvent(event), elog.FieldErr(err))
				if errors.Is(err, redis.Nil) {
					logger.Warn("access", fields...)
					return err
				}
				logger.Error("access", fields...)
				return err
			}

			if config.EnableAccessInterceptor || isSlowLog {
				fields = append(fields, elog.FieldEvent(event))
				if isSlowLog {
					logger.Warn("access", fields...)
				} else {
					logger.Info("access", fields...)
				}
			}
			return err
		},
	)
}

func traceInterceptor(compName string, config *config, logger *elog.Component) *interceptor {
	ip, port := peerInfo(config.Addr)
	tracer := etrace.NewTracer(trace.SpanKindClient)
	attrs := []attribute.KeyValue{
		semconv.NetHostIPKey.String(ip),
		semconv.NetPeerPortKey.Int(port),
		semconv.DBSystemRedis,
		semconv.DBNameKey.Int(config.DB),
	}
	return newInterceptor(compName, config, logger).setBeforeProcess(func(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
		ctx, span := tracer.Start(ctx, cmd.FullName(), nil, trace.WithAttributes(attrs...))
		span.SetAttributes(
			semconv.DBOperationKey.String(cmd.Name()),
			semconv.DBStatementKey.String(rediscmd.CmdString(cmd)),
		)
		return ctx, nil
	}).setAfterProcess(
		func(ctx context.Context, cmd redis.Cmder) error {
			span := trace.SpanFromContext(ctx)

			if err := cmd.Err(); err != nil && err != redis.Nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}

			span.End()
			return nil
		},
	)
}

func response(cmd redis.Cmder) string {
	switch cmd.(type) {
	case *redis.Cmd:
		return fmt.Sprintf("%v", cmd.(*redis.Cmd).Val())
	case *redis.StringCmd:
		return fmt.Sprintf("%v", cmd.(*redis.StringCmd).Val())
	case *redis.StatusCmd:
		return fmt.Sprintf("%v", cmd.(*redis.StatusCmd).Val())
	case *redis.IntCmd:
		return fmt.Sprintf("%v", cmd.(*redis.IntCmd).Val())
	case *redis.DurationCmd:
		return fmt.Sprintf("%v", cmd.(*redis.DurationCmd).Val())
	case *redis.BoolCmd:
		return fmt.Sprintf("%v", cmd.(*redis.BoolCmd).Val())
	case *redis.CommandsInfoCmd:
		return fmt.Sprintf("%v", cmd.(*redis.CommandsInfoCmd).Val())
	case *redis.StringSliceCmd:
		return fmt.Sprintf("%v", cmd.(*redis.StringSliceCmd).Val())
	default:
		return ""
	}
}

func getContextValue(c context.Context, key string) string {
	if key == "" {
		return ""
	}
	return cast.ToString(transport.Value(c, key))
}

// todo ipv6
func peerInfo(addr string) (hostname string, port int) {
	if idx := strings.IndexByte(addr, ':'); idx >= 0 {
		hostname = addr[:idx]
		port, _ = strconv.Atoi(addr[idx+1:])
	}
	return hostname, port
}

func fileWithLineNum() string {
	// the second caller usually from internal, so set i start from 2
	for i := 2; i < 15; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		if (!(strings.Contains(file, "ego-component/eredis") && strings.HasSuffix(file, "interceptor.go")) && !(strings.Contains(file, "ego-component/eredis") && strings.HasSuffix(file, "comopnent_cmds.go")) && !strings.Contains(file, "go-redis/redis")) || strings.HasSuffix(file, "_test.go") {
			return file + ":" + strconv.FormatInt(int64(line), 10)
		}
	}
	return ""
}
