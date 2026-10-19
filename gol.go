package main

import (
	"cmp"
	"context"
	"crypto/tls"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/programs/system"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"github.com/weeaa/jito-go/clients/searcher_client"
)

const hashKey, streamKey, landKey = "jito:tipfloor", "jito:tipfloor:hist", "jito:tipbot:land"
const jitoRPC, readWait, histLen = "https://mainnet.block-engine.jito.wtf/api/v1", 40 * time.Second, 3600

type tipMsg struct {
	Time string  `json:"time"`
	P25  float64 `json:"landed_tips_25th_percentile"`
	P50  float64 `json:"landed_tips_50th_percentile"`
	P75  float64 `json:"landed_tips_75th_percentile"`
	P95  float64 `json:"landed_tips_95th_percentile"`
	P99  float64 `json:"landed_tips_99th_percentile"`
}

func lamports(sol float64) uint64 { return uint64(sol*1e9 + 0.5) }

func (m tipMsg) fields() map[string]any {
	return map[string]any{
		"floor25": lamports(m.P25), "floor50": lamports(m.P50), "floor75": lamports(m.P75),
		"floor95": lamports(m.P95), "floor99": lamports(m.P99), "ts": m.Time, "seen": time.Now().Unix(),
	}
}

type bot struct {
	rdb                   *redis.Client
	jito                  *searcher_client.Client
	rpc                   *rpc.Client
	key                   solana.PrivateKey
	floor                 atomic.Value
	mu                    sync.Mutex
	sentIDs               map[string]bool
	sent, landed, dropped int64
}

func (b *bot) finish(id string, ok bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.sentIDs[id] {
		return false
	}
	delete(b.sentIDs, id)
	if ok {
		b.landed++
	} else {
		b.dropped++
	}
	return true
}

func (b *bot) counts() (sent, landed, dropped int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sent, b.landed, b.dropped
}

func main() {
	wsURL := flag.String("ws", cmp.Or(os.Getenv("TIP_STREAM_URL"), "wss://bundles.jito.wtf/api/v1/bundles/tip_stream"), "tip stream url")
	redisAddr := flag.String("redis", cmp.Or(os.Getenv("REDIS_ADDR"), "127.0.0.1:6379"), "redis addr")
	engine := flag.String("engine", cmp.Or(os.Getenv("BLOCK_ENGINE_URL"), "ny.mainnet.block-engine.jito.wtf:443"), "block engine grpc addr")
	rpcURL := flag.String("rpc", cmp.Or(os.Getenv("RPC_URL"), rpc.MainNetBeta_RPC), "solana rpc url")
	keyPath := flag.String("keypair", cmp.Or(os.Getenv("KEYPAIR"), os.ExpandEnv("$HOME/.config/solana/id.json")), "auth + payer keypair file")
	every := flag.Duration("every", 20*time.Second, "bundle interval")
	pct := flag.String("pct", "50", "tip floor percentile to pay (25/50/75/95/99)")
	capSol := flag.Float64("cap", 0.001, "max tip in sol")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)).With("svc", "tipbot"))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	must := func(err error, msg string, kv ...any) {
		if err != nil {
			slog.Error(msg, append(kv, "err", err)...)
			os.Exit(1)
		}
	}

	key, err := solana.PrivateKeyFromSolanaKeygenFile(*keyPath)
	must(err, "keypair", "path", *keyPath)
	b := &bot{rdb: redis.NewClient(&redis.Options{Addr: *redisAddr}), rpc: rpc.New(*rpcURL), key: key, sentIDs: map[string]bool{}}
	must(b.rdb.Ping(ctx).Err(), "redis unreachable", "addr", *redisAddr)
	b.jito, err = searcher_client.New(ctx, *engine, rpc.New(jitoRPC), b.rpc, key, &tls.Config{})
	must(err, "jito auth", "engine", *engine, "pubkey", key.PublicKey())
	slog.Info("session established", "engine", *engine, "pubkey", key.PublicKey())

	var wg sync.WaitGroup
	wg.Go(func() { retry(ctx, "bundle results", func() error { return b.watchResults(ctx) }) })
	wg.Go(func() {
		retry(ctx, "tip loop", func() error { return b.tipLoop(ctx, *every, "floor"+*pct, lamports(*capSol)) })
	})
	retry(ctx, "tip stream", func() error { return b.stream(ctx, *wsURL) })
	wg.Wait()
	sent, landed, dropped := b.counts()
	slog.Info("bye", "sent", sent, "landed", landed, "dropped", dropped)
}

func retry(ctx context.Context, what string, fn func() error) {
	backoff := time.Second
	for ctx.Err() == nil {
		err := fn()
		if ctx.Err() != nil {
			return
		}
		slog.Warn(what+" dropped", "err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 20*time.Second)
	}
}

func (b *bot) stream(ctx context.Context, url string) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	defer context.AfterFunc(ctx, func() { conn.Close() })()
	for {
		conn.SetReadDeadline(time.Now().Add(readWait))
		var batch []tipMsg
		if err := conn.ReadJSON(&batch); err != nil {
			return err
		}
		if len(batch) == 0 || batch[len(batch)-1].P50 <= 0 {
			continue
		}
		f := batch[len(batch)-1].fields()
		b.floor.Store(f)
		pipe := b.rdb.Pipeline()
		pipe.HSet(ctx, hashKey, f)
		pipe.XAdd(ctx, &redis.XAddArgs{Stream: streamKey, MaxLen: histLen, Approx: true, Values: f})
		if _, err := pipe.Exec(ctx); err != nil && ctx.Err() == nil {
			slog.Error("redis write", "key", hashKey, "err", err)
		}
	}
}

func (b *bot) tipLoop(ctx context.Context, every time.Duration, pct string, cap uint64) error {
	payer := b.key.PublicKey()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		m, _ := b.floor.Load().(map[string]any)
		tip, _ := m[pct].(uint64)
		if tip = min(tip, cap); tip == 0 {
			continue
		}
		acct, err := b.jito.GetRandomTipAccount()
		if err != nil {
			return err
		}
		bh, err := b.rpc.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
		if err != nil {
			return err
		}
		ix := system.NewTransferInstruction(tip, payer, solana.MustPublicKeyFromBase58(acct)).Build()
		tx, err := solana.NewTransaction([]solana.Instruction{ix}, bh.Value.Blockhash, solana.TransactionPayer(payer))
		if err != nil {
			return err
		}
		if _, err = tx.Sign(func(solana.PublicKey) *solana.PrivateKey { return &b.key }); err != nil {
			return err
		}
		resp, err := b.jito.BroadcastBundle([]*solana.Transaction{tx})
		if err != nil {
			return err
		}
		b.mu.Lock()
		b.sentIDs[resp.Uuid] = true
		b.sent++
		b.mu.Unlock()
		b.flush(ctx, "bundle sent", resp.Uuid, "tip", tip, "pct", pct)
	}
}

func (b *bot) watchResults(ctx context.Context) error {
	sub, err := b.jito.NewBundleSubscriptionResults()
	if err != nil {
		return err
	}
	for {
		r, err := sub.Recv()
		if err != nil {
			return err
		}
		landed := r.GetProcessed() != nil || r.GetFinalized() != nil
		if landed || r.GetRejected() != nil || r.GetDropped() != nil {
			if b.finish(r.BundleId, landed) {
				b.flush(ctx, "bundle result", r.BundleId, "state", r.Result)
			}
		}
	}
}

func (b *bot) flush(ctx context.Context, msg, uuid string, kv ...any) {
	sent, landed, dropped := b.counts()
	f := map[string]any{"sent": sent, "landed": landed, "dropped": dropped, "rate": float64(landed) / float64(max(sent, 1)), "ts": time.Now().Unix()}
	slog.Info(msg, append(kv, "uuid", uuid, "landed", landed, "sent", sent)...)
	if err := b.rdb.HSet(ctx, landKey, f).Err(); err != nil && ctx.Err() == nil {
		slog.Error("redis write", "key", landKey, "err", err)
	}
}
