package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"strconv"
	"time"

	log "github.com/thinkphoebe/golog"
	"github.com/thinkphoebe/goprocess"
	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/resolver"
)

type DaemonGroup struct {
	Name        string   // 用作unix socket文件名的一部分作为标识, 不同daemonServer共用socketPath时应避免冲突
	ExePath     string   // daemon子进程可执行文件的路径
	DaemonCount int      // daemon子进程的数量
	Args        []string // daemon_addr内部使用，如果Args包含daemon_addr会被覆盖
	CbLogJson   func(level log.LogLevel, j log.Json)
	subprocess  []*goprocess.SubprocessTask

	SocketPath string // 放置unix socket的目录
	Host       string // 如果SocketPath为空，使用普通网络
	PortStart  int
}

func (self *DaemonGroup) getAddr(index int) string {
	if self.SocketPath != "" {
		return "unix://" + path.Join(self.SocketPath, self.Name+strconv.Itoa(index)+".unix")
	} else {
		return self.Host + ":" + strconv.Itoa(self.PortStart+index)
	}
}

func (self *DaemonGroup) Start() *grpc.ClientConn {
	log.Infof("start daemon child ...")
	for i := 0; i < self.DaemonCount; i++ {
		go self.runDaemon(i)
	}

	if self.DaemonCount == 1 {
		log.Warnf("only one daemon, no resolver used")
		for i := 0; i < 10; i++ {
			opts := []grpc.DialOption{}
			opts = append(opts, grpc.WithInsecure())
			addr := self.getAddr(0)
			conn, err := grpc.Dial(addr, opts...)
			if err == nil {
				log.Infof("[%s] grpc.Dial [%s] complete", self.Name, addr)
				return conn
			}
			log.Errorf("[%s] grpc.Dial [%d] times got err:%s", self.Name, i, err.Error())
			time.Sleep(time.Second * 5)
		}
	} else {
		log.Infof("register resolver [%s]", self.Name)
		builder := resolverBuilder{scheme: self.Name}
		for i := 0; i < self.DaemonCount; i++ {
			addr := self.getAddr(i)
			log.Infof("Name:%s, child addr:%s", self.Name, addr)
			builder.addresses = append(builder.addresses, addr)
		}
		resolver.Register(&builder)

		for i := 0; i < 10; i++ {
			opts := []grpc.DialOption{}
			opts = append(opts, grpc.WithBalancerName(roundrobin.Name), grpc.WithInsecure())
			conn, err := grpc.Dial(self.Name+"://dummy_authority/dummy_endpoint", opts...)
			if err == nil {
				// ATTENTION grpc.Dial()并未真正建立连接，返回成功后子进程可能还未启动，立即做grpc调用可能会失败
				time.Sleep(time.Second * 3)
				log.Infof("[%s] grpc.Dial complete", self.Name)
				return conn
			}
			log.Errorf("[%s] grpc.Dial [%d] times got err:%s", self.Name, i, err.Error())
			time.Sleep(time.Second * 5)
		}
	}

	log.Errorf("[%s] grpc.Dial FAILED!", self.Name)
	return nil
}

func (self *DaemonGroup) runDaemon(index int) {
	var runCount int

	sub := &goprocess.SubprocessTask{}
	sub.TaskId = self.Name + strconv.Itoa(index)
	sub.Exe = self.ExePath
	sub.Args = append(self.Args, "-daemon_addr", self.getAddr(index))
	sub.KeepAlivePrefix = "@#-#@[keepalive]"
	sub.Timeout = 100
	sub.CbLogJson = self.CbLogJson
	sub.ShowMemInterval = 300 // 5分钟打印一次daemon的内存占用情况
	log.Infof("[%s] Init, exe [%s], args [%#v]", sub.TaskId, sub.Exe, sub.Args)

	for {
		pid, err := sub.Start()
		runCount++
		log.Infof("[%s] Start, pid:%d, err:%v, runCount:%d", sub.TaskId, pid, err, runCount)
		if err != nil {
			if self.CbLogJson != nil {
				self.CbLogJson(log.LevelWarn, log.Json{"cmd": "daemon_start_failed", "name": sub.TaskId,
					"run_count": runCount, "error_msg": fmt.Sprintf("%v", err)})
			}
			time.Sleep(time.Second * 5)
			continue
		}

		retCode, err := sub.Wait()
		log.Infof("[%s] Wait complete, retCode:%d, err:%v", sub.TaskId, retCode, err)
		if self.CbLogJson != nil {
			self.CbLogJson(log.LevelWarn, log.Json{"cmd": "daemon_exited", "name": sub.TaskId, "pid": pid,
				"run_count": runCount, "ret_code": retCode, "error_msg": fmt.Sprintf("%v", err)})
		}
		if retCode != 0 || err != nil {
			time.Sleep(time.Second * 5)
		}
	}
}

type resolverBuilder struct {
	scheme    string
	addresses []string
}

type listResolver struct {
	target    resolver.Target
	cc        resolver.ClientConn
	addresses []string
}

func (self *resolverBuilder) Build(target resolver.Target, cc resolver.ClientConn,
	opts resolver.BuildOptions) (resolver.Resolver, error) {
	r := &listResolver{
		target:    target,
		cc:        cc,
		addresses: self.addresses,
	}
	r.start()
	return r, nil
}

func (self *resolverBuilder) Scheme() string {
	return self.scheme
}

func (r *listResolver) start() {
	var addresses []resolver.Address
	for _, addr := range r.addresses {
		addresses = append(addresses, resolver.Address{Addr: addr})
	}
	r.cc.UpdateState(resolver.State{Addresses: addresses})
}

func (*listResolver) ResolveNow(o resolver.ResolveNowOptions) {}
func (*listResolver) Close()                                  {}

func ServeDaemon(bindAddr string, cbRegServer func(*grpc.Server)) error {
	var lis net.Listener
	u, err := url.Parse(bindAddr)
	if err != nil {
		log.Warnf("url.Parse() bindAddr [%s] got err [%s], try tcp bind", bindAddr, err.Error())
		lis, err = net.Listen("tcp", bindAddr)
	} else {
		log.Infof("bindAddr [%s], url [%v]", bindAddr, u)
		if u.Scheme == "unix" {
			log.Infof("open grpc server on [%s][%s]", u.Scheme, u.Path)
			os.Remove(u.Path)
			lis, err = net.Listen(u.Scheme, u.Path)
		} else {
			log.Infof("open grpc server on [%s][%s]", u.Scheme, u.Host)
			lis, err = net.Listen(u.Scheme, u.Host)
		}
	}
	if err != nil {
		log.Errorf("net.Listen() err:%s", err.Error())
		return errors.New("net.Listen() got err:" + err.Error())
	}

	ctx, cancel := context.WithCancel(context.TODO())
	go func(ctx context.Context) {
		ticker := time.NewTicker(time.Second * 30)
		tickChan := ticker.C
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-tickChan:
				fmt.Fprintf(os.Stderr, "@#-#@[keepalive]\n")
			}
		}
	}(ctx)

	s := grpc.NewServer()
	cbRegServer(s)
	if err := s.Serve(lis); err != nil {
		log.Errorf("grpc.Serve ret err:%s", err.Error())
		cancel()
		return errors.New("grpc.Serve() got err:" + err.Error())
	} else {
		log.Errorf("grpc.Serve ret nil")
		cancel()
		return nil
	}
}
