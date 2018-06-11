package plugins

import (
	"context"
	"encoding/json"
	"time"

	"google.golang.org/grpc/grpclog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/naming"
	"google.golang.org/grpc/status"

	etcd "github.com/coreos/etcd/clientv3"

	wrapper "github.com/messixukejia/grpc-wrapper"
)

const (
	resolverTimeOut = 10 * time.Second
)

type etcdRegistry struct {
	cancal context.CancelFunc
	cli    *etcd.Client
	lsCli  etcd.Lease
}

type etcdWatcher struct {
	cli       *etcd.Client
	target    string
	cancel    context.CancelFunc
	ctx       context.Context
	watchChan etcd.WatchChan
}

//NewEtcdResolver create a resolver for grpc
func NewEtcdResolver(cli *etcd.Client) naming.Resolver {
	return &etcdRegistry{
		cli: cli,
	}
}

//NewEtcdRegisty create a reistry for registering server addr
func NewEtcdRegisty(cli *etcd.Client) wrapper.Registry {
	return &etcdRegistry{
		cli:   cli,
		lsCli: etcd.NewLease(cli),
	}
}

//用于生成Watcher,监听注册中心中的服务信息变化
func (er *etcdRegistry) Resolve(target string) (naming.Watcher, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), resolverTimeOut)
	w := &etcdWatcher{
		cli:    er.cli,
		target: target + "/",
		ctx:    ctx,
		cancel: cancel,
	}
	return w, nil
}

func (er *etcdRegistry) Register(ctx context.Context, target string, update naming.Update, opts ...wrapper.RegistryOptions) (err error) {
	var upBytes []byte
	//将服务信息序列化成json格式
	if upBytes, err = json.Marshal(update); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	ctx, cancel := context.WithTimeout(context.TODO(), resolverTimeOut)
	er.cancal = cancel
	rgOpt := wrapper.RegistryOption{TTL: wrapper.DefaultRegInfTTL}
	for _, opt := range opts {
		opt(&rgOpt)
	}

	switch update.Op {
	case naming.Add:
		lsRsp, err := er.lsCli.Grant(ctx, int64(rgOpt.TTL/time.Second))
		if err != nil {
			return err
		}
		//Put服务信息到etcd,并设置key的值TTL,通过后面的KeepAlive接口
		//对TTL进行续期,超过TTL的时间未收到续期请求,则说明服务可能挂了,从而清除服务信息
		etcdOpts := []etcd.OpOption{etcd.WithLease(lsRsp.ID)}
		key := target + "/" + update.Addr
		_, err = er.cli.KV.Put(ctx, key, string(upBytes), etcdOpts...)
		if err != nil {
			return err
		}
		//保持心跳
		lsRspChan, err := er.lsCli.KeepAlive(context.TODO(), lsRsp.ID)
		if err != nil {
			return err
		}
		go func() {
			for {
				_, ok := <-lsRspChan
				if !ok {
					grpclog.Fatalf("%v keepalive channel is closing", key)
					break
				}
			}
		}()
	case naming.Delete:
		_, err = er.cli.Delete(ctx, target+"/"+update.Addr)
	default:
		return status.Error(codes.InvalidArgument, "unsupported op")
	}
	return nil
}

func (er *etcdRegistry) Close() {
	er.cancal()
	er.cli.Close()
}

//Next接口主要用于获取注册的服务信息,通过channel以及watch,当服务信息发生
//变化时,Next接口会将变化返回给grpc框架从而实现服务信息变更.
func (ew *etcdWatcher) Next() ([]*naming.Update, error) {
	var updates []*naming.Update
	//初次获取时,创建监听channel,并返回获取到的服务信息
	if ew.watchChan == nil {
		//create new chan
		resp, err := ew.cli.Get(ew.ctx, ew.target, etcd.WithPrefix(), etcd.WithSerializable())
		if err != nil {
			return nil, err
		}
		for _, kv := range resp.Kvs {
			var upt naming.Update
			if err := json.Unmarshal(kv.Value, &upt); err != nil {
				continue
			}
			updates = append(updates, &upt)
			grpclog.Fatalf("etcd get upt is %v\n", upt)
		}
		//创建etcd的watcher监听target(服务名)的信息.
		opts := []etcd.OpOption{etcd.WithRev(resp.Header.Revision + 1), etcd.WithPrefix(), etcd.WithPrevKV()}
		ew.watchChan = ew.cli.Watch(context.TODO(), ew.target, opts...)
		return updates, nil
	}

	//阻塞监听,服务发生变化时才返回给上层
	wrsp, ok := <-ew.watchChan
	if !ok {
		err := status.Error(codes.Unavailable, "etcd watch closed")
		return nil, err
	}
	if wrsp.Err() != nil {
		return nil, wrsp.Err()
	}
	for _, e := range wrsp.Events {
		var upt naming.Update
		var err error
		switch e.Type {
		case etcd.EventTypePut:
			err = json.Unmarshal(e.Kv.Value, &upt)
			upt.Op = naming.Add
		case etcd.EventTypeDelete:
			err = json.Unmarshal(e.PrevKv.Value, &upt)
			upt.Op = naming.Delete
		}

		if err != nil {
			continue
		}
		updates = append(updates, &upt)
		grpclog.Fatalf("etcd watch upt is %v\n", upt)
	}
	return updates, nil
}

func (ew *etcdWatcher) Close() {
	ew.cancel()
	ew.cli.Close()
}
